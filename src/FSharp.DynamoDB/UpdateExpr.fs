module internal FSharp.DynamoDB.UpdateExprs

open System
open System.Collections.Generic
open System.Reflection

open Microsoft.FSharp.Quotations
open Microsoft.FSharp.Quotations.Patterns
open Microsoft.FSharp.Quotations.DerivedPatterns
open Microsoft.FSharp.Quotations.ExprShape

open Swensen.Unquote

open Amazon.DynamoDBv2
open Amazon.DynamoDBv2.Model

open FSharp.DynamoDB.Common
open FSharp.DynamoDB.TypeShape
open FSharp.DynamoDB.FieldConverter

type AttrId = string

type Operand =
    | Attribute of id:string
    | Value of id:string

type UpdateValue =
    | Operand of Operand
    | Op_Addition of Operand * Operand
    | Op_Subtraction of Operand * Operand

and UpdateExpr =
    | Set of attrId:string * UpdateValue
    | Add of attrId:string * op:Operand
    | Delete of attrId:string * op:Operand

type UpdateExpression =
    {
        UpdateExprs : UpdateExpr list
        Expression : string
        Attributes : Map<string, RecordProperty>
        Values     : Map<string, FsAttributeValue>
    }
with
    member __.DAttributes = 
        __.Attributes
        |> Seq.map (fun kv -> keyVal kv.Key kv.Value.Name)
        |> cdict

    member __.DValues = 
        __.Values 
        |> Seq.map (fun kv -> keyVal kv.Key (FsAttributeValue.ToAttributeValue kv.Value))
        |> cdict

let updateExprToString(uexprs : UpdateExpr list) =
    let opStr op = match op with Attribute id -> id | Value id -> id
    let valStr value = 
        match value with 
        | Operand op -> opStr op 
        | Op_Addition(l, r) -> sprintf "%s + %s" (opStr l) (opStr r)
        | Op_Subtraction(l, r) -> sprintf "%s - %s" (opStr l) (opStr r)

    let sb = new System.Text.StringBuilder()
    let append (s:string) = sb.Append s |> ignore
    let toggle = let b = ref false in fun () -> if !b then append " " else b := true
    match uexprs |> List.choose (function Set(id, v) -> Some(id,v) | _ -> None) with 
    | [] -> () 
    | (id, v) :: tail -> 
        toggle()
        sprintf "SET %s = %s" id (valStr v) |> append
        for id,v in tail do sprintf ", %s = %s" id (valStr v) |> append

    match uexprs |> List.choose (function Add(id, v) -> Some(id,v) | _ -> None) with
    | [] -> ()
    | (id, o) :: tail ->
        toggle()
        sprintf "ADD %s %s" id (opStr o) |> append
        for id, o in tail do sprintf ", %s %s" id (opStr o) |> append

    match uexprs |> List.choose (function Delete(id, v) -> Some(id,v) | _ -> None) with
    | [] -> ()
    | (id, o) :: tail ->
        toggle()
        sprintf "REMOVE %s %s" id (opStr o) |> append
        for id, o in tail do sprintf ", %s %s" id (opStr o) |> append

    sb.ToString()
        

let extractUpdateExpr (recordInfo : RecordInfo) (expr : Expr<'TRecord -> 'TRecord>) =
    if not expr.IsClosed then invalidArg "expr" "supplied update expression contains free variables."
    let invalidExpr() = invalidArg "expr" <| sprintf "Supplied expression is not a valid update expression."

    let values = new Dictionary<FsAttributeValue, string> ()
    let getValue (fav : FsAttributeValue) =
        let ok,found = values.TryGetValue fav
        if ok then found
        else
            let name = sprintf ":val%d" values.Count
            values.Add(fav, name)
            name

    let getValueExpr (conv : FieldConverter) (expr : Expr) =
        let o = evalRaw expr
        let fav = conv.OfFieldUntyped o
        getValue fav

    let attributes = new Dictionary<string, string * RecordProperty> ()
    let getAttr (rp : RecordProperty) =
        let ok, found = attributes.TryGetValue rp.Name
        if ok then fst found
        else
            let name = sprintf "#ATTR%d" attributes.Count
            attributes.Add(rp.Name, (name, rp))
            name

    match expr with
    | Lambda(r,body) ->
        let rec stripBindings (bindings : Map<Var, Expr>) (expr : Expr) =
            match expr with
            | Let(v, body, cont) -> stripBindings (Map.add v body bindings) cont
            | NewRecord(_, assignments) -> bindings, assignments
            | _ -> invalidExpr()

        let bindings, assignments = stripBindings Map.empty body

        let tryExtractValueExpr (i : int) (assignment : Expr) =
            let rp = recordInfo.Properties.[i]
            match assignment with
            | PropertyGet(Some (Var y), prop, []) when r = y && rp.PropertyInfo = prop -> None
            | Var v when bindings.ContainsKey v -> Some(rp, bindings.[v])
            | e -> Some (rp, e)

        let updateExprs = assignments |> List.mapi tryExtractValueExpr |> List.choose id

        let (|RecordPropertyGet|_|) (e : Expr) =
            match e with
            | PropertyGet(Some (Var r'), p, []) when r = r' ->
                recordInfo.Properties |> Array.tryFind(fun rp -> rp.PropertyInfo = p)
            | _ -> None

        let rec extractOperand (expr : Expr) =
            match expr with
            | _ when expr.IsClosed ->
                let conv = FieldConverter.resolveUntyped expr.Type
                let id = getValueExpr conv expr in Value id
            | PipeRight e | PipeLeft e -> extractOperand e
            | RecordPropertyGet rp -> let id = getAttr rp in Attribute id
            | _ -> invalidExpr()

        let rec extractUpdateValue (expr : Expr) =
            match expr with
            | PipeRight e | PipeLeft e -> extractUpdateValue e
            | SpecificCall2 <@ (+) @> (None, _, _, [left; right]) ->
                let conv = FieldConverter.resolveUntyped left.Type
                if not conv.IsScalar then invalidArg "expr" "Addition of non-scalar types not supported"
                let l, r = extractOperand left, extractOperand right
                Op_Addition(l, r)

            | SpecificCall2 <@ (-) @> (None, _, _, [left; right]) ->
                let conv = FieldConverter.resolveUntyped left.Type
                if not conv.IsScalar then invalidArg "expr" "Addition of non-scalar types not supported"
                let l, r = extractOperand left, extractOperand right
                Op_Subtraction(l, r)

            | _ -> extractOperand expr |> Operand

        let rec extractUpdateExpr (rp : RecordProperty) (expr : Expr) =
            match expr with
            | PipeRight e | PipeLeft e -> extractUpdateExpr rp e
            | SpecificCall2 <@ Set.add @> (None, _, _, [elem; RecordPropertyGet rp']) when rp = rp' ->
                let attrId = getAttr rp
                let op = extractOperand elem
                Add(attrId, op)

            | SpecificCall2 <@ fun (s : Set<_>) e -> s.Add e @> (Some (RecordPropertyGet rp'), _, _, [elem]) when rp = rp' ->
                let attrId = getAttr rp
                let op = extractOperand elem
                Add(attrId, op)

            | SpecificCall2 <@ Set.remove @> (None, _, _, [elem ; RecordPropertyGet rp']) when rp = rp' ->
                let attrId = getAttr rp
                let op = extractOperand elem
                Delete(attrId, op)

            | SpecificCall2 <@ fun (s : Set<_>) e -> s.Remove e @> (Some (RecordPropertyGet rp'), _, _, [elem]) when rp = rp' ->
                let attrId = getAttr rp
                let op = extractOperand elem
                Delete(attrId, op)

            | SpecificCall2 <@ Set.addSeq @> (None, _, _, [elems ; RecordPropertyGet rp']) when rp = rp' ->
                let attrId = getAttr rp
                let op = extractOperand elems
                Delete(attrId, op)                

            | SpecificCall2 <@ fun (s : Set<_>) (ts : seq<_>) -> s.Add ts @> (Some (RecordPropertyGet rp'), _, _, [ts]) when rp = rp' ->
                let attrId = getAttr rp
                let op = extractOperand ts
                Delete(attrId, op)

            | SpecificCall2 <@ Set.removeSeq @> (None, _, _, [elems ; RecordPropertyGet rp']) when rp = rp' ->
                let attrId = getAttr rp
                let op = extractOperand elems
                Delete(attrId, op)                

            | SpecificCall2 <@ fun (s : Set<_>) (ts : seq<_>) -> s.Remove ts @> (Some (RecordPropertyGet rp'), _, _, [elems]) when rp = rp' ->
                let attrId = getAttr rp
                let op = extractOperand elems
                Delete(attrId, op)

            | SpecificCall2 <@ (+) @> (None, _, _, ([RecordPropertyGet rp'; other] | [other ; RecordPropertyGet rp'])) when rp = rp' && not rp.Converter.IsScalar ->
                let attrId = getAttr rp
                let op = extractOperand other
                Add(attrId, op)

            | SpecificCall2 <@ (-) @> (None, _, _, ([RecordPropertyGet rp'; other] | [other ; RecordPropertyGet rp'])) when rp = rp' && not rp.Converter.IsScalar ->
                let attrId = getAttr rp
                let op = extractOperand other
                Delete(attrId, op)

            | e -> 
                let attrId = getAttr rp
                let uv = extractUpdateValue e
                Set(attrId, uv)

        let updateExprs = updateExprs |> List.map (fun (rp,e) -> extractUpdateExpr rp e)

        {
            UpdateExprs = updateExprs
            Expression = updateExprToString updateExprs
            Attributes = attributes |> Seq.map (fun kv -> kv.Value) |> Map.ofSeq
            Values = values |> Seq.map (fun kv -> kv.Value, kv.Key) |> Map.ofSeq
        }

    | _ -> invalidExpr()