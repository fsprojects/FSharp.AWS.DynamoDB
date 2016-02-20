module internal FSharp.DynamoDB.UpdateExpr

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

open FSharp.DynamoDB.FieldConverter
open FSharp.DynamoDB.ExprCommon

type Operand =
    | Attribute of AttributePath
    | Value of FsAttributeValue

type UpdateValue =
    | Operand of Operand
    | Op_Addition of Operand * Operand
    | Op_Subtraction of Operand * Operand
    | List_Append of Operand * Operand

and UpdateExpr =
    | Set of AttributePath * UpdateValue
    | Remove of AttributePath
    | Add of AttributePath * Operand
    | Delete of AttributePath * Operand

type UpdateExpression =
    {
        UpdateExprs : UpdateExpr list
        Expression : string
        Attributes : Map<string, string>
        Values     : Map<string, FsAttributeValue>
    }
with
    member __.DAttributes = 
        __.Attributes
        |> cdict

    member __.DValues = 
        __.Values 
        |> Seq.map (fun kv -> keyVal kv.Key (FsAttributeValue.ToAttributeValue kv.Value))
        |> cdict


let updateExprsToString (getAttrId : AttributePath -> string) (getValueId : FsAttributeValue -> string) (uexprs : UpdateExpr list) =
    let opStr op = match op with Attribute id -> getAttrId id | Value id -> getValueId id
    let valStr value = 
        match value with 
        | Operand op -> opStr op 
        | Op_Addition(l, r) -> sprintf "%s + %s" (opStr l) (opStr r)
        | Op_Subtraction(l, r) -> sprintf "%s - %s" (opStr l) (opStr r)
        | List_Append(l,r) -> sprintf "(list_append(%s, %s))" (opStr l) (opStr r)

    let sb = new System.Text.StringBuilder()
    let append (s:string) = sb.Append s |> ignore
    let toggle = let b = ref false in fun () -> if !b then append " " else b := true
    match uexprs |> List.choose (function Set(id, v) -> Some(id,v) | _ -> None) with 
    | [] -> () 
    | (id, v) :: tail -> 
        toggle()
        sprintf "SET %s = %s" (getAttrId id) (valStr v) |> append
        for id,v in tail do sprintf ", %s = %s" (getAttrId id) (valStr v) |> append

    match uexprs |> List.choose (function Add(id, v) -> Some(id,v) | _ -> None) with
    | [] -> ()
    | (id, o) :: tail ->
        toggle()
        sprintf "ADD %s %s" (getAttrId id) (opStr o) |> append
        for id, o in tail do sprintf ", %s %s" (getAttrId id) (opStr o) |> append

    match uexprs |> List.choose (function Delete(id, v) -> Some(id,v) | _ -> None) with
    | [] -> ()
    | (id, o) :: tail ->
        toggle()
        sprintf "DELETE %s %s" (getAttrId id) (opStr o) |> append
        for id, o in tail do sprintf ", %s %s" (getAttrId id) (opStr o) |> append

    match uexprs |> List.choose (function Remove id -> Some id | _ -> None) with
    | [] -> ()
    | id :: tail ->
        toggle()
        sprintf "REMOVE %s" (getAttrId id) |> append
        for id in tail do sprintf ", %s" (getAttrId id) |> append

    sb.ToString()
        

let extractUpdateExpr (recordInfo : RecordInfo) (expr : Expr<'TRecord -> 'TRecord>) =
    if not expr.IsClosed then invalidArg "expr" "supplied update expression contains free variables."
    let invalidExpr() = invalidArg "expr" <| sprintf "Supplied expression is not a valid update expression."

    let getValue (conv : FieldConverter) (expr : Expr) =
        match expr |> evalRaw |> conv.Coerce with
        | None -> Undefined
        | Some av -> FsAttributeValue.FromAttributeValue av

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

        let (|AttributeGet|_|) (e : Expr) = AttributePath.Extract r recordInfo e

        let rec extractOperand (conv : FieldConverter) (expr : Expr) =
            match expr with
            | _ when expr.IsClosed -> let av = getValue conv expr in Value av
            | PipeRight e | PipeLeft e -> extractOperand conv e
            | AttributeGet attr -> Attribute attr
            | _ -> invalidExpr()

        let rec extractUpdateValue (conv : FieldConverter) (expr : Expr) =
            match expr with
            | PipeRight e | PipeLeft e -> extractUpdateValue conv e
            | SpecificCall2 <@ (+) @> (None, _, _, [left; right]) when conv.IsScalar ->
                let l, r = extractOperand conv left, extractOperand conv right
                Op_Addition(l, r)

            | SpecificCall2 <@ (-) @> (None, _, _, [left; right]) when conv.IsScalar ->
                let l, r = extractOperand conv left, extractOperand conv right
                Op_Subtraction(l, r)

            | SpecificCall2 <@ Array.append @> (None, _, _, [left; right]) ->
                let l, r = extractOperand conv left, extractOperand conv right
                List_Append(l ,r)

            | SpecificCall2 <@ (@) @> (None, _, _, [left; right]) ->
                let l, r = extractOperand conv left, extractOperand conv right
                List_Append(l ,r)

            | SpecificCall2 <@ List.append @> (None, _, _, [left; right]) ->
                let l, r = extractOperand conv left, extractOperand conv right
                List_Append(l ,r)

            | _ -> extractOperand conv expr |> Operand

        let rec extractUpdateExpr (parent : AttributePath) (expr : Expr) =
            match expr with
            | PipeRight e | PipeLeft e -> extractUpdateExpr parent e
            | SpecificCall2 <@ Set.add @> (None, _, _, [elem; AttributeGet attr]) when parent = attr ->
                let op = extractOperand parent.Converter elem
                Add(attr, op)

            | SpecificCall2 <@ fun (s : Set<_>) e -> s.Add e @> (Some (AttributeGet attr), _, _, [elem]) when attr = parent ->
                let op = extractOperand parent.Converter elem
                Add(attr, op)

            | SpecificCall2 <@ Set.remove @> (None, _, _, [elem ; AttributeGet attr]) when attr = parent ->
                let op = extractOperand parent.Converter elem
                Delete(attr, op)

            | SpecificCall2 <@ fun (s : Set<_>) e -> s.Remove e @> (Some (AttributeGet attr), _, _, [elem]) when attr = parent ->
                let op = extractOperand parent.Converter elem
                Delete(attr, op)

            | SpecificCall2 <@ Set.addSeq @> (None, _, _, [elems ; AttributeGet attr]) when attr = parent ->
                let op = extractOperand parent.Converter elems
                Add(attr, op)

            | SpecificCall2 <@ fun (s : Set<_>) (ts : seq<_>) -> s.Add ts @> (Some (AttributeGet attr), _, _, [ts]) when attr = parent ->
                let op = extractOperand parent.Converter ts
                Add(attr, op)

            | SpecificCall2 <@ Set.removeSeq @> (None, _, _, [elems ; AttributeGet attr]) when attr = parent ->
                let op = extractOperand parent.Converter elems
                Delete(attr, op)

            | SpecificCall2 <@ fun (s : Set<_>) (ts : seq<_>) -> s.Remove ts @> (Some (AttributeGet attr), _, _, [elems]) when attr = parent ->
                let op = extractOperand parent.Converter elems
                Delete(attr, op)

            | SpecificCall2 <@ (+) @> (None, _, _, ([AttributeGet attr; other] | [other ; AttributeGet attr])) when attr = parent && not attr.Converter.IsScalar ->
                let op = extractOperand parent.Converter other
                Add(attr, op)

            | SpecificCall2 <@ (-) @> (None, _, _, [AttributeGet attr; other]) when attr = parent && not attr.Converter.IsScalar ->
                let op = extractOperand parent.Converter other
                Delete(attr, op)

            | e -> 
                let uv = extractUpdateValue parent.Converter e
                Set(parent, uv)

        let updateExprs = updateExprs |> List.map (fun (rp,e) -> extractUpdateExpr (Root rp) e)

        let attrs = new Dictionary<string, string> ()
        let getAttrId (attr : AttributePath) =
            let ok,found = attrs.TryGetValue attr.RootId
            if ok then attr.Id
            else
                attrs.Add(attr.RootId, attr.RootName)
                attr.Id

        let values = new Dictionary<FsAttributeValue, string>()
        let getValueId (fsv : FsAttributeValue) =
            let ok,found = values.TryGetValue fsv
            if ok then found
            else
                let id = sprintf ":uval%d" values.Count
                values.Add(fsv, id)
                id

        let exprString = updateExprsToString getAttrId getValueId updateExprs

        {
            UpdateExprs = updateExprs
            Expression = exprString
            Attributes = attrs |> Seq.map (fun kv -> kv.Key, kv.Value) |> Map.ofSeq
            Values = values |> Seq.map (fun kv -> kv.Value, kv.Key) |> Map.ofSeq
        }

    | _ -> invalidExpr()