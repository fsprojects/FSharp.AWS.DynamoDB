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
    | Value of AttributeValue
    | Undefined
with
    member op.IsUndefinedValue = match op with Undefined -> true | _ -> false

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
        Attributes : (string * string) []
        Values     : (string * AttributeValue) []
    }
with
    member __.WriteAttributesTo(target : Dictionary<string,string>) =
        for k,v in __.Attributes do target.[k] <- v

    member __.WriteValuesTo(target : Dictionary<string, AttributeValue>) =
        for k,v in __.Values do target.[k] <- v

let extractUpdateExprs (recordInfo : RecordInfo) (expr : Expr<'TRecord -> 'TRecord>) =
    if not expr.IsClosed then invalidArg "expr" "supplied update expression contains free variables."
    let invalidExpr() = invalidArg "expr" <| sprintf "Supplied expression is not a valid update expression."

    let getValue (conv : FieldConverter) (expr : Expr) =
        match expr |> evalRaw |> conv.Coerce with
        | None -> Undefined
        | Some av -> Value av

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
            | _ when rp.IsHashKey -> invalidArg "expr" "update expression cannot update hash key."
            | _ when rp.IsRangeKey -> invalidArg "expr" "update expression cannot update range key."
            | Var v when bindings.ContainsKey v -> Some(rp, bindings.[v])
            | e -> Some (rp, e)

        let updateExprs = assignments |> List.mapi tryExtractValueExpr |> List.choose id

        let (|AttributeGet|_|) (e : Expr) = AttributePath.Extract r recordInfo e

        let rec extractOperand (conv : FieldConverter) (expr : Expr) =
            match expr with
            | _ when expr.IsClosed -> getValue conv expr
            | PipeRight e | PipeLeft e -> extractOperand conv e
            | AttributeGet attr -> Attribute attr
            | _ -> invalidExpr()

        let rec extractUpdateValue (conv : FieldConverter) (expr : Expr) =
            match expr with
            | PipeRight e | PipeLeft e -> extractUpdateValue conv e
            | SpecificCall2 <@ (+) @> (None, _, _, [left; right]) when conv.Representation = FieldRepresentation.Number ->
                let l, r = extractOperand conv left, extractOperand conv right
                if l.IsUndefinedValue then Operand r
                elif r.IsUndefinedValue then Operand l
                else
                    Op_Addition(l, r)

            | SpecificCall2 <@ (-) @> (None, _, _, [left; right]) when conv.Representation = FieldRepresentation.Number ->
                let l, r = extractOperand conv left, extractOperand conv right
                if l.IsUndefinedValue then Operand r
                elif r.IsUndefinedValue then Operand l
                else
                    Op_Subtraction(l, r)

            | SpecificCall2 <@ Array.append @> (None, _, _, [left; right]) ->
                let l, r = extractOperand conv left, extractOperand conv right
                if l.IsUndefinedValue then Operand r
                elif r.IsUndefinedValue then Operand l
                else
                    List_Append(l, r)

            | SpecificCall2 <@ (@) @> (None, _, _, [left; right]) ->
                let l, r = extractOperand conv left, extractOperand conv right
                if l.IsUndefinedValue then Operand r
                elif r.IsUndefinedValue then Operand l
                else
                    List_Append(l, r)

            | SpecificCall2 <@ List.append @> (None, _, _, [left; right]) ->
                let l, r = extractOperand conv left, extractOperand conv right
                if l.IsUndefinedValue then Operand r
                elif r.IsUndefinedValue then Operand l
                else
                    List_Append(l, r)

            | _ -> extractOperand conv expr |> Operand

        let rec tryExtractUpdateExpr (parent : AttributePath) (expr : Expr) =
            match expr with
            | PipeRight e | PipeLeft e -> tryExtractUpdateExpr parent e
            | SpecificCall2 <@ Set.add @> (None, _, _, [elem; AttributeGet attr]) when parent = attr ->
                let op = extractOperand parent.Converter elem
                if op.IsUndefinedValue then None
                else Add(attr, op) |> Some

            | SpecificCall2 <@ fun (s : Set<_>) e -> s.Add e @> (Some (AttributeGet attr), _, _, [elem]) when attr = parent ->
                let op = extractOperand parent.Converter elem
                if op.IsUndefinedValue then None
                else
                    Add(attr, op) |> Some

            | SpecificCall2 <@ Set.remove @> (None, _, _, [elem ; AttributeGet attr]) when attr = parent ->
                let op = extractOperand parent.Converter elem
                if op.IsUndefinedValue then None
                else
                    Delete(attr, op) |> Some

            | SpecificCall2 <@ fun (s : Set<_>) e -> s.Remove e @> (Some (AttributeGet attr), _, _, [elem]) when attr = parent ->
                let op = extractOperand parent.Converter elem
                if op.IsUndefinedValue then None
                else
                    Delete(attr, op) |> Some

            | SpecificCall2 <@ (+) @> (None, _, _, ([AttributeGet attr; other] | [other ; AttributeGet attr])) when attr = parent && not attr.Converter.IsScalar ->
                let op = extractOperand parent.Converter other
                if op.IsUndefinedValue then None
                else
                    Add(attr, op) |> Some

            | SpecificCall2 <@ (-) @> (None, _, _, [AttributeGet attr; other]) when attr = parent && not attr.Converter.IsScalar ->
                let op = extractOperand parent.Converter other
                if op.IsUndefinedValue then None
                else
                    Delete(attr, op) |> Some

            | SpecificCall2 <@ Map.add @> (None, _, _, [keyE; value; AttributeGet attr]) when attr = parent ->
                let key = evalRaw keyE
                let attr = Suffix(key, parent)
                let econv = unbox<ICollectionConverter>(parent.Converter).ElementConverter
                match extractUpdateValue econv value with
                | Operand op when op.IsUndefinedValue -> Some(Remove attr)
                | uv -> Some(Set(attr, uv))

            | SpecificCall2 <@ Map.remove @> (None, _, _, [keyE; AttributeGet attr]) when attr = parent ->
                let key = evalRaw keyE
                let attr = Suffix(key, parent)
                Some(Remove attr)

            | e -> 
                match extractUpdateValue parent.Converter e with
                | Operand op when op.IsUndefinedValue -> Some(Remove parent)
                | uv -> Some(Set(parent, uv))

        updateExprs |> List.choose (fun (rp,e) -> tryExtractUpdateExpr (Root rp) e)
    | _ -> invalidExpr()


let updateExprsToString (getAttrId : AttributePath -> string) 
                        (getValueId : AttributeValue -> string) (uexprs : UpdateExpr list) =

    let opStr op = 
        match op with 
        | Attribute id -> getAttrId id 
        | Value id -> getValueId id
        | Undefined -> invalidOp "internal error: attempting to reference undefined value in update expression."

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

let extractUpdateExpression (recordInfo : RecordInfo) (expr : Expr<'Record -> 'Record>) =
    match extractUpdateExprs recordInfo expr with
    | [] -> invalidArg "expr" "No update clauses found in expression"
    | updateExprs ->
        let attrs = new Dictionary<string, string> ()
        let getAttrId (attr : AttributePath) =
            let ok,found = attrs.TryGetValue attr.RootId
            if ok then attr.Id
            else
                attrs.Add(attr.RootId, attr.RootName)
                attr.Id

        let values = new Dictionary<AttributeValue, string>(new AttributeValueComparer())
        let getValueId (av : AttributeValue) =
            let ok,found = values.TryGetValue av
            if ok then found
            else
                let id = sprintf ":uval%d" values.Count
                values.Add(av, id)
                id

        let exprString = updateExprsToString getAttrId getValueId updateExprs

        {
            UpdateExprs = updateExprs
            Expression = exprString
            Attributes = attrs |> Seq.map (fun kv -> kv.Key, kv.Value) |> Seq.toArray
            Values = values |> Seq.map (fun kv -> kv.Value, kv.Key) |> Seq.toArray
        }