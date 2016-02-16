module internal FSharp.DynamoDB.ConditionalExprs

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

// http://docs.aws.amazon.com/amazondynamodb/latest/developerguide/Expressions.SpecifyingConditions.html#ConditionExpressionReference

type QueryExpr =
    | False
    | True
    | Not of QueryExpr
    | And of QueryExpr * QueryExpr
    | Or of QueryExpr * QueryExpr
    | Compare of Comparator * Operand * Operand
    | Between of Operand * Operand * Operand
    | BeginsWith of attr:string * valId:string
    | Contains of attr:string * Operand
    | SizeOf of attr:string

and Comparator =
    | EQ
    | NE
    | LT
    | GT
    | LE
    | GE

and Operand = 
    | Value of id:string
    | Attribute of id:string

type ConditionalExpression =
    {
        QueryExpr : QueryExpr
        Expression : string
        Attributes : Map<string, RecordProperty>
        Values : Map<string, FsAttributeValue>
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

let queryExprToString (qExpr : QueryExpr) =
    let sb = new System.Text.StringBuilder()
    let inline (!) (p:string) = sb.Append p |> ignore
    let inline writeOp o = ! (match o with Value id -> id | Attribute id -> id)
    let inline writeCmp cmp =
        match cmp with
        | EQ -> ! " = "
        | NE -> ! " <> "
        | LT -> ! " < "
        | GT -> ! " > "
        | GE -> ! " >= "
        | LE -> ! " <= "

    let rec aux q =
        match q with
        | False | True -> invalidOp "internal error: invalid query representation"
        | Not q -> ! "( NOT " ; aux q ; ! " )"
        | And (l,r) -> ! "( " ; aux l ; ! " AND " ; aux r ; ! " )"
        | Or (l,r) -> ! "( " ; aux l ; ! " OR " ; aux r ; ! " )"
        | Compare (cmp, l, r) -> ! "( " ; writeOp l ; writeCmp cmp ; writeOp r ; ! " )"
        | Between (v,l,u) -> ! "( " ; writeOp v ; ! " BETWEEN " ; writeOp l ; ! " AND " ; writeOp u ; ! " )"
        | BeginsWith (attr, valId) -> ! "( begins_with ( " ; ! attr ; ! ", " ; ! valId ; !" ))"
        | Contains (attr, op) -> ! "( contains ( " ; !attr ; !", " ; writeOp op ; !" ))"
        | SizeOf attr -> ! "( size ( " ; !attr ; ! " ))"

    aux qExpr
    sb.ToString()

let extractQueryExpr (recordInfo : RecordInfo) (expr : Expr<'TRecord -> bool>) =
    let invalidQuery() = invalidArg "expr" <| sprintf "Supplied expression is not a valid conditional."
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

    let (|RecordPropertyGet|_|) (e : Expr) =
        match e with
        | PropertyGet(Some (Var _), p, []) when p.DeclaringType = recordInfo.RecordType ->
            recordInfo.Properties |> Array.tryFind(fun rp -> rp.PropertyInfo = p)
        | _ -> None

    let (|Operands|) (exprs : Expr list) =
        match exprs |> List.tryPick (|RecordPropertyGet|_|) with
        | Some rp ->
            if rp.Converter.Representation = FieldRepresentation.Serializer then
                invalidArg "expr" "cannot query serialized properties"

            let extractOperand (expr : Expr) =
                match expr with
                | e when e.IsClosed ->
                    let id = getValueExpr rp.Converter e
                    Value id

                | RecordPropertyGet rp' ->
                    let id = getAttr rp'
                    Attribute id

                | _ -> invalidQuery()

            rp, exprs |> List.map extractOperand

        | None  -> invalidQuery()

    let (|Comparison|_|) (pat : Expr) (expr : Expr) =
        match expr with
        | SpecificCall pat (None, _, Operands(rp, [left; right])) ->
            if not rp.Converter.IsScalar then
                invalidArg "expr" "conditional expression uses comparison on non-scalar values."

            Some(left, right)

        | _ -> None

    let rec extractQuery (expr : Expr) =
        match expr with
        | e when e.IsClosed -> if evalRaw e then True else False
        | SpecificCall <@ not @> (None, _, [body]) -> 
            match extractQuery body with
            | Not q -> q
            | q -> Not q

        | AndAlso(left, right) -> 
            match extractQuery left, extractQuery right with
            | False, _ -> False
            | _, False -> False
            | True, r -> r
            | l, True -> l
            | l, r -> And(l, r)

        | OrElse(left, right) -> 
            match extractQuery left, extractQuery right with
            | True, _ -> True
            | _, True -> True
            | False, r -> r
            | l, False -> l
            | l, r -> Or(l, r)

        | PipeLeft e
        | PipeRight e -> extractQuery e

        | RecordPropertyGet rp -> 
            let attr = getAttr rp
            let value = getValue (Bool true) 
            Compare(EQ, Attribute(attr), Value(value))

        | Comparison <@ (=) @> (left, right) -> Compare(EQ, left, right)
        | Comparison <@ (<>) @> (left, right) -> Compare(NE, left, right)
        | Comparison <@ (<) @> (left, right) -> Compare(LT, left, right)
        | Comparison <@ (>) @> (left, right) -> Compare(GT, left, right)
        | Comparison <@ (<=) @> (left, right) -> Compare(LE, left, right)
        | Comparison <@ (>=) @> (left, right) -> Compare(GE, left, right)

        | SpecificCall2 <@ fun (x:string) y -> x.StartsWith y @> (Some (RecordPropertyGet rp), _, _, [value]) when value.IsClosed ->
            let attrId = getAttr rp
            let valId = getValueExpr rp.Converter value
            BeginsWith(attrId, valId)

        | SpecificCall2 <@ fun (x:string) y -> x.Contains y @> (Some (RecordPropertyGet rp), _, _, [value]) when value.IsClosed ->
            let attrId = getAttr rp
            let valId = getValueExpr rp.Converter value
            Contains(attrId, Value valId)

        | SpecificCall2 <@ Set.contains @> (None, _, _, [elem; RecordPropertyGet rp]) when elem.IsClosed ->
            let attrId = getAttr rp
            let fconv = rp.PropertyInfo.PropertyType.GetGenericArguments().[0] |> FieldConverter.resolveUntyped
            let valId = getValueExpr fconv elem
            Contains(attrId, Value valId)

        | SpecificCall2 <@ fun (x:Set<_>) e -> x.Contains e @> (Some(RecordPropertyGet rp), _, _, [elem]) when elem.IsClosed ->
            let attrId = getAttr rp
            let fconv = rp.PropertyInfo.PropertyType.GetGenericArguments().[0] |> FieldConverter.resolveUntyped
            let valId = getValueExpr fconv elem
            Contains(attrId, Value valId)

        | SpecificCall2 <@ fun (x : HashSet<_>) y -> x.Contains y @> (Some(RecordPropertyGet rp), _, _, [elem]) when elem.IsClosed ->
            let attrId = getAttr rp
            let fconv = rp.PropertyInfo.PropertyType.GetGenericArguments().[0] |> FieldConverter.resolveUntyped
            let valId = getValueExpr fconv elem
            Contains(attrId, Value valId)

        | SpecificCall2 <@ Map.containsKey @> (None, _, _, [elem; RecordPropertyGet rp]) when elem.IsClosed ->
            let attrId = getAttr rp
            let fconv = rp.PropertyInfo.PropertyType.GetGenericArguments().[0] |> FieldConverter.resolveUntyped
            let valId = getValueExpr fconv elem
            Contains(attrId, Value valId)

        | SpecificCall2 <@ fun (x : Map<_,_>) y -> x.ContainsKey y @> (Some(RecordPropertyGet rp), _, _, [elem]) when elem.IsClosed ->
            let attrId = getAttr rp
            let fconv = rp.PropertyInfo.PropertyType.GetGenericArguments().[0] |> FieldConverter.resolveUntyped
            let valId = getValueExpr fconv elem
            Contains(attrId, Value valId)

        | SpecificCall2 <@ fun (x : Dictionary<_,_>) y -> x.ContainsKey y @> (Some(RecordPropertyGet rp), _, _, [elem]) when elem.IsClosed ->
            let attrId = getAttr rp
            let fconv = rp.PropertyInfo.PropertyType.GetGenericArguments().[0] |> FieldConverter.resolveUntyped
            let valId = getValueExpr fconv elem
            Contains(attrId, Value valId)


        | _ -> invalidQuery()

    if not expr.IsClosed then invalidArg "expr" "supplied query is not a closed expression."

    match expr with
    | Lambda(x, body) when x.Type = typeof<'TRecord> -> 
        match extractQuery body with
        | False | True -> invalidArg "expr" "supplied query is tautological."
        | q ->

        {
            QueryExpr = q
            Expression = queryExprToString q
            Attributes = attributes |> Seq.map (fun kv -> kv.Value) |> Map.ofSeq
            Values = values |> Seq.map (fun kv -> kv.Value, kv.Key) |> Map.ofSeq
        }

    | _ -> invalidQuery()