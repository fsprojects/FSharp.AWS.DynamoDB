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
    | BooleanAttribute of attr:string
    | Compare of Comparator * Operand * Operand
    | Between of Operand * Operand * Operand
    | BeginsWith of attr:string * valId:string
    | Contains of attr:string * Operand
    | SizeOf of attr:string

and Comparator =
    | EQ
    | NEQ
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
        Attributes : Map<string, string>
        Values : Map<string, FsAttributeValue>
    }
with
    member __.IsTautological =
        match __.QueryExpr with
        | False | True -> true
        | _ -> false

let queryExprToString (qExpr : QueryExpr) =
    let sb = new System.Text.StringBuilder()
    let inline (!) (p:string) = sb.Append p |> ignore
    let inline writeOp o = ! (match o with Value id -> id | Attribute id -> id)
    let inline writeCmp cmp l r = ! "( " ; writeOp l ; ! cmp ; writeOp r ; ! " )"
    let rec aux q =
        match q with
        | False -> ! "false"
        | True -> ! "true"
        | Not q -> ! "( NOT " ; aux q ; ! " )"
        | And (l,r) -> ! "( " ; aux l ; ! " AND " ; aux r ; ! " )"
        | Or (l,r) -> ! "( " ; aux l ; ! " OR " ; aux r ; ! " )"
        | BooleanAttribute attr -> ! attr
        | Compare (EQ, l, r) -> writeCmp " = " l r
        | Compare (NEQ, l, r) -> writeCmp " <> " l r
        | Compare (LT, l, r) -> writeCmp " < " l r
        | Compare (GT, l, r) -> writeCmp " > " l r
        | Compare (LE, l, r) -> writeCmp " <= " l r
        | Compare (GE, l, r) -> writeCmp " >= " l r
        | Between (v,l,u) -> ! "( " ; writeOp v ; ! " BETWEEN " ; writeOp l ; ! " AND " ; writeOp u ; ! " )"
        | BeginsWith (attr, valId) -> ! "( begins_with ( " ; ! attr ; ! ", " ; ! valId ; !" ))"
        | Contains (attr, op) -> ! "( contains ( " ; !attr ; !", " ; writeOp op ; !" ))"
        | SizeOf attr -> ! "( size ( " ; !attr ; ! " ))"

    aux qExpr
    sb.ToString()

let extractQueryExpr (recordInfo : RecordInfo) (expr : Expr<'TRecord -> bool>) =
    let invalidQuery() = invalidArg "expr" <| sprintf "Supplied expression is not a valid conditional."
    let values = new Dictionary<FsAttributeValue, string> ()
    let getValueExpr (conv : FieldConverter) (expr : Expr) =
        let o = evalRaw expr
        let fav = conv.OfFieldUntyped o
        let ok,found = values.TryGetValue fav
        if ok then found
        else
            let name = sprintf ":val%d" values.Count
            values.Add(fav, name)
            name

    let attributes = new Dictionary<string, string> ()
    let getAttr (rp : RecordProperty) =
        let ok, found = attributes.TryGetValue rp.Name
        if ok then found
        else
            let name = sprintf "#ATTR%d" attributes.Count
            attributes.Add(rp.Name, name)
            name

    let (|RecordPropertyGet|_|) (e : Expr) =
        match e with
        | PropertyGet(Some (Var _), p, []) when p.DeclaringType = recordInfo.RecordType ->
            recordInfo.Properties |> Array.tryFind(fun rp -> rp.PropertyInfo = p)
        | _ -> None

    let (|Operands|) (exprs : Expr list) =
        match exprs |> List.tryPick (|RecordPropertyGet|_|) with
        | Some rp ->
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
        | SpecificCall <@ not @> (None, _, [body]) -> Not (extractQuery body)
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
        | RecordPropertyGet rp -> BooleanAttribute(getAttr rp)

        | Comparison <@ (=) @> (left, right) -> Compare(EQ, left, right)
        | Comparison <@ (<>) @> (left, right) -> Compare(NEQ, left, right)
        | Comparison <@ (<) @> (left, right) -> Compare(LT, left, right)
        | Comparison <@ (>) @> (left, right) -> Compare(GT, left, right)
        | Comparison <@ (<=) @> (left, right) -> Compare(LE, left, right)
        | Comparison <@ (>=) @> (left, right) -> Compare(GE, left, right)

        | SpecificCall <@ fun (x:string) -> x.StartsWith "" @> (Some (RecordPropertyGet rp), _, [value]) when value.IsClosed ->
            let attrId = getAttr rp
            let valId = getValueExpr rp.Converter value
            BeginsWith(attrId, valId)

        | SpecificCall <@ fun (x:string) -> x.Contains "" @> (Some (RecordPropertyGet rp), _, [value]) when value.IsClosed ->
            let attrId = getAttr rp
            let valId = getValueExpr rp.Converter value
            Contains(attrId, Value valId)

        | SpecificCall <@ Set.contains @> (None, _, [elem; RecordPropertyGet rp]) when elem.IsClosed ->
            let attrId = getAttr rp
            let fconv = rp.PropertyInfo.PropertyType.GetGenericArguments().[0] |> FieldConverter.resolveUntyped
            let valId = getValueExpr fconv elem
            Contains(attrId, Value valId)

        | _ -> invalidQuery()

    match expr with
    | Lambda(x, body) when x.Type = typeof<'TRecord> -> 
        let q = extractQuery body
        {
            QueryExpr = q
            Expression = queryExprToString q
            Attributes = attributes |> Seq.map (fun kv -> kv.Value, kv.Key) |> Map.ofSeq
            Values = values |> Seq.map (fun kv -> kv.Value, kv.Key) |> Map.ofSeq
        }

    | _ -> invalidQuery()