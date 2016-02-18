module internal FSharp.DynamoDB.ConditionalExpr

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
    | SizeOf of attr:string

type ConditionalExpression =
    {
        QueryExpr : QueryExpr
        Expression : string
        Attributes : Map<string, RecordPropertyInfo>
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
    let inline writeOp o = match o with Value id | Attribute id -> !id | SizeOf id -> ! "( size ( " ; !id ; ! " ))"
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

    aux qExpr
    sb.ToString()

let extractQueryExpr (recordInfo : RecordInfo) (expr : Expr<'TRecord -> bool>) =
    if not expr.IsClosed then invalidArg "expr" "supplied query is not a closed expression."
    let invalidQuery() = invalidArg "expr" <| sprintf "Supplied expression is not a valid conditional."

    match expr with
    | Lambda(r, body) ->
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
            let fav = conv.OfFieldUntyped o |> FsAttributeValue.FromAttributeValue
            getValue fav

        let attributes = new Dictionary<RecordPropertyInfo, string> ()
        let getAttr (rp : RecordPropertyInfo list) =
            let ok, found = attributes.TryGetValue (List.head rp)
            let name =
                if ok then found
                else
                    let name = sprintf "#ATTR%d" attributes.Count
                    attributes.Add(List.head rp, name)
                    name

            seq {
                yield name
                for p in List.tail rp -> p.Name 
            } |> String.concat "."

        let (|RecordPropertyGet|_|) (e : Expr) =
            let rec extractProps props e =
                match e with
                | PropertyGet(Some (Var r'), p, []) when r = r' -> Some (Choice1Of2 p :: props)
                | PropertyGet(Some e, p, []) -> extractProps (Choice1Of2 p :: props) e
                | SpecificCall2 <@ fst @> (None, _, _, [e]) -> extractProps (Choice2Of2 1 :: props) e
                | SpecificCall2 <@ snd @> (None, _, _, [e]) -> extractProps (Choice2Of2 2 :: props) e
                | _ -> None

            let checkProperty isLast (rp : RecordPropertyInfo) =
                if rp.Converter.ConverterType = ConverterType.Serialized then
                    invalidArg "expr" "cannot perform queries on serialized properties."

                if isLast && rp.Converter.ConverterType = ConverterType.Record then
                    invalidArg "expr" "cannot perform queries on record values."

            let rec resolveRecordProps acc (ctx : RecordInfo option) curr =
                match curr with
                | [] -> Some(List.head acc, List.rev acc)
                | Choice1Of2 p :: tail ->
                    let rp = ctx.Value.Properties |> Array.find (fun rp -> rp.PropertyInfo = p)
                    checkProperty (List.isEmpty tail) rp
                    resolveRecordProps (rp :: acc) rp.NestedRecord tail

                | Choice2Of2 i :: tail -> 
                    let rp = ctx.Value.Properties.[i-1]
                    checkProperty (List.isEmpty tail) rp
                    resolveRecordProps (rp :: acc) rp.NestedRecord tail

            extractProps [] e |> Option.bind (resolveRecordProps [] (Some recordInfo))
                

        let extractOperand (rp : RecordPropertyInfo option) (expr : Expr) =
            match expr with
            | _ when expr.IsClosed ->
                let conv = match rp with Some rp -> rp.Converter | None -> FieldConverter.resolveUntyped expr.Type
                let id = getValueExpr conv expr
                Value id

            | RecordPropertyGet (_,path) ->
                let id = getAttr path
                Attribute id

            | SpecificProperty <@ fun (s : string) -> s.Length @> (Some (RecordPropertyGet (_,path)), _, []) ->
                let id = getAttr path
                SizeOf id

            | SpecificProperty <@ fun (l : _ list) -> l.Length @> (Some (RecordPropertyGet (_,path)), _, []) ->
                let id = getAttr path
                SizeOf id

            | SpecificProperty <@ List.length @> (None, _, [RecordPropertyGet (_,path)]) ->
                let id = getAttr path
                SizeOf id

            | SpecificProperty <@ fun (s : Set<_>) -> s.Count @> (Some (RecordPropertyGet (_,path)), _, []) ->
                let id = getAttr path
                SizeOf id

            | SpecificProperty <@ Set.count @> (None, _, [RecordPropertyGet (_,path)]) ->
                let id = getAttr path
                SizeOf id

            | SpecificProperty <@ fun (m : Map<_,_>) -> m.Count @> (Some (RecordPropertyGet (_,path)), _, []) ->
                let id = getAttr path
                SizeOf id

            | SpecificProperty <@ fun (a : _ []) -> a.Length @> (Some (RecordPropertyGet (_,path)), _, []) ->
                let id = getAttr path
                SizeOf id

            | SpecificProperty <@ Array.length @> (None, _, [RecordPropertyGet (_,path)]) ->
                let id = getAttr path
                SizeOf id

            | _ -> invalidQuery()

        let (|Comparison|_|) (pat : Expr) (expr : Expr) =
            match expr with
            | SpecificCall pat (None, _, args) ->
                let rp = args |> List.tryPick (|RecordPropertyGet|_|) |> Option.map fst
                args |> List.map (extractOperand rp) |> Some

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

            | RecordPropertyGet (_,rp) -> 
                let attr = getAttr rp |> Attribute
                let value = getValue (Bool true) |> Value
                Compare(EQ, attr, value)

            | Comparison <@ (=) @> [left; right] -> Compare(EQ, left, right)
            | Comparison <@ (<>) @> [left; right] -> Compare(NE, left, right)
            | Comparison <@ (<) @> [left; right] -> Compare(LT, left, right)
            | Comparison <@ (>) @> [left; right] -> Compare(GT, left, right)
            | Comparison <@ (<=) @> [left; right] -> Compare(LE, left, right)
            | Comparison <@ (>=) @> [left; right] -> Compare(GE, left, right)

            | SpecificCall2 <@ fun (x:string) y -> x.StartsWith y @> (Some (RecordPropertyGet (rp,path)), _, _, [value]) when value.IsClosed ->
                let attrId = getAttr path
                let valId = getValueExpr rp.Converter value
                BeginsWith(attrId, valId)

            | SpecificCall2 <@ fun (x:string) y -> x.Contains y @> (Some (RecordPropertyGet (rp, path)), _, _, [value]) when value.IsClosed ->
                let attrId = getAttr path
                let valId = extractOperand (Some rp) value
                Contains(attrId, valId)

            | SpecificCall2 <@ Set.contains @> (None, _, _, [elem; RecordPropertyGet (rp,path)]) when elem.IsClosed ->
                let attrId = getAttr path
                let valId = extractOperand (Some rp) elem
                Contains(attrId, valId)

            | SpecificCall2 <@ fun (x:Set<_>) e -> x.Contains e @> (Some(RecordPropertyGet (rp,path)), _, _, [elem]) when elem.IsClosed ->
                let attrId = getAttr path
                let valId = extractOperand (Some rp) elem
                Contains(attrId, valId)

            | SpecificCall2 <@ fun (x : HashSet<_>) y -> x.Contains y @> (Some(RecordPropertyGet (rp,path)), _, _, [elem]) when elem.IsClosed ->
                let attrId = getAttr path
                let valId = extractOperand (Some rp) elem
                Contains(attrId, valId)

            | SpecificCall2 <@ Map.containsKey @> (None, _, _, [elem; RecordPropertyGet (rp,path)]) when elem.IsClosed ->
                let attrId = getAttr path
                let valId = extractOperand (Some rp) elem
                Contains(attrId, valId)

            | SpecificCall2 <@ fun (x : Map<_,_>) y -> x.ContainsKey y @> (Some(RecordPropertyGet (rp,path)), _, _, [elem]) when elem.IsClosed ->
                let attrId = getAttr path
                let valId = extractOperand (Some rp) elem
                Contains(attrId, valId)

            | SpecificCall2 <@ fun (x : Dictionary<_,_>) y -> x.ContainsKey y @> (Some(RecordPropertyGet (rp,path)), _, _, [elem]) when elem.IsClosed ->
                let attrId = getAttr path
                let valId = extractOperand (Some rp) elem
                Contains(attrId, valId)

            | _ -> invalidQuery()

        match extractQuery body with
        | False | True -> invalidArg "expr" "supplied query is tautological."
        | q ->

        {
            QueryExpr = q
            Expression = queryExprToString q
            Attributes = attributes |> Seq.map (fun kv -> kv.Value, kv.Key) |> Map.ofSeq
            Values = values |> Seq.map (fun kv -> kv.Value, kv.Key) |> Map.ofSeq
        }

    | _ -> invalidQuery()