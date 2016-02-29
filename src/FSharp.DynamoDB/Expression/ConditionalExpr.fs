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

open FSharp.DynamoDB.ExprCommon

//
//  Converts an F# quotation into an appropriate DynamoDB conditional expression.
//
//  see http://docs.aws.amazon.com/amazondynamodb/latest/developerguide/Expressions.SpecifyingConditions.html

/// DynamoDB query expression
type QueryExpr =
    | False // True & False not part of the DynamoDB spec;
    | True  // used here for simplifying and identifying tautological conditions
    | Not of QueryExpr
    | And of QueryExpr * QueryExpr
    | Or of QueryExpr * QueryExpr
    | Compare of Comparator * Operand * Operand
    | Between of Operand * Operand * Operand
    | In of Operand * Operand list
    | BeginsWith of AttributeId * Operand
    | Contains of AttributeId * Operand
    | Attribute_Exists of AttributeId
    | Attribute_Not_Exists of AttributeId

and Comparator =
    | EQ
    | NE
    | LT
    | GT
    | LE
    | GE

and Operand =
    | Undefined
    | Value of AttributeValueEqWrapper
    | Param of index:int * Pickler
    | Attribute of AttributeId
    | SizeOf of AttributeId

/// Conditional expression parsed from quotation expr
type ConditionalExpression =
    {
        /// Query conditional expression
        QueryExpr : QueryExpr
        /// Specifies whether this query is compatible with DynamoDB key conditions
        IsKeyConditionCompatible : bool
        /// If query expression is parametric, specifies an array of picklers for input parameters
        NParams : int
    }
with
    member __.IsParametericExpr = __.NParams > 0

// A DynamoDB key condition expression must satisfy the following conditions:
// 1. Must only reference HashKey & RangeKey attributes.
// 2. Must reference HashKey attribute exactly once.
// 3. Must reference RangeKey attribute at most once.
// 4. HashKey comparison must be equality comparison only.
// 5. Must not contain OR and NOT clauses.
// 6. Must not contain nested operands.
let isKeyConditionCompatible (qExpr : QueryExpr) =
    let hashKeyRefs  = ref 0
    let rangeKeyRefs = ref 0
    let rec aux qExpr =
        match qExpr with
        | False | True
        | Attribute_Exists _
        | Attribute_Not_Exists _
        | Contains _
        | In _
        | Not _
        | Or _ -> false
        | And(l,r) -> aux l && aux r
        | BeginsWith(attr,_) when attr.IsHashKey -> false
        | BeginsWith(attr,_) when attr.IsRangeKey -> incr rangeKeyRefs ; true
        | BeginsWith _ -> false
        | Between (Attribute attr, (Value _ | Param _), (Value _ | Param _)) ->
            if attr.IsRangeKey then incr rangeKeyRefs ; true
            else false

        | Between _ -> false
        | Compare(cmp, Attribute attr, (Value _ | Param _))
        | Compare(cmp, (Value _ | Param _), Attribute attr) ->
            if attr.IsHashKey then
                incr hashKeyRefs ; cmp = EQ
            elif attr.IsRangeKey then
                incr rangeKeyRefs ; true
            else
                false

        | Compare _ -> false

    if aux qExpr then !hashKeyRefs = 1 && !rangeKeyRefs <= 1
    else false


let ensureNotTautological query =
    match query with
    | False | True -> invalidArg "expr" "supplied query is tautological."
    | _ -> ()

/// Extracts a query expression from a quoted F# predicate
let extractQueryExpr (recordInfo : RecordInfo) (expr : Expr) : ConditionalExpression =
    if not expr.IsClosed then invalidArg "expr" "supplied query is not a closed expression."
    let invalidQuery() = invalidArg "expr" <| sprintf "Supplied expression is not a valid conditional."

    let nParams, (|PVar|_|), expr' = extractExprParams recordInfo expr

    match expr' with
    | Lambda(r,body) when r.Type = recordInfo.Type ->

        let getAttrValue (pickler : Pickler) (expr : Expr) =
            expr |> evalRaw |> pickler.PickleCoerced

        let (|AttributeGet|_|) e = 
            match QuotedAttribute.TryExtract r recordInfo e with
            | None -> None
            | Some qa as aopt ->
                qa.Iter (fun pickler -> 
                    if pickler.PicklerType = PicklerType.Serialized then 
                        invalidArg "expr" "cannot perform queries on serialized attributes.")

                aopt

        let rec extractOperand (pickler : Pickler option) (expr : Expr) =
            match expr with
            | _ when expr.IsClosed ->
                let pickler = match pickler with Some c -> c | None -> Pickler.resolveUntyped expr.Type
                match getAttrValue pickler expr with
                | None -> Undefined
                | Some av -> Value (wrap av)

            | Coerce(e,_)
            | PipeLeft e
            | PipeRight e -> extractOperand pickler e

            | AttributeGet attr -> Attribute attr.Id
            | PVar i -> 
                let pickler = match pickler with Some p -> p | None -> Pickler.resolveUntyped expr.Type
                Param(i, pickler)

            | SpecificProperty <@ fun (s : string) -> s.Length @> (Some (AttributeGet attr), _, []) -> 
                SizeOf attr.Id

            | SpecificProperty <@ fun (l : _ list) -> l.Length @> (Some (AttributeGet attr), _, []) -> 
                SizeOf attr.Id

            | SpecificCall2 <@ List.length @> (None, _, _, [AttributeGet attr]) -> 
                SizeOf attr.Id

            | SpecificProperty <@ fun (s : Set<_>) -> s.Count @> (Some (AttributeGet attr), _, []) -> 
                SizeOf attr.Id

            | SpecificCall2 <@ Set.count @> (None, _, _, [AttributeGet attr]) -> 
                SizeOf attr.Id

            | SpecificProperty <@ fun (m : Map<_,_>) -> m.Count @> (Some (AttributeGet attr), _, []) -> 
                SizeOf attr.Id

            | SpecificProperty <@ fun (a : _ []) -> a.Length @> (Some (AttributeGet attr), _, []) -> 
                SizeOf attr.Id

            | SpecificCall2 <@ Array.length @> (None, _, _, [AttributeGet attr]) -> 
                SizeOf attr.Id

            | _ -> invalidQuery()

        let (|Comparison|_|) (pat : Expr) (expr : Expr) =
            match expr with
            | SpecificCall pat (None, _, args) ->
                let pickler = args |> List.tryPick (|AttributeGet|_|) |> Option.map (fun attr -> attr.Pickler)
                args |> List.map (extractOperand pickler) |> Some

            | _ -> None

        let extractComparison (cmp : Comparator) (left : Operand) (right : Operand) =
            if left = right then
                match cmp with
                | LE | EQ | GE -> True
                | LT | NE | GT -> False
            else

            let assignUndefined op =
                match op with
                | Attribute attr ->
                    match cmp with
                    | NE -> Attribute_Exists attr
                    | EQ -> Attribute_Not_Exists attr
                    | _ -> True
                | _ -> invalidOp "internal error; assigning undefined value to non attribute path."

            if left = Undefined then assignUndefined right
            elif right = Undefined then assignUndefined left
            else 
                Compare(cmp, left, right)

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

            | AttributeGet attr -> Compare(EQ, Attribute attr.Id, Value (wrap (AttributeValue(BOOL = true))))

            | Comparison <@ (=) @> [left; right] -> extractComparison EQ left right
            | Comparison <@ (<>) @> [left; right] -> extractComparison NE left right
            | Comparison <@ (<) @> [left; right] -> extractComparison LT left right
            | Comparison <@ (>) @> [left; right] -> extractComparison GT left right
            | Comparison <@ (<=) @> [left; right] -> extractComparison LE left right
            | Comparison <@ (>=) @> [left; right] -> extractComparison GE left right

            | SpecificCall2 <@ fun (x:string) y -> x.StartsWith y @> (Some (AttributeGet attr), _, _, [value]) ->
                let op = extractOperand None value
                BeginsWith(attr.Id, op)

            | SpecificCall2 <@ fun (x:string) y -> x.Contains y @> (Some (AttributeGet attr), _, _, [value]) ->
                let op = extractOperand None value
                Contains(attr.Id, op)

            | SpecificCall2 <@ Set.contains @> (None, _, _, [elem; AttributeGet attr]) ->
                let ep = getElemPickler attr.Pickler
                let op = extractOperand (Some ep) elem
                Contains(attr.Id, op)

            | SpecificCall2 <@ fun (x:Set<_>) e -> x.Contains e @> (Some(AttributeGet attr), _, _, [elem]) ->
                let ep = getElemPickler attr.Pickler
                let op = extractOperand (Some ep) elem
                Contains(attr.Id, op)

            | SpecificCall2 <@ Map.containsKey @> (None, _, _, [key; AttributeGet attr]) when key.IsClosed ->
                let key = evalRaw key
                if not <| isValidFieldName key then
                    invalidArg key "map key must be alphanumeric not starting with a digit"

                Attribute_Exists (attr.Id.Append key)

            | SpecificCall2 <@ fun (x : Map<_,_>) y -> x.ContainsKey y @> (Some(AttributeGet attr), _, _, [key]) when key.IsClosed ->
                let key = evalRaw key
                if not <| isValidFieldName key then
                    invalidArg key "map key must be alphanumeric not starting with a digit"

                Attribute_Exists (attr.Id.Append key)

            | SpecificCall2 <@ BETWEEN @> (None, _, _, [value ; lower; upper]) ->
                let pickler = Pickler.resolveUntyped value.Type
                if pickler.IsScalar then
                    let sc = Some pickler
                    let vOp = extractOperand sc value
                    let lOp = extractOperand sc lower
                    let uOp = extractOperand sc upper
                    Between(vOp, lOp, uOp)
                else
                    invalidArg "expr" "BETWEEN predicate only applies to scalar attributes."

            | SpecificCall2 <@ EXISTS @> (None, _, _, [AttributeGet attr]) ->
                Attribute_Exists attr.Id

            | SpecificCall2 <@ NOT_EXISTS @> (None, _, _, [AttributeGet attr]) ->
                Attribute_Not_Exists attr.Id

            | _ -> invalidQuery()

        let queryExpr = extractQuery body
        ensureNotTautological queryExpr
        {
            QueryExpr = queryExpr
            NParams = nParams
            IsKeyConditionCompatible = isKeyConditionCompatible queryExpr
        }

    | _ -> invalidQuery()

/// applies a set of input values to a parametric query expression
let applyParams (cond : ConditionalExpression) (inputValues : obj[]) =
    let applyOperand (op : Operand) =
        match op with
        | Param (i, pickler) ->
            match pickler.PickleCoerced inputValues.[i] with
            | Some av -> Value (wrap av)
            | None -> Undefined

        | _ -> op

    let rec applyQuery q =
        match q with
        | False | True
        | Attribute_Exists _ | Attribute_Not_Exists _ -> q
        | Not q -> 
            match applyQuery q with
            | Not q' -> q'
            | q' -> Not q'

        | And(q, q') -> 
            match applyQuery q, applyQuery q' with
            | False, _ | _, False -> False
            | q, True  | True, q  -> q
            | q, q' -> And(q,q')

        | Or(q, q') -> 
            match applyQuery q, applyQuery q' with
            | True, _  | _, True  -> True
            | q, False | q, False -> q
            | q, q' -> Or(q, q')

        | In(o, os) -> In(applyOperand o, List.map applyOperand os)
        | Between(x, l, u) -> Between(applyOperand x, applyOperand l, applyOperand u)
        | BeginsWith(attr, o) -> BeginsWith(attr, applyOperand o)
        | Contains(attr, o) -> Contains(attr, applyOperand o)
        | Compare(cmp, l, r) ->
            let l' = applyOperand l
            let r' = applyOperand r
            let assignUndefined op =
                match op with
                | Attribute attr ->
                    match cmp with
                    | NE -> Attribute_Exists attr
                    | EQ -> Attribute_Not_Exists attr
                    | _ -> True
                | _ -> invalidOp "internal error; assigning undefined value to non attribute path."

            if l' = Undefined then assignUndefined r'
            elif r' = Undefined then assignUndefined l'
            else Compare(cmp, l', r')

    let reduced = applyQuery cond.QueryExpr
    ensureNotTautological reduced
    { cond with QueryExpr = reduced ; NParams = 0 }

/// prints a query expression to string recognizable by the DynamoDB APIs
let writeConditionExpression (writer : AttributeWriter) (cond : ConditionalExpression) =
    let sb = new System.Text.StringBuilder()
    let inline (!) (p:string) = sb.Append p |> ignore
    let inline writeOp o = 
        match o with 
        | Undefined -> invalidOp "internal error: attempting to reference undefined value in query expression."
        | Param _ -> invalidOp "internal error: attempting to reference parameter value in query expression."
        | Value v -> !(writer.WriteValue (unwrap v))
        | Attribute a -> !(writer.WriteAttibute a)
        | SizeOf a -> ! "( size ( " ; !(writer.WriteAttibute a) ; ! " ))"

    let rec writeOps ops =
        match ops with
        | o :: [] -> writeOp o
        | o :: tl -> writeOp o ; ! ", " ; writeOps tl
        | [] -> invalidOp "Internal error: empty operand sequence."

    let inline writeCmp cmp =
        match cmp with
        | EQ -> ! " = "
        | NE -> ! " <> "
        | LT -> ! " < "
        | GT -> ! " > "
        | GE -> ! " >= "
        | LE -> ! " <= "

    let rec writeQuery q =
        match q with
        | False | True -> invalidOp "internal error: invalid query representation"
        | Not q -> ! "( NOT " ; writeQuery q ; ! " )"
        | And (l,r) -> ! "( " ; writeQuery l ; ! " AND " ; writeQuery r ; ! " )"
        | Or (l,r) -> ! "( " ; writeQuery l ; ! " OR " ; writeQuery r ; ! " )"
        | Compare (cmp, l, r) -> ! "( " ; writeOp l ; writeCmp cmp ; writeOp r ; ! " )"
        | Between (v,l,u) -> ! "( " ; writeOp v ; ! " BETWEEN " ; writeOp l ; ! " AND " ; writeOp u ; ! " )"
        | BeginsWith (attr, op) -> ! "( begins_with ( " ; !(writer.WriteAttibute attr) ; ! ", " ;  writeOp op ; !" ))"
        | Contains (attr, op) -> ! "( contains ( " ; !(writer.WriteAttibute attr) ; !", " ; writeOp op ; !" ))"
        | Attribute_Exists attr -> ! "( attribute_exists ( " ; !(writer.WriteAttibute attr) ; ! "))"
        | Attribute_Not_Exists attr -> ! "( attribute_not_exists ( " ; !(writer.WriteAttibute attr) ; ! "))"
        | In(op,ops) -> ! "(" ; writeOp op ; ! " IN (" ; writeOps ops ; ! "))"

    writeQuery cond.QueryExpr
    sb.ToString()

/// Generates a conditional that verifies whether a given item exists
let mkItemExistsCondition (schema : TableKeySchema) =
    {
        QueryExpr = AttributeId.FromKeySchema schema |> Attribute_Exists
        IsKeyConditionCompatible = true
        NParams = 0
    }

/// Generates a conditional that verifies whether a given item does not exist
let mkItemNotExistsCondition (schema : TableKeySchema) =
    {
        QueryExpr = AttributeId.FromKeySchema schema |> Attribute_Not_Exists
        IsKeyConditionCompatible = true
        NParams = 0
    }

type ConditionalExpression with
    member cond.Apply([<ParamArray>]parameters : obj[]) =
        applyParams cond parameters

    member cond.Write (writer : AttributeWriter) =
        writeConditionExpression writer cond

    member cond.GetDebugData() =
        let aw = new AttributeWriter()
        let expr = writeConditionExpression aw cond
        let names = aw.Names |> Seq.map (fun kv -> kv.Key, kv.Value) |> Seq.toList
        let values = aw.Values |> Seq.map (fun kv -> kv.Key, kv.Value.Print()) |> Seq.toList
        expr, names, values

    static member Extract (recordInfo : RecordInfo) (expr : Expr) =
        extractQueryExpr recordInfo expr