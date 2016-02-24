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
    | BeginsWith of AttributePath * Operand
    | Contains of AttributePath * Operand
    | Attribute_Exists of AttributePath
    | Attribute_Not_Exists of AttributePath

and Comparator =
    | EQ
    | NE
    | LT
    | GT
    | LE
    | GE

and Operand =
    | Undefined
    | Value of AttributeValue
    | Attribute of AttributePath
    | SizeOf of AttributePath

/// Extracts a query expression from a quoted F# predicate
let extractQueryExpr (recordInfo : RecordInfo) (expr : Expr<'TRecord -> bool>) : QueryExpr =
    if not expr.IsClosed then invalidArg "expr" "supplied query is not a closed expression."
    let invalidQuery() = invalidArg "expr" <| sprintf "Supplied expression is not a valid conditional."

    match expr with
    | Lambda(r, body) ->

        let getAttrValue (pickler : Pickler) (expr : Expr) =
            expr |> evalRaw |> pickler.PickleCoerced

        let (|AttributeGet|_|) e = 
            match AttributePath.TryExtract r recordInfo e with
            | None -> None
            | Some attr as aopt ->
                attr.Iter (fun pickler -> 
                    if pickler.PicklerType = PicklerType.Serialized then 
                        invalidArg "expr" "cannot perform queries on serialized attributes.")

                aopt

        let rec extractOperand (pickler : Pickler option) (expr : Expr) =
            match expr with
            | _ when expr.IsClosed ->
                let pickler = match pickler with Some c -> c | None -> Pickler.resolveUntyped expr.Type
                match getAttrValue pickler expr with
                | None -> Undefined
                | Some av -> Value av

            | PipeLeft e
            | PipeRight e -> extractOperand pickler e

            | AttributeGet attr -> Attribute attr

            | SpecificProperty <@ fun (s : string) -> s.Length @> (Some (AttributeGet attr), _, []) -> 
                SizeOf attr

            | SpecificProperty <@ fun (l : _ list) -> l.Length @> (Some (AttributeGet attr), _, []) -> 
                SizeOf attr

            | SpecificCall2 <@ List.length @> (None, _, _, [AttributeGet attr]) -> 
                SizeOf attr

            | SpecificProperty <@ fun (s : Set<_>) -> s.Count @> (Some (AttributeGet attr), _, []) -> 
                SizeOf attr

            | SpecificCall2 <@ Set.count @> (None, _, _, [AttributeGet attr]) -> 
                SizeOf attr

            | SpecificProperty <@ fun (m : Map<_,_>) -> m.Count @> (Some (AttributeGet attr), _, []) -> 
                SizeOf attr

            | SpecificProperty <@ fun (a : _ []) -> a.Length @> (Some (AttributeGet attr), _, []) -> 
                SizeOf attr

            | SpecificCall2 <@ Array.length @> (None, _, _, [AttributeGet attr]) -> 
                SizeOf attr

            | _ -> invalidQuery()

        let (|Comparison|_|) (pat : Expr) (expr : Expr) =
            match expr with
            | SpecificCall pat (None, _, args) ->
                let pickler = args |> List.tryPick (|AttributeGet|_|) |> Option.map (fun a -> a.Pickler)
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

            | AttributeGet attr -> Compare(EQ, Attribute attr, Value (AttributeValue(BOOL = true)))

            | Comparison <@ (=) @> [left; right] -> extractComparison EQ left right
            | Comparison <@ (<>) @> [left; right] -> extractComparison NE left right
            | Comparison <@ (<) @> [left; right] -> extractComparison LT left right
            | Comparison <@ (>) @> [left; right] -> extractComparison GT left right
            | Comparison <@ (<=) @> [left; right] -> extractComparison LE left right
            | Comparison <@ (>=) @> [left; right] -> extractComparison GE left right

            | SpecificCall2 <@ fun (x:string) y -> x.StartsWith y @> (Some (AttributeGet attr), _, _, [value]) ->
                let op = extractOperand None value
                BeginsWith(attr, op)

            | SpecificCall2 <@ fun (x:string) y -> x.Contains y @> (Some (AttributeGet attr), _, _, [value]) ->
                let op = extractOperand None value
                Contains(attr, op)

            | SpecificCall2 <@ Set.contains @> (None, _, _, [elem; AttributeGet attr]) ->
                let ep = getElemPickler attr.Pickler
                let op = extractOperand (Some ep) elem
                Contains(attr, op)

            | SpecificCall2 <@ fun (x:Set<_>) e -> x.Contains e @> (Some(AttributeGet attr), _, _, [elem]) ->
                let ep = getElemPickler attr.Pickler
                let op = extractOperand (Some ep) elem
                Contains(attr, op)

            | SpecificCall2 <@ Map.containsKey @> (None, _, _, [key; AttributeGet attr]) when key.IsClosed ->
                let key = evalRaw key
                if not <| isValidFieldName key then
                    invalidArg key "map key must be alphanumeric not starting with a digit"

                Attribute_Exists (Suffix(key, attr))

            | SpecificCall2 <@ fun (x : Map<_,_>) y -> x.ContainsKey y @> (Some(AttributeGet attr), _, _, [key]) when key.IsClosed ->
                let key = evalRaw key
                if not <| isValidFieldName key then
                    invalidArg key "map key must be alphanumeric not starting with a digit"

                Attribute_Exists (Suffix(key, attr))

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

            | _ -> invalidQuery()

        extractQuery body

    | _ -> invalidQuery()

/// prints a query expression to string recognizable by the DynamoDB APIs
let queryExprToString (getAttrId : AttributePath -> string) 
                        (getValueId : AttributeValue -> string) (qExpr : QueryExpr) =

    let sb = new System.Text.StringBuilder()
    let inline (!) (p:string) = sb.Append p |> ignore
    let inline writeOp o = 
        match o with 
        | Undefined -> invalidOp "internal error: attempting to reference undefined value in query expression."
        | Value v -> !(getValueId v) 
        | Attribute a -> !(getAttrId a) 
        | SizeOf a -> ! "( size ( " ; !(getAttrId a) ; ! " ))"

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

    let rec aux q =
        match q with
        | False | True -> invalidOp "internal error: invalid query representation"
        | Not q -> ! "( NOT " ; aux q ; ! " )"
        | And (l,r) -> ! "( " ; aux l ; ! " AND " ; aux r ; ! " )"
        | Or (l,r) -> ! "( " ; aux l ; ! " OR " ; aux r ; ! " )"
        | Compare (cmp, l, r) -> ! "( " ; writeOp l ; writeCmp cmp ; writeOp r ; ! " )"
        | Between (v,l,u) -> ! "( " ; writeOp v ; ! " BETWEEN " ; writeOp l ; ! " AND " ; writeOp u ; ! " )"
        | BeginsWith (attr, op) -> ! "( begins_with ( " ; !(getAttrId attr) ; ! ", " ;  writeOp op ; !" ))"
        | Contains (attr, op) -> ! "( contains ( " ; !(getAttrId attr) ; !", " ; writeOp op ; !" ))"
        | Attribute_Exists attr -> ! "( attribute_exists ( " ; !(getAttrId attr) ; ! "))"
        | Attribute_Not_Exists attr -> ! "( attribute_not_exists ( " ; !(getAttrId attr) ; ! "))"
        | In(op,ops) -> ! "(" ; writeOp op ; ! " IN (" ; writeOps ops ; ! "))"

    aux qExpr
    sb.ToString()

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
        | BeginsWith(ap,_) when ap.RootProperty.IsHashKey -> false
        | BeginsWith(ap,_) when ap.RootProperty.IsRangeKey -> incr rangeKeyRefs ; true
        | BeginsWith _ -> false
        | Between (Attribute ap, Value _, Value _) ->
            if ap.RootProperty.IsRangeKey then incr rangeKeyRefs ; true
            else false

        | Between _ -> false
        | Compare(cmp, Attribute ap, Value _)
        | Compare(cmp, Value _, Attribute ap) ->
            if ap.RootProperty.IsHashKey then
                incr hashKeyRefs ; cmp = EQ
            elif ap.RootProperty.IsRangeKey then
                incr rangeKeyRefs ; true
            else
                false

        | Compare _ -> false

    if aux qExpr then !hashKeyRefs = 1 && !rangeKeyRefs <= 1
    else false


let private attrValueCmp = new AttributeValueComparer() :> IEqualityComparer<_>

/// Conditional expression parsed form holder
[<CustomEquality; NoComparison>]
type ConditionalExpression =
    {
        /// Query conditional expression
        QueryExpr : QueryExpr
        /// Specifies whether this query is compatible with DynamoDB key conditions
        IsQueryCompatible : bool
        /// Expression string
        Expression : string
        /// Expression attribute names
        Attributes : (string * string) []
        /// Expression attribute values
        Values : (string * AttributeValue) []
    }
with
    member __.WriteAttributesTo(target : Dictionary<string,string>) =
        for k,v in __.Attributes do target.[k] <- v

    member __.WriteValuesTo(target : Dictionary<string, AttributeValue>) =
        for k,v in __.Values do target.[k] <- v

    /// Extracts condition expression from given quoted F# predicate
    static member Extract (recordInfo : RecordInfo) (expr : Expr<'TRecord -> bool>) =
        match extractQueryExpr recordInfo expr with
        | False | True -> invalidArg "expr" "supplied query is tautological."
        | query ->

        let attrs = new Dictionary<string, string> ()
        let getAttrId (attr : AttributePath) =
            let rp = attr.RootProperty
            let ok,found = attrs.TryGetValue rp.AttrId
            if ok then attr.Id
            else
                attrs.Add(rp.AttrId, rp.Name)
                attr.Id

        let values = new Dictionary<AttributeValue, string>(attrValueCmp)
        let getValueId (av : AttributeValue) =
            let ok,found = values.TryGetValue av
            if ok then found
            else
                let id = sprintf ":cval%d" values.Count
                values.Add(av, id)
                id

        let exprString = queryExprToString getAttrId getValueId query
        let isQueryCompatible = isKeyConditionCompatible query

        {
            QueryExpr = query
            Expression = exprString
            IsQueryCompatible = isQueryCompatible
            Attributes = attrs |> Seq.map (fun kv -> kv.Key, kv.Value) |> Seq.sortBy fst |> Seq.toArray
            Values = values |> Seq.map (fun kv -> kv.Value, kv.Key) |> Seq.sortBy fst |> Seq.toArray
        }

    /// Append expression variables to existing state, while building a new expression string
    /// Used for combined key and filter conditions
    member cexpr.BuildAppendedConditional(attrs : Dictionary<string, string>, values : Dictionary<string, AttributeValue>) : string =
        let getAttrId (attr : AttributePath) =
            let rp = attr.RootProperty
            let ok,found = attrs.TryGetValue rp.AttrId
            if ok then attr.Id
            else
                attrs.Add(rp.AttrId, rp.Name)
                attr.Id

        let lvals = new Dictionary<AttributeValue, string>(attrValueCmp)
        let getValueId (av : AttributeValue) =
            let ok,found = lvals.TryGetValue av
            if ok then found
            else
                let id = sprintf ":cval%d" values.Count
                lvals.Add(av, id)
                values.Add(id, av)
                id

        queryExprToString getAttrId getValueId cexpr.QueryExpr

    override cexpr.Equals obj =
        match obj with
        | :? ConditionalExpression as cexpr' ->
            cexpr.Expression = cexpr'.Expression &&
            cexpr.Values.Length = cexpr'.Values.Length &&
            let eq (k,v) (k',v') = k = k' && attrValueCmp.Equals(v,v') in
            Array.forall2 eq cexpr.Values cexpr'.Values
        | _ -> false

    override cexpr.GetHashCode() =
        let mutable vhash = 0
        for k,v in cexpr.Values do
            let th = combineHash (hash k) (attrValueCmp.GetHashCode v)
            vhash <- combineHash vhash th

        hash2 cexpr.Expression vhash