module internal FSharp.AWS.DynamoDB.ConditionalExpr

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

open FSharp.AWS.DynamoDB.ExprCommon

//
//  Converts an F# quotation into an appropriate DynamoDB conditional expression.
//
//  see http://docs.aws.amazon.com/amazondynamodb/latest/developerguide/Expressions.SpecifyingConditions.html

/// DynamoDB query expression
type QueryExpr =
    | False // True & False not part of the DynamoDB spec;
    | True  // used here for reducing condition expressions
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
with
    member cmp.IsComparison =
        match cmp with
        | EQ | NE -> false
        | _ -> true

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
        KeyCondition : TableKeySchema option
        /// If query expression is parametric, specifies an array of picklers for input parameters
        NParams : int
    }
with
    member __.IsParametericExpr = __.NParams > 0

type QueryExpr with
    static member EAnd l r =
        match l with
        | False -> False
        | True -> r
        | _ ->
            match r with
            | False -> False
            | True -> l
            | _ -> And(l,r)

    static member EOr l r =
        match l with
        | True -> True
        | False -> r
        | _ ->
            match r with
            | True -> True
            | False -> l
            | _ -> Or(l,r)

    static member ENot q =
        match q with
        | Not q -> q
        | q -> Not q

// A DynamoDB key condition expression must satisfy the following conditions:
// 1. Must only reference HashKey & RangeKey attributes.
// 2. Must reference HashKey attribute exactly once.
// 3. Must reference RangeKey attribute at most once.
// 4. HashKey comparison must be equality comparison only.
// 5. Must not contain OR and NOT clauses.
// 6. Must not contain nested operands.
let extractKeyCondition (qExpr : QueryExpr) =
    let hashKeyRef = ref None
    let rangeKeyRef = ref None
    let trySet r v =
        if Option.isSome !r then false
        else r := Some v ; true
        
    let rec isKeyCond qExpr =
        match qExpr with
        | False | True
        | Attribute_Exists _
        | Attribute_Not_Exists _
        | Contains _
        | In _
        | Not _
        | Or _ -> false
        | And(l,r) -> isKeyCond l && isKeyCond r
        | BeginsWith(attr,_) when attr.IsRangeKey -> 
            trySet rangeKeyRef attr
        | BeginsWith(attr,_) when attr.IsHashKey -> false

        | BeginsWith _ -> false
        | Between (Attribute attr, (Value _ | Param _), (Value _ | Param _)) when attr.IsRangeKey -> trySet rangeKeyRef attr

        | Between _ -> false
        | Compare(cmp, Attribute attr, (Value _ | Param _))
        | Compare(cmp, (Value _ | Param _), Attribute attr) ->
            if attr.IsHashKey && cmp = EQ then
                trySet hashKeyRef attr
            elif attr.IsRangeKey then
                trySet rangeKeyRef attr
            else
                false

        | Compare _ -> false

    if isKeyCond qExpr then
        match !hashKeyRef with
        | None -> None
        | Some hk ->

        match !rangeKeyRef with
        | None -> hk.KeySchemata |> Array.tryPick (function (ks,KeyType.Hash) -> Some ks | _ -> None)
        | Some rk ->
            Seq.joinBy (fun (ks,kt) (ks',kt') -> kt = KeyType.Hash && kt' = KeyType.Range && ks = ks') hk.KeySchemata rk.KeySchemata
            |> Seq.tryPick (fun ((a,_),_) -> Some a)

    else None

let ensureNotTautological query =
    match query with
    | False | True -> invalidArg "expr" "supplied query is tautological."
    | _ -> ()

/// Extracts a query expression from a quoted F# predicate
let extractQueryExpr (recordInfo : RecordTableInfo) (expr : Expr) : ConditionalExpression =
    if not expr.IsClosed then invalidArg "expr" "supplied query is not a closed expression."
    let invalidQuery() = invalidArg "expr" <| sprintf "Supplied expression is not a valid conditional."

    let nParams, (|PVar|_|), expr' = extractExprParams recordInfo expr

    match expr' with
    | Lambda(r,body) when r.Type = recordInfo.Type ->

        let getAttrValue (pickler : Pickler) (expr : Expr) =
            expr |> evalRaw |> pickler.PickleCoerced

        let (|AttributeGet|_|) e = 
            match QuotedAttribute.TryExtract (|PVar|_|) r recordInfo e with
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

        let extractNestedField (expr : Expr) =
            let op = extractOperand None expr
            match op with
            | Value av when av.AttributeValue.S <> null -> FField av.AttributeValue.S
            | Value av when av.AttributeValue.N <> null -> FIndex (int av.AttributeValue.N)
            | Param(i, _) -> FParam i
            | _ -> invalidQuery()

        let (|Comparison|_|) (pat : Expr) (expr : Expr) =
            match expr with
            | SpecificCall pat (None, _, args) ->
                let pickler = args |> List.tryPick (|AttributeGet|_|) |> Option.map (fun attr -> attr.Pickler)
                let operands = args |> List.map (extractOperand pickler)
                let pickler = match pickler with Some p -> p | None -> Pickler.resolveUntyped args.[0].Type
                Some(pickler, operands)

            | _ -> None

        let extractComparison (p : Pickler) (cmp : Comparator) (left : Operand) (right : Operand) =
            if cmp.IsComparison && not p.IsComparable then
                sprintf "Representation of type '%O' does not support comparisons." p.Type
                |> invalidArg "expr"

            elif left = right then
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
                QueryExpr.ENot (extractQuery body)

            | AndAlso(left, right) ->
                QueryExpr.EAnd (extractQuery left) (extractQuery right)

            | OrElse(left, right) ->
                QueryExpr.EOr (extractQuery left) (extractQuery right)

            | PipeLeft e
            | PipeRight e -> extractQuery e

            | AttributeGet attr -> Compare(EQ, Attribute attr.Id, Value (wrap (AttributeValue(BOOL = true))))

            | Comparison <@ (=) @> (p, [left; right])  -> extractComparison p EQ left right
            | Comparison <@ (<>) @> (p, [left; right]) -> extractComparison p NE left right
            | Comparison <@ (<) @> (p, [left; right]) -> extractComparison p LT left right
            | Comparison <@ (>) @> (p, [left; right]) -> extractComparison p GT left right
            | Comparison <@ (<=) @> (p, [left; right]) -> extractComparison p LE left right
            | Comparison <@ (>=) @> (p, [left; right]) -> extractComparison p GE left right

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

            | SpecificCall2 <@ Map.containsKey @> (None, _, _, [key; AttributeGet attr]) ->
                let nf = extractNestedField key
                Attribute_Exists (attr.Id.Append nf)

            | SpecificCall2 <@ fun (x : Map<_,_>) y -> x.ContainsKey y @> (Some(AttributeGet attr), _, _, [key]) when key.IsClosed ->
                let nf = extractNestedField key
                Attribute_Exists (attr.Id.Append nf)

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

            | SpecificCall2 <@ Array.isEmpty @> (None, _, _, [AttributeGet attr]) -> 
                Attribute_Not_Exists attr.Id

            | SpecificCall2 <@ List.isEmpty @> (None, _, _, [AttributeGet attr]) -> 
                Attribute_Not_Exists attr.Id

            | SpecificCall2 <@ Option.isSome @> (None, _, _, [AttributeGet attr]) -> 
                Attribute_Exists attr.Id

            | SpecificCall2 <@ Option.isNone @> (None, _, _, [AttributeGet attr]) -> 
                Attribute_Not_Exists attr.Id

            | _ -> invalidQuery()

        let queryExpr = extractQuery body
        do ensureNotTautological queryExpr
        {
            QueryExpr = queryExpr
            NParams = nParams
            KeyCondition = extractKeyCondition queryExpr
        }

    | _ -> invalidQuery()

/// applies a set of input values to a parametric query expression
let applyParams (cond : ConditionalExpression) (inputValues : obj[]) =
    let applyAttr (attr:AttributeId) = attr.Apply inputValues
    let applyOperand (op : Operand) =
        match op with
        | Param (i, pickler) ->
            match pickler.PickleCoerced inputValues.[i] with
            | Some av -> Value (wrap av)
            | None -> Undefined

        | Attribute attr -> Attribute(applyAttr attr)
        | _ -> op

    let rec applyQuery q =
        match q with
        | False | True -> q
        | Attribute_Exists attr -> Attribute_Exists(applyAttr attr)
        | Attribute_Not_Exists attr -> Attribute_Not_Exists(applyAttr attr)
        | Not q -> QueryExpr.ENot (applyQuery q)
        | And(q, q') -> QueryExpr.EAnd (applyQuery q) (applyQuery q')
        | Or(q, q') -> QueryExpr.EOr (applyQuery q) (applyQuery q')
        | In(o, os) -> In(applyOperand o, List.map applyOperand os)
        | Between(x, l, u) -> Between(applyOperand x, applyOperand l, applyOperand u)
        | BeginsWith(attr, o) -> BeginsWith(applyAttr attr, applyOperand o)
        | Contains(attr, o) -> Contains(applyAttr attr, applyOperand o)
        | Compare(cmp, l, r) ->
            let l' = applyOperand l
            let r' = applyOperand r
            let assignUndefined op =
                match op with
                | Attribute attr ->
                    match cmp with
                    | NE -> Attribute_Exists (applyAttr attr)
                    | EQ -> Attribute_Not_Exists (applyAttr attr)
                    | _ -> True
                | _ -> invalidOp "internal error; assigning undefined value to non attribute path."

            if l' = Undefined then assignUndefined r'
            elif r' = Undefined then assignUndefined l'
            else Compare(cmp, l', r')

    let reduced = applyQuery cond.QueryExpr
    do ensureNotTautological reduced
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
        KeyCondition = Some schema
        NParams = 0
    }

/// Generates a conditional that verifies whether a given item does not exist
let mkItemNotExistsCondition (schema : TableKeySchema) =
    {
        QueryExpr = AttributeId.FromKeySchema schema |> Attribute_Not_Exists
        KeyCondition = Some schema
        NParams = 0
    }

/// Generates a hash key equality condition for given key schema
let mkHashKeyEqualityCondition (schema : TableKeySchema) (av : AttributeValue) =
    let hkAttrId = AttributeId.FromKeySchema schema 
    {
        QueryExpr = Compare(EQ, Attribute hkAttrId, Value (wrap av))
        KeyCondition = Some schema
        NParams = 0    
    }

type ConditionalExpression with
    member cond.Apply([<ParamArray>]parameters : obj[]) =
        applyParams cond parameters

    member cond.Write (writer : AttributeWriter) =
        writeConditionExpression writer cond

    member cond.IsKeyConditionCompatible = Option.isSome cond.KeyCondition
    member cond.IndexName =
        match cond.KeyCondition with
        | Some kc -> kc.Type.IndexName
        | None -> None

    member cond.GetDebugData() =
        let aw = new AttributeWriter()
        let expr = writeConditionExpression aw cond
        let names = aw.Names |> Seq.map (fun kv -> kv.Key, kv.Value) |> Seq.toList
        let values = aw.Values |> Seq.map (fun kv -> kv.Key, kv.Value.Print()) |> Seq.toList
        expr, names, values

    static member Extract (recordInfo : RecordTableInfo) (expr : Expr) =
        extractQueryExpr recordInfo expr

    /// Extract a KeyCondition for records that specify a default hashkey
    static member TryExtractHashKeyCondition (recordInfo : RecordTableInfo) =
        match recordInfo.PrimaryKeyStructure with
        | DefaultHashKey(_, value, pickler, _) ->
            let av = pickler.PickleUntyped value |> Option.get
            let cond = mkHashKeyEqualityCondition recordInfo.PrimaryKeySchema av
            Some cond
        | _ -> None