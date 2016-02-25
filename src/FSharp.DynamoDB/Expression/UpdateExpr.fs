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

open FSharp.DynamoDB.ExprCommon

//
//  Converts an F# quotation into an appropriate DynamoDB update expression.
//  This section is responsible for converting two types of expressions:
//
//  1. Record update expressions
//
//  Converts expressions of the form <@ fun r -> { r with A = r.A + 1 ; B = "new value" } @>
//  This is a more 'functional' approach but offers a much more limited degree of control
//  compared to the actual DynamoDB capabilities
//
//  2. Update operation expressions
//
//  Converts expressions of the form <@ fun r -> SET r.A.B[0] 2 &&& REMOVE r.T &&& ADD r.D [1] @>
//  This is a more 'imperative' approach that fully exposes the underlying DynamoDB update API.
//
//  see http://docs.aws.amazon.com/amazondynamodb/latest/developerguide/Expressions.Modifying.html

/// DynamoDB update opration
type UpdateOperation =
    | Set of AttributePath * UpdateValue
    | Remove of AttributePath
    | Add of AttributePath * Operand
    | Delete of AttributePath * Operand
with    
    member uo.Type =
        match uo with
        | Set _ -> 0
        | Remove _ -> 1
        | Add _ -> 2
        | Delete _ -> 3

/// Update value used for SET operations
and UpdateValue =
    | Operand of Operand
    | Op_Addition of Operand * Operand
    | Op_Subtraction of Operand * Operand
    | List_Append of Operand * Operand

/// Update value operand
and Operand =
    | Attribute of AttributePath
    | Value of AttributeValue
    | Param of index:int
    | Undefined

/// Intermediate representation of update expressions
type UpdateExprs = 
    { 
        /// Record variable identifier used by predicate
        RVar : Var
        /// External update params
        Params : Dictionary<Var, int * Pickler>
        /// Record conversion info
        RecordInfo : RecordInfo
        /// Collection of SET assignments that require further conversions
        Assignments : (AttributePath * Expr) [] 
        /// Already extracted update operations
        UpdateOps : UpdateOperation []
    }

/// Extracts update expressions from a quoted record update predicate
let extractRecordExprUpdaters (recordInfo : RecordInfo) (expr : Expr) =
    if not expr.IsClosed then invalidArg "expr" "supplied update expression contains free variables."
    let invalidExpr() = invalidArg "expr" <| sprintf "Supplied expression is not a valid update expression."

    let eparams = new Dictionary<Var, int * Pickler> ()
    let rec extractParams i expr =
        match expr with
        | Lambda(v, body) when v.Type <> recordInfo.Type ->
            let p = Pickler.resolveUntyped v.Type
            eparams.Add(v, (i, p))
            extractParams (i + 1) body
        | _ -> expr

    let expr' = extractParams 0 expr

    match expr' with
    | Lambda(r,body) when r.Type = recordInfo.Type ->
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
            | Var v when bindings.ContainsKey v -> Some(Root rp, bindings.[v])
            | e -> Some (Root rp, e)

        let assignmentExprs = 
            assignments 
            |> Seq.mapi tryExtractValueExpr 
            |> Seq.choose id 
            |> Seq.toArray

        { RVar = r ; Params = eparams ; Assignments = assignmentExprs ; RecordInfo = recordInfo ; UpdateOps = [||] }

    | _ -> invalidExpr()


/// Extracts update expressions from a quoted update operation predicate
let extractOpExprUpdaters (recordInfo : RecordInfo) (expr : Expr) =
    if not expr.IsClosed then invalidArg "expr" "supplied update expression contains free variables."
    let invalidExpr() = invalidArg "expr" <| sprintf "Supplied expression is not a valid update expression."

    let eparams = new Dictionary<Var, int * Pickler> ()
    let rec extractParams i expr =
        match expr with
        | Lambda(v, body) when v.Type <> recordInfo.Type ->
            let p = Pickler.resolveUntyped v.Type
            eparams.Add(v, (i, p))
            extractParams (i + 1) body
        | _ -> expr

    let expr' = extractParams 0 expr

    let (|PVar|_|) (e : Expr) =
        match e with
        | Var v ->
            let ok, found = eparams.TryGetValue v
            if ok then Some(fst found)
            else None
        | _ -> None

    let getOperand (pickler : Pickler) (expr : Expr) =
        match expr with
        | PVar i -> Param i
        | _ ->
            match expr |> evalRaw |> pickler.PickleCoerced with
            | None -> Undefined
            | Some av -> Value av

    match expr' with
    | Lambda(r,body) when r.Type = recordInfo.Type ->
        let (|AttributeGet|_|) (e : Expr) = AttributePath.TryExtract r recordInfo e
        let attrs = new ResizeArray<AttributePath>()
        let assignments = new ResizeArray<AttributePath * Expr> ()
        let updateOps = new ResizeArray<UpdateOperation> ()
        let rec extract e =
            match e with
            | SpecificCall2 <@ (&&&) @> (None, _, _, [l; r]) ->
                extract l ; extract r

            | SpecificCall2 <@ SET @> (None, _, _, [AttributeGet attr; value]) ->
                attrs.Add attr
                assignments.Add(attr, value)

            | SpecificCall2 <@ REMOVE @> (None, _, _, [AttributeGet attr]) ->
                attrs.Add attr
                updateOps.Add (Remove attr)

            | SpecificCall2 <@ ADD @> (None, _, _, [AttributeGet attr; value]) ->
                let op = getOperand attr.Pickler value
                attrs.Add attr
                updateOps.Add (Add (attr, op))

            | SpecificCall2 <@ DELETE @> (None, _, _, [AttributeGet attr; value]) ->
                let op = getOperand attr.Pickler value
                attrs.Add attr
                updateOps.Add (Delete (attr, op))

            | _ -> invalidExpr()

        do extract body

        match tryFindConflictingPaths attrs with
        | Some (p1,p2) -> 
            let msg = sprintf "found conflicting paths '%s' and '%s' being accessed in update expression." p1 p2
            invalidArg "expr" msg

        | None -> ()

        let assignments = assignments.ToArray()
        let updateOps = updateOps.ToArray()

        { RVar = r ; Params = eparams ; RecordInfo = recordInfo ; Assignments = assignments ; UpdateOps = updateOps }

    | _ -> invalidExpr()

/// Completes conversion from intermediate update expression to final update operations
let extractUpdateOps (exprs : UpdateExprs) =
    let invalidExpr() = invalidArg "expr" <| sprintf "Supplied expression is not a valid update expression."
    let (|AttributeGet|_|) (e : Expr) = AttributePath.TryExtract exprs.RVar exprs.RecordInfo e

    let getValue (pickler : Pickler) (expr : Expr) =
        match expr |> evalRaw |> pickler.PickleCoerced with
        | None -> Undefined
        | Some av -> Value av

    let (|PVar|_|) (e : Expr) =
        match e with
        | Var v ->
            let ok, found = exprs.Params.TryGetValue v
            if ok then Some(fst found)
            else None
        | _ -> None

    let rec extractOperand (pickler : Pickler) (expr : Expr) =
        match expr with
        | _ when expr.IsClosed -> getValue pickler expr
        | PipeRight e | PipeLeft e -> extractOperand pickler e
        | AttributeGet attr -> Attribute attr
        | PVar i -> Param i
        | _ -> invalidExpr()

    let rec extractUpdateValue (pickler : Pickler) (expr : Expr) =
        match expr with
        | PipeRight e | PipeLeft e -> extractUpdateValue pickler e
        | SpecificCall2 <@ (+) @> (None, _, _, [left; right]) when pickler.PickleType = PickleType.Number ->
            let l, r = extractOperand pickler left, extractOperand pickler right
            if l = Undefined then Operand r
            elif r = Undefined then Operand l
            else
                Op_Addition(l, r)

        | SpecificCall2 <@ (-) @> (None, _, _, [left; right]) when pickler.PickleType = PickleType.Number ->
            let l, r = extractOperand pickler left, extractOperand pickler right
            if l = Undefined then Operand r
            elif r = Undefined then Operand l
            else
                Op_Subtraction(l, r)

        | SpecificCall2 <@ Array.append @> (None, _, _, [left; right]) ->
            let l, r = extractOperand pickler left, extractOperand pickler right
            if l = Undefined then Operand r
            elif r = Undefined then Operand l
            else
                List_Append(l, r)

        | SpecificCall2 <@ (@) @> (None, _, _, [left; right]) ->
            let l, r = extractOperand pickler left, extractOperand pickler right
            if l = Undefined then Operand r
            elif r = Undefined then Operand l
            else
                List_Append(l, r)

        | SpecificCall2 <@ List.append @> (None, _, _, [left; right]) ->
            let l, r = extractOperand pickler left, extractOperand pickler right
            if l = Undefined then Operand r
            elif r = Undefined then Operand l
            else
                List_Append(l, r)

        | _ -> extractOperand pickler expr |> Operand

    let rec tryExtractUpdateExpr (parent : AttributePath) (expr : Expr) =
        match expr with
        | PipeRight e | PipeLeft e -> tryExtractUpdateExpr parent e
        | SpecificCall2 <@ Set.add @> (None, _, _, [elem; AttributeGet attr]) when parent = attr ->
            let op = extractOperand parent.Pickler elem
            if op = Undefined then None
            else Add(attr, op) |> Some

        | SpecificCall2 <@ fun (s : Set<_>) e -> s.Add e @> (Some (AttributeGet attr), _, _, [elem]) when attr = parent ->
            let op = extractOperand parent.Pickler elem
            if op = Undefined then None
            else
                Add(attr, op) |> Some

        | SpecificCall2 <@ Set.remove @> (None, _, _, [elem ; AttributeGet attr]) when attr = parent ->
            let op = extractOperand parent.Pickler elem
            if op = Undefined then None
            else
                Delete(attr, op) |> Some

        | SpecificCall2 <@ fun (s : Set<_>) e -> s.Remove e @> (Some (AttributeGet attr), _, _, [elem]) when attr = parent ->
            let op = extractOperand parent.Pickler elem
            if op = Undefined then None
            else
                Delete(attr, op) |> Some

        | SpecificCall2 <@ (+) @> (None, _, _, ([AttributeGet attr; other] | [other ; AttributeGet attr])) when attr = parent && not attr.Pickler.IsScalar ->
            let op = extractOperand parent.Pickler other
            if op = Undefined then None
            else
                Add(attr, op) |> Some

        | SpecificCall2 <@ (-) @> (None, _, _, [AttributeGet attr; other]) when attr = parent && not attr.Pickler.IsScalar ->
            let op = extractOperand parent.Pickler other
            if op = Undefined then None
            else
                Delete(attr, op) |> Some

        | SpecificCall2 <@ Map.add @> (None, _, _, [keyE; value; AttributeGet attr]) when attr = parent ->
            let key = evalRaw keyE
            let attr = Suffix(key, parent)
            let ep = getElemPickler parent.Pickler
            match extractUpdateValue ep value with
            | Operand op when op = Undefined -> Some(Remove attr)
            | uv -> Some(Set(attr, uv))

        | SpecificCall2 <@ Map.remove @> (None, _, _, [keyE; AttributeGet attr]) when attr = parent ->
            let key = evalRaw keyE
            let attr = Suffix(key, parent)
            Some(Remove attr)

        | e -> 
            match extractUpdateValue parent.Pickler e with
            | Operand op when op = Undefined -> Some(Remove parent)
            | uv -> Some(Set(parent, uv))

    let updateOps = 
        exprs.Assignments 
        |> Seq.choose (fun (rp,e) -> tryExtractUpdateExpr rp e)
        |> Seq.append exprs.UpdateOps
        |> Seq.sortBy (fun o -> o.Type)
        |> Seq.toArray

    let picklers = 
        exprs.Params 
        |> Seq.map (fun kv -> kv.Value) 
        |> Seq.sortBy fst 
        |> Seq.map snd 
        |> Seq.toArray

    if updateOps.Length = 0 then invalidArg "expr" "No update clauses found in expression"

    picklers, updateOps

/// prints a set of update operations to string recognizable by the DynamoDB APIs
let updateExprsToString (getAttrId : AttributePath -> string) 
                        (getValueId : AttributeValue -> string) (uexprs : UpdateOperation []) =

    let opStr op = 
        match op with 
        | Attribute id -> getAttrId id 
        | Value id -> getValueId id
        | Undefined -> invalidOp "internal error: attempting to reference undefined value in update expression."
        | Param _ -> invalidOp "internal error: attempting to reference parametric value in update expression."

    let valStr value = 
        match value with 
        | Operand op -> opStr op 
        | Op_Addition(l, r) -> sprintf "%s + %s" (opStr l) (opStr r)
        | Op_Subtraction(l, r) -> sprintf "%s - %s" (opStr l) (opStr r)
        | List_Append(l,r) -> sprintf "(list_append(%s, %s))" (opStr l) (opStr r)

    let sb = new System.Text.StringBuilder()
    let append (s:string) = sb.Append s |> ignore
    let isFirst = 
        let tag = ref -1 in
        fun (op:UpdateOperation) -> 
            if !tag = -1 then 
                tag := op.Type
                true
            elif !tag <> op.Type then
                tag := op.Type
                append " "
                true
            else
                false

    for uo in uexprs do
        match uo with
        | Set(attr, op) when isFirst uo -> sprintf "SET %s = %s" (getAttrId attr) (valStr op) |> append
        | Set(attr, op) -> sprintf ", %s = %s" (getAttrId attr) (valStr op) |> append
        | Add(attr, op) when isFirst uo -> sprintf "ADD %s %s" (getAttrId attr) (opStr op) |> append
        | Add(attr, op) -> sprintf ", %s %s" (getAttrId attr) (opStr op) |> append
        | Delete(attr, op) when isFirst uo -> sprintf "DELETE %s %s" (getAttrId attr) (opStr op) |> append
        | Delete(attr, op) -> sprintf ", %s %s" (getAttrId attr) (opStr op) |> append
        | Remove(attr) when isFirst uo -> sprintf "REMOVE %s" (getAttrId attr) |> append
        | Remove(attr) -> sprintf ", %s" (getAttrId attr) |> append

    sb.ToString()

/// applies a set of input values to parametric update operations
let applyParametersToUpdateOps (inputValues : obj[]) (picklers : Pickler[]) (uops : UpdateOperation[]) =
    let paramValues = new Dictionary<int, Operand>()
    let applyOperand (op : Operand) =
        match op with
        | Param i ->
            let ok, found = paramValues.TryGetValue i
            if ok then found
            else
                let v = inputValues.[i]
                let pickler = picklers.[i]
                let op =
                    match pickler.PickleUntyped v with
                    | Some av -> Value av
                    | None -> Undefined

                paramValues.Add(i, op)
                op

        | _ -> op

    let applyUpdateValue (uv : UpdateValue) =
        match uv with
        | Operand op -> Operand(applyOperand op)
        | Op_Addition(l,r) -> 
            let l' = applyOperand l
            let r' = applyOperand r
            if l' = Undefined then Operand r'
            elif r' = Undefined then Operand l'
            else Op_Addition(l', r')

        | Op_Subtraction(l,r) -> 
            let l' = applyOperand l
            let r' = applyOperand r
            if l' = Undefined then Operand r'
            elif r' = Undefined then Operand l'
            else Op_Addition(l', r')

        | List_Append(l,r) ->
            let l' = applyOperand l
            let r' = applyOperand r
            if l' = Undefined then Operand r'
            elif r' = Undefined then Operand l'
            else List_Append(l', r')

    let applyUpdateOp uop =
        match uop with
        | Remove _ -> uop
        | Set(attr,uv) -> Set(attr, applyUpdateValue uv)
        | Add(attr, op) -> Add(attr, applyOperand op)
        | Delete(attr, op) -> Delete(attr, applyOperand op)

    uops |> Array.map applyUpdateOp
            
        

let private attrValueCmp = new AttributeValueComparer() :> IEqualityComparer<_>

/// Update expression parsed form holder
[<CustomEquality; NoComparison>]
type UpdateExpression =
    {
        Expression : string
        Attributes : (string * string) []
        Values     : (string * AttributeValue) []
    }
with
    member __.WriteAttributesTo(target : Dictionary<string,string>) =
        for k,v in __.Attributes do target.[k] <- v

    member __.WriteValuesTo(target : Dictionary<string, AttributeValue>) =
        for k,v in __.Values do target.[k] <- v

    /// Creates update expression from given update operations
    static member Create (updateOps : UpdateOperation[]) =
        let attrs = new Dictionary<string, string> ()
        let getAttrId (attr : AttributePath) =
            let rp = attr.RootProperty
            if rp.IsHashKey then invalidArg "expr" "update expression cannot update hash key."
            if rp.IsRangeKey then invalidArg "expr" "update expression cannot update range key."
            let ok,found = attrs.TryGetValue rp.AttrId
            if ok then attr.Id
            else
                attrs.Add(rp.AttrId, rp.Name)
                attr.Id

        let values = new Dictionary<AttributeValue, string>(new AttributeValueComparer())
        let getValueId (av : AttributeValue) =
            let ok,found = values.TryGetValue av
            if ok then found
            else
                let id = sprintf ":uval%d" values.Count
                values.Add(av, id)
                id

        let exprString = updateExprsToString getAttrId getValueId updateOps

        {
            Expression = exprString
            Attributes = attrs |> Seq.map (fun kv -> kv.Key, kv.Value) |> Seq.sortBy fst |> Seq.toArray
            Values = values |> Seq.map (fun kv -> kv.Value, kv.Key) |> Seq.sortBy fst |> Seq.toArray
        }

    static member ExtractUpdateExpr0 recordInfo (expr : Expr<'TRecord -> 'TRecord>) =
        let exprs = extractRecordExprUpdaters recordInfo expr
        let _,updateOps = extractUpdateOps exprs
        UpdateExpression.Create updateOps

    static member ExtractUpdateExpr1 recordInfo (expr : Expr<'T1 -> 'TRecord -> 'TRecord>) =
        let exprs = extractRecordExprUpdaters recordInfo expr
        let picklers,updateOps = extractUpdateOps exprs
        fun (t1:'T1) ->
            let updateOps' = applyParametersToUpdateOps [|t1|] picklers updateOps
            UpdateExpression.Create updateOps'

    static member ExtractUpdateExpr2 recordInfo (expr : Expr<'T1 -> 'T2 -> 'TRecord -> 'TRecord>) =
        let exprs = extractRecordExprUpdaters recordInfo expr
        let picklers,updateOps = extractUpdateOps exprs
        fun (t1:'T1) (t2:'T2) ->
            let updateOps' = applyParametersToUpdateOps [|t1;t2|] picklers updateOps
            UpdateExpression.Create updateOps'

    static member ExtractUpdateExpr3 recordInfo (expr : Expr<'T1 -> 'T2 -> 'T3 -> 'TRecord -> 'TRecord>) =
        let exprs = extractRecordExprUpdaters recordInfo expr
        let picklers,updateOps = extractUpdateOps exprs
        fun (t1:'T1) (t2:'T2) (t3:'T3) ->
            let updateOps' = applyParametersToUpdateOps [|t1;t2;t3|] picklers updateOps
            UpdateExpression.Create updateOps'

    static member ExtractOpExpr0 recordInfo (expr : Expr<'TRecord -> UpdateOp>) =
        let exprs = extractOpExprUpdaters recordInfo expr
        let _,updateOps = extractUpdateOps exprs
        UpdateExpression.Create updateOps

    static member ExtractOpExpr1 recordInfo (expr : Expr<'T1 -> 'TRecord -> UpdateOp>) =
        let exprs = extractOpExprUpdaters recordInfo expr
        let picklers,updateOps = extractUpdateOps exprs
        fun (t1:'T1) ->
            let updateOps' = applyParametersToUpdateOps [|t1|] picklers updateOps
            UpdateExpression.Create updateOps'

    static member ExtractOpExpr2 recordInfo (expr : Expr<'T1 -> 'T2 -> 'TRecord -> UpdateOp>) =
        let exprs = extractOpExprUpdaters recordInfo expr
        let picklers,updateOps = extractUpdateOps exprs
        fun (t1:'T1) (t2:'T2) ->
            let updateOps' = applyParametersToUpdateOps [|t1;t2|] picklers updateOps
            UpdateExpression.Create updateOps'

    static member ExtractOpExpr3 recordInfo (expr : Expr<'T1 -> 'T2 -> 'T3 -> 'TRecord -> UpdateOp>) =
        let exprs = extractOpExprUpdaters recordInfo expr
        let picklers,updateOps = extractUpdateOps exprs
        fun (t1:'T1) (t2:'T2) (t3:'T3) ->
            let updateOps' = applyParametersToUpdateOps [|t1;t2;t3|] picklers updateOps
            UpdateExpression.Create updateOps'

    override uexpr.Equals obj =
        match obj with
        | :? UpdateExpression as uexpr' ->
            uexpr.Expression = uexpr'.Expression &&
            uexpr.Values.Length = uexpr'.Values.Length &&
            let eq (k,v) (k',v') = k = k' && attrValueCmp.Equals(v,v') in
            Array.forall2 eq uexpr.Values uexpr'.Values
        | _ -> false

    override uexpr.GetHashCode() =
        let mutable vhash = 0
        for k,v in uexpr.Values do
            let th = combineHash (hash k) (attrValueCmp.GetHashCode v)
            vhash <- combineHash vhash th

        hash2 uexpr.Expression vhash