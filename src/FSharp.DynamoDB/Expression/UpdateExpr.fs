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
    | Skip
    | Set of AttributeId * UpdateValue
    | Remove of AttributeId
    | Add of AttributeId * Operand
    | Delete of AttributeId * Operand
with
    member uo.Attribute =
        match uo with
        | Skip -> invalidOp "no attribute contained in skip op"
        | Set(attr,_)  -> attr
        | Remove attr -> attr
        | Add(attr,_) -> attr
        | Delete(attr,_) -> attr

    member uo.Id =
        match uo with
        | Skip -> 0
        | Set _ -> 1
        | Remove _ -> 2
        | Add _ -> 3
        | Delete _ -> 4


/// Update value used for SET operations
and UpdateValue =
    | Operand of Operand
    | Op_Addition of Operand * Operand
    | Op_Subtraction of Operand * Operand
    | List_Append of Operand * Operand
    | If_Not_Exists of AttributeId * Operand

/// Update value operand
and Operand =
    | Attribute of AttributeId
    | Value of AttributeValueEqWrapper
    | Param of index:int * Pickler
    | Undefined

/// Intermediate representation of update expressions
type IntermediateUpdateExprs = 
    { 
        /// Record variable identifier used by predicate
        RVar : Var
        /// Number of parameters in update expression
        NParams : int
        /// Parameter variable recognizer
        ParamRecognizer : Expr -> int option
        /// Record conversion info
        RecordInfo : RecordInfo
        /// Collection of SET assignments that require further conversions
        Assignments : (QuotedAttribute * Expr) [] 
        /// Already extracted update operations
        UpdateOps : UpdateOperation []
    }

/// Update operations pars
type UpdateOperations =
    {
        /// Update operations, sorted by type
        UpdateOps : UpdateOperation []
        /// Number of external parameters in update operation
        NParams : int
    }
with
    member __.IsParametericExpr = __.NParams > 0

type UpdateValue with
    static member EOp_Addition (left : Operand) (right : Operand) =
        match left with
        | Undefined -> Operand right
        | _ ->
            match right with
            | Undefined -> Operand left
            | _ -> Op_Addition(left, right)

    static member EOp_Subtraction (left : Operand) (right : Operand) =
        match left with
        | Undefined -> Operand right
        | _ ->
            match right with
            | Undefined -> Operand left
            | _ -> Op_Subtraction(left, right)

    static member EList_Append (left : Operand) (right : Operand) =
        match left with
        | Undefined -> Operand right
        | _ ->
            match right with
            | Undefined -> Operand left
            | _ -> List_Append(left, right)

type UpdateOperation with
    static member EAdd attrId op =
        match op with
        | Undefined -> Skip
        | _ -> Add(attrId, op)

    static member EDelete attrId op =
        match op with
        | Undefined -> Skip
        | _ -> Delete(attrId, op)

    static member ESet attrId uv =
        match uv with
        | Operand Undefined -> Remove attrId
        | _ -> Set(attrId, uv)

/// Extracts update expressions from a quoted record update predicate
let extractRecordExprUpdaters (recordInfo : RecordInfo) (expr : Expr) : IntermediateUpdateExprs =
    if not expr.IsClosed then invalidArg "expr" "supplied update expression contains free variables."
    let invalidExpr() = invalidArg "expr" <| sprintf "Supplied expression is not a valid update expression."

    let nParams, pRecognizer, expr' = extractExprParams recordInfo expr

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

        { RVar = r ; NParams = nParams ; ParamRecognizer = pRecognizer ; 
          Assignments = assignmentExprs ; RecordInfo = recordInfo ; UpdateOps = [||] }

    | _ -> invalidExpr()


/// Extracts update expressions from a quoted update operation predicate
let extractOpExprUpdaters (recordInfo : RecordInfo) (expr : Expr) : IntermediateUpdateExprs =
    if not expr.IsClosed then invalidArg "expr" "supplied update expression contains free variables."
    let invalidExpr() = invalidArg "expr" <| sprintf "Supplied expression is not a valid update expression."

    let nParams, (|PVar|_|), expr' = extractExprParams recordInfo expr

    let rec getOperand (pickler : Pickler) (expr : Expr) =
        match expr with
        | Coerce(e, _) -> getOperand pickler e
        | PVar i -> Param (i, pickler)
        | _ when expr.IsClosed ->
            match expr |> evalRaw |> pickler.PickleCoerced with
            | None -> Undefined
            | Some av -> Value (wrap av)

        | _ -> invalidExpr()

    match expr' with
    | Lambda(r,body) when r.Type = recordInfo.Type ->
        let (|AttributeGet|_|) (e : Expr) = QuotedAttribute.TryExtract r recordInfo e
        let attrs = new ResizeArray<QuotedAttribute>()
        let assignments = new ResizeArray<QuotedAttribute * Expr> ()
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
                updateOps.Add (Remove attr.Id)

            | SpecificCall2 <@ ADD @> (None, _, _, [AttributeGet attr; value]) ->
                let op = getOperand attr.Pickler value
                attrs.Add attr
                updateOps.Add (Add (attr.Id, op))

            | SpecificCall2 <@ DELETE @> (None, _, _, [AttributeGet attr; value]) ->
                let op = getOperand attr.Pickler value
                attrs.Add attr
                updateOps.Add (Delete (attr.Id, op))

            | _ -> invalidExpr()

        do extract body

        match attrs |> Seq.map (fun attr -> attr.Id) |> tryFindConflictingPaths with
        | Some (p1,p2) -> 
            let msg = sprintf "found conflicting paths '%s' and '%s' being accessed in update expression." p1 p2
            invalidArg "expr" msg

        | None -> ()

        let assignments = assignments.ToArray()
        let updateOps = updateOps.ToArray()

        { RVar = r ; NParams = nParams ; ParamRecognizer = (|PVar|_|) ; RecordInfo = recordInfo ; Assignments = assignments ; UpdateOps = updateOps }

    | _ -> invalidExpr()

/// Completes conversion from intermediate update expression to final update operations
let extractUpdateOps (exprs : IntermediateUpdateExprs) =
    let invalidExpr() = invalidArg "expr" <| sprintf "Supplied expression is not a valid update expression."
    let (|AttributeGet|_|) (e : Expr) = QuotedAttribute.TryExtract exprs.RVar exprs.RecordInfo e

    let getValue (pickler : Pickler) (expr : Expr) =
        match expr |> evalRaw |> pickler.PickleCoerced with
        | None -> Undefined
        | Some av -> Value (wrap av)

    let (|PVar|_|) = exprs.ParamRecognizer

    let rec extractOperand (pickler : Pickler) (expr : Expr) =
        match expr with
        | _ when expr.IsClosed -> getValue pickler expr
        | PipeRight e | PipeLeft e -> extractOperand pickler e
        | AttributeGet attr -> Attribute attr.Id
        | PVar i -> Param (i, pickler)
        | _ -> invalidExpr()

    let rec extractUpdateValue (pickler : Pickler) (expr : Expr) =
        match expr with
        | PipeRight e | PipeLeft e -> extractUpdateValue pickler e
        | SpecificCall2 <@ (+) @> (None, _, _, [left; right]) when pickler.PickleType = PickleType.Number ->
            UpdateValue.EOp_Addition (extractOperand pickler left) (extractOperand pickler right)

        | SpecificCall2 <@ (-) @> (None, _, _, [left; right]) when pickler.PickleType = PickleType.Number ->
            UpdateValue.EOp_Subtraction (extractOperand pickler left) (extractOperand pickler right)

        | SpecificCall2 <@ Array.append @> (None, _, _, [left; right]) ->
            UpdateValue.EList_Append (extractOperand pickler left) (extractOperand pickler right)

        | SpecificCall2 <@ (@) @> (None, _, _, [left; right]) ->
            UpdateValue.EList_Append (extractOperand pickler left) (extractOperand pickler right)

        | ConsList(head, tail) ->
            UpdateValue.EList_Append (extractOperand pickler head) (extractOperand pickler tail)

        | SpecificCall2 <@ List.append @> (None, _, _, [left; right]) ->
            UpdateValue.EList_Append (extractOperand pickler left) (extractOperand pickler right)

        | SpecificCall2 <@ defaultArg @> (None, _, _, [AttributeGet attr; operand]) ->
            match extractOperand attr.Pickler operand with
            | Undefined -> invalidExpr()
            | op -> If_Not_Exists(attr.Id, op)

        | _ -> extractOperand pickler expr |> Operand

    let rec extractUpdateOp (parent : QuotedAttribute) (expr : Expr) : UpdateOperation =
        match expr with
        | PipeRight e | PipeLeft e -> extractUpdateOp parent e
        | SpecificCall2 <@ Set.add @> (None, _, _, [elem; AttributeGet attr]) when parent = attr ->
            UpdateOperation.EAdd attr.Id (extractOperand parent.Pickler elem)

        | SpecificCall2 <@ fun (s : Set<_>) e -> s.Add e @> (Some (AttributeGet attr), _, _, [elem]) when attr = parent ->
            UpdateOperation.EAdd attr.Id (extractOperand parent.Pickler elem)

        | SpecificCall2 <@ Set.remove @> (None, _, _, [elem ; AttributeGet attr]) when attr = parent ->
            UpdateOperation.EDelete attr.Id (extractOperand parent.Pickler elem)

        | SpecificCall2 <@ fun (s : Set<_>) e -> s.Remove e @> (Some (AttributeGet attr), _, _, [elem]) when attr = parent ->
            UpdateOperation.EDelete attr.Id (extractOperand parent.Pickler elem)

        | SpecificCall2 <@ (+) @> (None, _, _, ([AttributeGet attr; other] | [other ; AttributeGet attr])) when attr = parent && not attr.Pickler.IsScalar ->
            UpdateOperation.EAdd attr.Id (extractOperand parent.Pickler other)

        | SpecificCall2 <@ (-) @> (None, _, _, [AttributeGet attr; other]) when attr = parent && not attr.Pickler.IsScalar ->
            UpdateOperation.EDelete attr.Id (extractOperand parent.Pickler other)

        | SpecificCall2 <@ Map.add @> (None, _, _, [keyE; value; AttributeGet attr]) when attr = parent ->
            let key = evalRaw keyE
            let attr = parent.Id.AppendField key
            let ep = getElemPickler parent.Pickler
            UpdateOperation.ESet attr (extractUpdateValue ep value)

        | SpecificCall2 <@ Map.remove @> (None, _, _, [keyE; AttributeGet attr]) when attr = parent ->
            let key = evalRaw keyE
            let attr = parent.Id.AppendField key
            Remove attr

        | e -> 
            UpdateOperation.ESet parent.Id (extractUpdateValue parent.Pickler e)

    let updateOps = 
        exprs.Assignments 
        |> Seq.map (fun (rp,e) -> extractUpdateOp rp e)
        |> Seq.append exprs.UpdateOps
        |> Seq.filter (function Skip _ -> false | _ -> true)
        |> Seq.map (fun uop -> 
            if uop.Attribute.IsHashKey then invalidArg "expr" "update expression cannot update hash key."
            if uop.Attribute.IsRangeKey then invalidArg "expr" "update expression cannot update range key."
            uop)
        |> Seq.sortBy (fun uop -> uop.Id, uop.Attribute.Id)
        |> Seq.toArray

    if updateOps.Length = 0 then invalidArg "expr" "No update clauses found in expression"

    { UpdateOps = updateOps ; NParams = exprs.NParams }

/// applies a set of input values to parametric update operations
let applyParams (uops : UpdateOperations) (inputValues : obj[]) =
    let applyOperand (op : Operand) =
        match op with
        | Param (i, pickler) ->
            match pickler.PickleCoerced inputValues.[i] with
            | Some av -> Value (wrap av)
            | None -> Undefined

        | _ -> op

    let applyUpdateValue (uv : UpdateValue) =
        match uv with
        | Operand op -> Operand(applyOperand op)
        | Op_Addition(l,r) -> UpdateValue.EOp_Addition (applyOperand l) (applyOperand r)
        | Op_Subtraction(l,r) -> UpdateValue.EOp_Subtraction (applyOperand l) (applyOperand r)
        | List_Append(l,r) -> UpdateValue.EOp_Subtraction (applyOperand l) (applyOperand r)
        | If_Not_Exists(attr, op) -> If_Not_Exists(attr, applyOperand op)

    let applyUpdateOp uop =
        match uop with
        | Remove _ | Skip _ -> uop
        | Set(attr, uv) -> UpdateOperation.ESet attr (applyUpdateValue uv)
        | Add(attr, op) -> UpdateOperation.EAdd attr (applyOperand op)
        | Delete(attr, op) -> UpdateOperation.EDelete attr (applyOperand op)

    let updateOps' =
        uops.UpdateOps
        |> Seq.map applyUpdateOp
        |> Seq.filter (function Skip -> false | _ -> true)
        |> Seq.sortBy (fun uop -> uop.Id, uop.Attribute.Id)
        |> Seq.toArray

    { UpdateOps = updateOps' ; NParams = 0 }

/// prints a set of update operations to string recognizable by the DynamoDB APIs
let writeUpdateExpression (writer : AttributeWriter) (uops : UpdateOperations) =

    let sb = new System.Text.StringBuilder()
    let inline (!) (s:string) = sb.Append s |> ignore

    let inline writeAttr attr = !(writer.WriteAttibute attr)
    let inline writeVal v = !(writer.WriteValue (unwrap v))

    let writeOp op = 
        match op with 
        | Attribute attr -> writeAttr attr
        | Value v -> writeVal v
        | Undefined -> invalidOp "internal error: attempting to reference undefined value in update expression."
        | Param _ -> invalidOp "internal error: attempting to reference parametric value in update expression."

    let writeUV value = 
        match value with 
        | Operand op -> writeOp op 
        | Op_Addition(l, r) -> writeOp l; ! " + " ; writeOp r
        | Op_Subtraction(l, r) -> writeOp l ; ! " - " ; writeOp r
        | List_Append(l,r) -> ! "(list_append(" ; writeOp l ; ! ", " ; writeOp r ; ! "))"
        | If_Not_Exists(attr, Undefined) -> 
            sprintf "attempting to populate If_Not_Exists(%s) clause with value of undefined representation." attr.Id
            |> invalidOp

        | If_Not_Exists(attr, op) -> ! "(if_not_exists(" ; writeAttr attr ; ! ", " ; writeOp op ; ! "))"

    let isFirstGp =
        let lastId = ref -1
        fun (uo:UpdateOperation) ->
            if lastId.Value = -1 then
                lastId := uo.Id ; true
            elif lastId.Value <> uo.Id then
                lastId := uo.Id ; ! " " ; true
            else
                false

    for uop in uops.UpdateOps do
        match uop with
        | Skip -> ()
        | Set(attr, uv) -> 
            if isFirstGp uop then ! "SET " else ! ", "
            writeAttr attr ; !" = " ; writeUV uv

        | Remove attr ->
            if isFirstGp uop then ! "REMOVE " else ! ", "
            writeAttr attr

        | Add(attr, op) ->
            if isFirstGp uop then ! "ADD " else ! ", "
            writeAttr attr ; ! " " ; writeOp op

        | Delete(attr, op) ->
            if isFirstGp uop then ! "DELETE " else ! ", "
            writeAttr attr ; ! " " ; writeOp op

    sb.ToString()


type UpdateOperations with
    member uops.Apply([<ParamArray>]parameters : obj[]) =
        applyParams uops parameters

    member uops.Write (writer : AttributeWriter) =
        writeUpdateExpression writer uops

    member uops.GetDebugData() =
        let aw = new AttributeWriter()
        let expr = writeUpdateExpression aw uops
        let names = aw.Names |> Seq.map (fun kv -> kv.Key, kv.Value) |> Seq.toList
        let values = aw.Values |> Seq.map (fun kv -> kv.Key, kv.Value.Print()) |> Seq.toList
        expr, names, values

    static member ExtractUpdateExpr (recordInfo : RecordInfo) (expr : Expr) =
        let iexprs = extractRecordExprUpdaters recordInfo expr
        extractUpdateOps iexprs

    static member ExtractOpExpr (recordInfo : RecordInfo) (expr : Expr) =
        let iexprs = extractOpExprUpdaters recordInfo expr
        extractUpdateOps iexprs