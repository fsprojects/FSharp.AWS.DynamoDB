namespace FSharp.AWS.DynamoDB

open System
open System.Collections.Generic

open Microsoft.FSharp.Quotations

open FSharp.AWS.DynamoDB.ExprCommon
open FSharp.AWS.DynamoDB.ConditionalExpr
open FSharp.AWS.DynamoDB.UpdateExpr
open FSharp.AWS.DynamoDB.ProjectionExpr

//
//  Public converted condition expression wrapper implementations
//

/// Represents a condition expression for a given record type
[<Sealed; AutoSerializable(false)>]
type ConditionExpression<'TRecord> internal (cond : ConditionalExpression) =
    let data = lazy(cond.GetDebugData())
    /// Gets whether given conditional is a valid key condition
    member __.IsKeyConditionCompatible = cond.IsKeyConditionCompatible
    /// Gets the infered local secondary index for the query, if applicable
    member __.KeyCondition = cond.KeyCondition
    /// Secondary index name for condition expression, if applicable
    member __.IndexName = cond.IndexName

    /// Internal condition expression object
    member internal __.Conditional = cond
    /// DynamoDB condition expression string
    member __.Expression = let expr,_,_ = data.Value in expr
    /// DynamoDB attribute names
    member __.Names = let _,names,_ = data.Value in names
    /// DynamoDB attribute values
    member __.Values = let _,_,values = data.Value in values

    override __.Equals(other : obj) =
        match other with
        | :? ConditionExpression<'TRecord> as other -> cond.QueryExpr = other.Conditional.QueryExpr
        | _ -> false

    override __.GetHashCode() = hash cond.QueryExpr

type ConditionExpression =
    
    /// <summary>
    ///     Applies the AND operation on two conditionals
    /// </summary>
    static member And(left : ConditionExpression<'TRecord>, right : ConditionExpression<'TRecord>) =
        let qExpr = QueryExpr.EAnd left.Conditional.QueryExpr right.Conditional.QueryExpr
        ensureNotTautological qExpr
        new ConditionExpression<'TRecord>(
            { 
                QueryExpr = qExpr
                KeyCondition = extractKeyCondition qExpr
                NParams = 0   
            })

    /// <summary>
    ///     Applies the OR operation on two conditionals
    /// </summary>
    static member Or(left : ConditionExpression<'TRecord>, right : ConditionExpression<'TRecord>) =
        let qExpr = QueryExpr.EOr left.Conditional.QueryExpr right.Conditional.QueryExpr
        ensureNotTautological qExpr
        new ConditionExpression<'TRecord>(
            { 
                QueryExpr = qExpr
                KeyCondition = extractKeyCondition qExpr
                NParams = 0   
            })

    /// <summary>
    ///     Applies the NOT operation on a conditional
    /// </summary>
    static member Not(conditional : ConditionExpression<'TRecord>) =
        let qExpr = QueryExpr.ENot conditional.Conditional.QueryExpr
        ensureNotTautological qExpr
        new ConditionExpression<'TRecord>(
            { 
                QueryExpr = qExpr
                KeyCondition = extractKeyCondition qExpr
                NParams = 0   
            })

/// Represents an update expression for a given record type
[<Sealed; AutoSerializable(false)>]
type UpdateExpression<'TRecord> internal (updateOps : UpdateOperations) =
    let data = lazy(updateOps.GetDebugData())
    /// Internal update expression object
    member internal __.UpdateOps = updateOps
    /// DynamoDB update expression string
    member __.Expression = let expr,_,_ = data.Value in expr
    /// DynamoDB attribute names
    member __.Names = let _,names,_ = data.Value in names
    /// DynamoDB attribute values
    member __.Values = let _,_,values = data.Value in values

    override __.Equals(other : obj) =
        match other with
        | :? UpdateExpression<'TRecord> as other -> updateOps.UpdateOps = other.UpdateOps.UpdateOps
        | _ -> false

    override __.GetHashCode() = hash updateOps.UpdateOps

    static member (&&&) (this : UpdateExpression<'TRecord>, that : UpdateExpression<'TRecord>) =
        UpdateExpression.Combine(this, that)

and UpdateExpression =
    /// Combines a collection of compatible update expressions into a single expression.
    static member Combine([<ParamArray>]exprs : UpdateExpression<'TRecord> []) =
        match exprs with
        | [||] -> invalidArg "exprs" "must specify at least one update expression."
        | [|expr|] -> expr
        | _ ->

        let uops = exprs |> Array.collect (fun e -> e.UpdateOps.UpdateOps)
        match uops |> Seq.map (fun o -> o.Attribute) |> tryFindConflictingPaths with
        | None -> ()
        | Some(p1,p2) ->
            let msg = sprintf "found conflicting paths '%s' and '%s' being accessed in update expression." p1 p2
            invalidArg "expr" msg

        new UpdateExpression<'TRecord>({ UpdateOps = uops ; NParams = 0 })

/// Represents a projection expression for a given record type
[<Sealed; AutoSerializable(false)>]
type ProjectionExpression<'TRecord, 'TProjection> internal (expr : ProjectionExpr) =
    let data = lazy(expr.GetDebugData())
    /// Internal projection expression object
    member internal __.ProjectionExpr = expr
    /// DynamoDB projection expression string
    member __.Expression = let expr,_ = data.Value in expr
    /// DynamoDB attribute names
    member __.Names = let _,names = data.Value in names

    member internal __.UnPickle(ro : RestObject) = expr.Ctor ro :?> 'TProjection

    override __.Equals(other : obj) =
        match other with
        | :? ProjectionExpression<'TRecord, 'TProjection> as other -> expr.Attributes = other.ProjectionExpr.Attributes
        | _ -> false

    override __.GetHashCode() = hash expr.Attributes