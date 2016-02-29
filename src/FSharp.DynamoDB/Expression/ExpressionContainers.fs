namespace FSharp.DynamoDB

open System
open System.Collections.Generic

open Microsoft.FSharp.Quotations

open FSharp.DynamoDB.ExprCommon
open FSharp.DynamoDB.ConditionalExpr
open FSharp.DynamoDB.UpdateExpr

//
//  Public converted condition expression wrapper implementations
//

/// Represents a condition expression for a given record type
[<Sealed; AutoSerializable(false)>]
type ConditionExpression<'TRecord> internal (cond : ConditionalExpression) =
    let data = lazy(cond.GetDebugData())
    /// Gets whether given conditional is a valid key condition
    member __.IsKeyConditionCompatible = cond.IsKeyConditionCompatible
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
        let lc,rc = left.Conditional, right.Conditional
        new ConditionExpression<'TRecord>(
            { 
                QueryExpr = And(lc.QueryExpr, rc.QueryExpr)
                IsKeyConditionCompatible = lc.IsKeyConditionCompatible && rc.IsKeyConditionCompatible 
                NParams = 0   
            })

    /// <summary>
    ///     Applies the OR operation on two conditionals
    /// </summary>
    static member Or(left : ConditionExpression<'TRecord>, right : ConditionExpression<'TRecord>) =
        let lc,rc = left.Conditional, right.Conditional
        new ConditionExpression<'TRecord>(
            { 
                QueryExpr = Or(lc.QueryExpr, rc.QueryExpr)
                IsKeyConditionCompatible = lc.IsKeyConditionCompatible && rc.IsKeyConditionCompatible 
                NParams = 0   
            })

    /// <summary>
    ///     Applies the NOT operation on a conditional
    /// </summary>
    static member Not(conditional : ConditionExpression<'TRecord>) =
        let c = conditional.Conditional
        new ConditionExpression<'TRecord>(
            { 
                QueryExpr = Not c.QueryExpr
                IsKeyConditionCompatible = c.IsKeyConditionCompatible
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