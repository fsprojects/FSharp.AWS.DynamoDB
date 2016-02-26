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
type ConditionExpression<'Record> internal (cond : ConditionalExpression) =
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
        | :? ConditionExpression<'Record> as other -> cond.QueryExpr = other.Conditional.QueryExpr
        | _ -> false

    override __.GetHashCode() = hash cond.QueryExpr

/// Represents an update expression for a given record type
[<Sealed; AutoSerializable(false)>]
type UpdateExpression<'Record> internal (updateOps : UpdateOperations) =
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
        | :? UpdateExpression<'Record> as other -> updateOps.UpdateOps = other.UpdateOps.UpdateOps
        | _ -> false

    override __.GetHashCode() = hash updateOps.UpdateOps