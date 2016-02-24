namespace FSharp.DynamoDB

open System
open System.Collections.Generic

open Microsoft.FSharp.Quotations

open FSharp.DynamoDB.ConditionalExpr
open FSharp.DynamoDB.UpdateExpr

//
//  Public converted condition expression wrapper implementations
//

/// Represents a condition expression for a given record type
type ConditionExpression<'Record> internal (cond : ConditionalExpression) =
    /// DynamoDB condition expression string
    member __.Expression = cond.Expression
    /// DynamoDB attribute names
    member __.Attributes = cond.Attributes
    /// DynamoDB attribute values
    member __.Values = cond.Values |> Array.map (fun (k,v) -> k, v.Print())
    /// Gets whether given conditional is a valid key condition
    member __.IsQueryCompatible = cond.IsQueryCompatible
    /// Internal condition expression object
    member internal __.Conditional = cond

    override __.Equals(other : obj) =
        match other with
        | :? ConditionExpression<'Record> as other -> cond = other.Conditional
        | _ -> false

    override __.GetHashCode() = hash cond

/// Represents an update expression for a given record type
type UpdateExpression<'Record> internal (updater : UpdateExpression) =
    /// DynamoDB update expression string
    member __.Expression = updater.Expression
    /// DynamoDB attribute names
    member __.Attributes = updater.Attributes
    /// DynamoDB attribute values
    member __.Values = updater.Values |> Array.map (fun (k,v) -> k, v.Print())
    /// Internal update expression object
    member internal __.Updater = updater

    override __.Equals(other : obj) =
        match other with
        | :? UpdateExpression<'Record> as other -> updater = other.Updater
        | _ -> false

    override __.GetHashCode() = hash updater