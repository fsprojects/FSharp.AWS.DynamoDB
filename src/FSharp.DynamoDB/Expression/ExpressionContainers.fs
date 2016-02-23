namespace FSharp.DynamoDB

open System
open System.Collections.Generic

open Microsoft.FSharp.Quotations

open FSharp.DynamoDB.ConditionalExpr
open FSharp.DynamoDB.UpdateExpr

type ConditionExpression<'Record> internal (cond : ConditionalExpression) =
    member __.Expression = cond.Expression
    member __.Attributes = cond.Attributes
    member __.Values = cond.Values |> Array.map (fun (k,v) -> k, v.Print())
    member __.IsQueryCompatible = cond.IsQueryCompatible
    member internal __.Conditional = cond

    override __.Equals(other : obj) =
        match other with
        | :? ConditionExpression<'Record> as other -> cond = other.Conditional
        | _ -> false

    override __.GetHashCode() = hash cond

type UpdateExpression<'Record> internal (updater : UpdateExpression) =
    member __.Expression = updater.Expression
    member __.Attributes = updater.Attributes
    member __.Values = updater.Values |> Array.map (fun (k,v) -> k, v.Print())
    member internal __.Updater = updater

    override __.Equals(other : obj) =
        match other with
        | :? UpdateExpression<'Record> as other -> updater = other.Updater
        | _ -> false

    override __.GetHashCode() = hash updater