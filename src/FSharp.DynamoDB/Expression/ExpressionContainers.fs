namespace FSharp.DynamoDB

open System
open System.Collections.Generic

open Microsoft.FSharp.Quotations

open FSharp.DynamoDB.ConditionalExpr
open FSharp.DynamoDB.UpdateExpr

[<AutoOpen>]
module private ExprUtils =
    let comparer = new ExprEqualityComparer() :> IEqualityComparer<Expr>

type ConditionExpression<'Record> internal (cond : ConditionalExpression, expr : Expr) =
    member __.Source = expr
    member __.Expression = cond.Expression
    member __.Attributes = cond.Attributes
    member __.Values = cond.Values |> Array.map (fun (k,v) -> k, v.Print())
    member __.IsQueryCompatible = cond.IsQueryCompatible
    member internal __.Conditional = cond

    override __.Equals(other : obj) =
        match other with
        | :? ConditionExpression<'Record> as other -> comparer.Equals(expr, other.Source)
        | _ -> false

    override __.GetHashCode() = comparer.GetHashCode expr

type UpdateExpression<'Record> internal (updater : UpdateExpression, expr : Expr) =
    member __.Source = expr
    member __.Expression = updater.Expression
    member __.Attributes = updater.Attributes
    member __.Values = updater.Values |> Array.map (fun (k,v) -> k, v.Print())
    member internal __.Updater = updater

    override __.Equals(other : obj) =
        match other with
        | :? UpdateExpression<'Record> as other -> comparer.Equals(expr, other.Source)
        | _ -> false

    override __.GetHashCode() = comparer.GetHashCode expr