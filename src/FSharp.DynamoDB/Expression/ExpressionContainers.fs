namespace FSharp.DynamoDB

open System
open System.Collections.Generic

open Microsoft.FSharp.Quotations

open FSharp.DynamoDB.ConditionalExpr
open FSharp.DynamoDB.UpdateExpr

[<AutoOpen>]
module private ExprUtils =
    let comparer = new ExprEqualityComparer() :> IEqualityComparer<Expr>

type ConditionalExpression<'Record> internal (cond : ConditionalExpression, expr : Expr) =
    member __.Expr = expr
    member internal __.Conditional = cond

    override __.Equals(other : obj) =
        match other with
        | :? ConditionalExpression<'Record> as other -> comparer.Equals(expr, other.Expr)
        | _ -> false

    override __.GetHashCode() = comparer.GetHashCode expr

type UpdateExpression<'Record> internal (updater : UpdateExpression, expr : Expr) =
    member __.Expr = expr
    member internal __.Updater = updater

    override __.Equals(other : obj) =
        match other with
        | :? UpdateExpression<'Record> as other -> comparer.Equals(expr, other.Expr)
        | _ -> false

    override __.GetHashCode() = comparer.GetHashCode expr