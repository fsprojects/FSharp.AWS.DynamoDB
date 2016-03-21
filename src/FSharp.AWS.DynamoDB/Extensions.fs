namespace FSharp.AWS.DynamoDB

open Microsoft.FSharp.Quotations

/// Collection of extensions for the public API
[<AutoOpen>]
module Extensions =

    /// Precomputes a template expression
    let inline template<'TRecord> = RecordTemplate.Define<'TRecord>()

    /// A conditional which verifies that given item exists
    let inline itemExists<'TRecord> = template<'TRecord>.ItemExists
    /// A conditional which verifies that given item does not exist
    let inline itemDoesNotExist<'TRecord> = template<'TRecord>.ItemDoesNotExist
    
    /// Precomputes a conditional expression
    let inline cond (expr : Expr<'TRecord -> bool>) : ConditionExpression<'TRecord> = 
        template<'TRecord>.PrecomputeConditionalExpr expr

    /// Precomputes an update expression
    let inline update (expr : Expr<'TRecord -> 'TRecord>) : UpdateExpression<'TRecord> = 
        template<'TRecord>.PrecomputeUpdateExpr expr

    /// Precomputes an update operation expression
    let inline updateOp (expr : Expr<'TRecord -> UpdateOp>) : UpdateExpression<'TRecord> = 
        template<'TRecord>.PrecomputeUpdateExpr expr

    /// Precomputes a projection expression
    let inline proj (expr : Expr<'TRecord -> 'TProjection>) : ProjectionExpression<'TRecord, 'TProjection> =
        template<'TRecord>.PrecomputeProjectionExpr<'TProjection> expr