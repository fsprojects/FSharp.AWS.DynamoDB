namespace FSharp.AWS.DynamoDB

open Amazon.DynamoDBv2.Model
//open FSharp.AWS.DynamoDB
open FSharp.AWS.DynamoDB.ExprCommon
open FSharp.Control
open Microsoft.FSharp.Quotations
open System.Net

module AsyncSeq =
    let private scanAsyncSeq (tableContext : TableContext<'a>)
        (filterCondition : ConditionalExpr.ConditionalExpression option)
        (projectionExpr : ProjectionExpr.ProjectionExpr option) (consistentRead : bool option) =
        asyncSeq {
            let rec aux last =
                asyncSeq {
                    let request = new ScanRequest(tableContext.TableName)
                    let writer =
                        new AttributeWriter(request.ExpressionAttributeNames, request.ExpressionAttributeValues)
                    match filterCondition with
                    | None -> ()
                    | Some fc -> request.FilterExpression <- fc.Write writer
                    match projectionExpr with
                    | None -> ()
                    | Some pe -> request.ProjectionExpression <- pe.Write writer
                    consistentRead |> Option.iter (fun cr -> request.ConsistentRead <- cr)
                    last |> Option.iter (fun l -> request.ExclusiveStartKey <- l)
                    let! ct = Async.CancellationToken
                    let! response = tableContext.Client.ScanAsync(request, ct) |> Async.AwaitTaskCorrect
                    if response.HttpStatusCode <> HttpStatusCode.OK then
                        failwithf "Query request returned error %O" response.HttpStatusCode
                    for item in response.Items do
                        yield item
                    if response.LastEvaluatedKey.Count > 0 then yield! aux (Some response.LastEvaluatedKey)
                }
            yield! aux None
        }

    type TableContext<'a> with

        /// <summary>
        ///     Asynchronously scans table with given condition expressions.
        /// </summary>
        /// <param name="filterCondition">Filter condition expression.</param>
        /// <param name="limit">Maximum number of items to evaluate.</param>
        /// <param name="consistentRead">Specify whether to perform consistent read operation.</param>
        member __.ScanAsyncSeq(?filterCondition : ConditionExpression<'TRecord>, ?consistentRead : bool) : AsyncSeq<'a> =
            let filterCondition = filterCondition |> Option.map (fun fc -> fc.Conditional)
            scanAsyncSeq __ filterCondition None consistentRead
            |> AsyncSeq.map __.Template.OfAttributeValues

        /// <summary>
        ///     Asynchronously scans table with given condition expressions.
        /// </summary>
        /// <param name="filterCondition">Filter condition expression.</param>
        /// <param name="limit">Maximum number of items to evaluate.</param>
        /// <param name="consistentRead">Specify whether to perform consistent read operation.</param>
        member __.ScanAsyncSeq(filterExpr : Expr<'a -> bool>, ?consistentRead : bool) : AsyncSeq<'a> =
            let cond = __.Template.PrecomputeConditionalExpr filterExpr
            __.ScanAsyncSeq(cond, ?consistentRead = consistentRead)

        /// <summary>
        ///     Asynchronously scans table with given condition expressions.
        ///     Uses supplied projection expression to narrow downloaded attributes.
        ///     Projection type must be a tuple of zero or more non-conflicting properties.
        /// </summary>
        /// <param name="projection">Projection expression.</param>
        /// <param name="filterCondition">Filter condition expression.</param>
        /// <param name="limit">Maximum number of items to evaluate.</param>
        /// <param name="consistentRead">Specify whether to perform consistent read operation.</param>
        member __.ScanProjectedAsyncSeq(projection : ProjectionExpression<'TRecord, 'TProjection>,
                                        ?filterCondition : ConditionExpression<'a>, ?consistentRead : bool) : AsyncSeq<'TProjection> =
            let filterCondition = filterCondition |> Option.map (fun fc -> fc.Conditional)
            scanAsyncSeq __ filterCondition (Some projection.ProjectionExpr) consistentRead |> AsyncSeq.map projection.UnPickle

        /// <summary>
        ///     Asynchronously scans table with given condition expressions.
        ///     Uses supplied projection expression to narrow downloaded attributes.
        ///     Projection type must be a tuple of zero or more non-conflicting properties.
        /// </summary>
        /// <param name="projection">Projection expression.</param>
        /// <param name="filterCondition">Filter condition expression.</param>
        /// <param name="limit">Maximum number of items to evaluate.</param>
        /// <param name="consistentRead">Specify whether to perform consistent read operation.</param>
        member __.ScanProjectedAsyncSeq<'TProjection>(projection : Expr<'a -> 'TProjection>,
                                                      ?filterCondition : Expr<'a -> bool>, ?consistentRead : bool) : AsyncSeq<'TProjection> =
            let filterCondition =
                filterCondition |> Option.map (fun fc -> __.Template.PrecomputeConditionalExpr fc)
            __.ScanProjectedAsyncSeq
                (__.Template.PrecomputeProjectionExpr projection, ?filterCondition = filterCondition,
                 ?consistentRead = consistentRead)
