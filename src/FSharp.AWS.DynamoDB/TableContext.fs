namespace FSharp.AWS.DynamoDB

open System.Collections.Generic
open System.Net

open Microsoft.FSharp.Quotations

open Amazon.DynamoDBv2
open Amazon.DynamoDBv2.Model

open FSharp.AWS.DynamoDB.KeySchema
open FSharp.AWS.DynamoDB.ExprCommon

/// Exception raised by DynamoDB in case where write preconditions are not satisfied
type ConditionalCheckFailedException = Amazon.DynamoDBv2.Model.ConditionalCheckFailedException

/// Exception raised by DynamoDB in case where resources are not found
type ResourceNotFoundException = Amazon.DynamoDBv2.Model.ResourceNotFoundException

/// Represents the provisioned throughput for given table or index
type ProvisionedThroughput = Amazon.DynamoDBv2.Model.ProvisionedThroughput

/// Represents the operation performed on the table, for metrics collection purposes
type Operation = GetItem | PutItem | UpdateItem | DeleteItem | BatchGetItems | BatchWriteItems | Scan | Query

/// Represents metrics returned by the table operation, for plugging in to an observability framework
type RequestMetrics =
    {
        TableName : string
        Operation : Operation
        ConsumedCapacity : ConsumedCapacity list
        ItemCount : int
    }

/// Scan/query limit type (internal only)
type private LimitType = All | Default | Count of int
    with
    member x.GetCount () =
        match x with
        | Count l -> Some l
        | _ -> None
    member x.IsDownloadIncomplete (count : int) =
        match x with
        | Count l -> count < l
        | All -> true
        | Default -> false
    static member AllOrCount (l : int option) = l |> Option.map Count |> Option.defaultValue All
    static member DefaultOrCount (l : int option) = l |> Option.map Count |> Option.defaultValue Default

/// DynamoDB client object for performing table operations in the context of given F# record representations
[<Sealed; AutoSerializable(false)>]
type TableContext<'TRecord> internal
    (   client : IAmazonDynamoDB,
        tableName : string,
        template : RecordTemplate<'TRecord>,
        defaultMetricsCollector : (RequestMetrics -> unit) option) =

    let reportMetrics collector (operation : Operation) (consumedCapacity : ConsumedCapacity list) (itemCount : int) =
        collector { TableName = tableName; Operation = operation; ConsumedCapacity = consumedCapacity; ItemCount = itemCount }
    let metricsOptions collector =
        match collector |> Option.orElse defaultMetricsCollector with
        | Some sink -> ReturnConsumedCapacity.INDEXES, Some (reportMetrics sink)
        | None -> ReturnConsumedCapacity.NONE, None

    let tryGetItemAsync (key : TableKey) (proj : ProjectionExpr.ProjectionExpr option) collector = async {
        let kav = template.ToAttributeValues(key)
        let request = GetItemRequest(tableName, kav)
        match proj with
        | None -> ()
        | Some proj ->
            let aw = AttributeWriter(request.ExpressionAttributeNames, null)
            request.ProjectionExpression <- proj.Write aw

        let returnConsumedCapacity, maybeReport = metricsOptions collector
        request.ReturnConsumedCapacity <- returnConsumedCapacity

        let! ct = Async.CancellationToken
        let! response = client.GetItemAsync(request, ct) |> Async.AwaitTaskCorrect
        match maybeReport with None -> () | Some r -> r GetItem [ response.ConsumedCapacity ] (if response.IsItemSet then 1 else 0)
        if response.HttpStatusCode <> HttpStatusCode.OK then
            failwithf "GetItem request returned error %O" response.HttpStatusCode

        if response.IsItemSet then return Some response.Item
        else return None
    }

    let getItemAsync (key : TableKey) (proj : ProjectionExpr.ProjectionExpr option) collector = async {
        match! tryGetItemAsync key proj collector with
        | Some item -> return item
        | None -> return raise <| ResourceNotFoundException(sprintf "could not find item %O" key)
    }

    let batchGetItemsAsync (keys : seq<TableKey>) (consistentRead : bool option)
                            (projExpr : ProjectionExpr.ProjectionExpr option)
                            collector = async {

        let consistentRead = defaultArg consistentRead false
        let kna = KeysAndAttributes()
        kna.AttributesToGet.AddRange(template.Info.Properties |> Seq.map (fun p -> p.Name))
        kna.Keys.AddRange(keys |> Seq.map template.ToAttributeValues)
        kna.ConsistentRead <- consistentRead
        match projExpr with
        | None -> ()
        | Some projExpr ->
            let aw = AttributeWriter(kna.ExpressionAttributeNames, null)
            kna.ProjectionExpression <- projExpr.Write aw

        let request = BatchGetItemRequest()
        request.RequestItems.[tableName] <- kna
        let returnConsumedCapacity, maybeReport = metricsOptions collector
        request.ReturnConsumedCapacity <- returnConsumedCapacity

        let! ct = Async.CancellationToken
        let! response = client.BatchGetItemAsync(request, ct) |> Async.AwaitTaskCorrect
        match maybeReport with None -> () | Some r -> r BatchGetItems (List.ofSeq response.ConsumedCapacity) response.Responses.[tableName].Count

        if response.HttpStatusCode <> HttpStatusCode.OK then
            failwithf "BatchGetItem request returned error %O" response.HttpStatusCode

        return response.Responses.[tableName]
    }

    let queryPaginatedAsync (keyCondition : ConditionalExpr.ConditionalExpression)
                    (filterCondition : ConditionalExpr.ConditionalExpression option)
                    (projectionExpr : ProjectionExpr.ProjectionExpr option)
                    (limit: LimitType) (exclusiveStartKey : IndexKey option)
                    (consistentRead : bool option) (scanIndexForward : bool option)
                    collector = async {

        if not keyCondition.IsKeyConditionCompatible then
            invalidArg "keyCondition"
                """key conditions must satisfy the following constraints:
* Must only reference HashKey & RangeKey attributes.
* Must reference HashKey attribute exactly once.
* Must reference RangeKey attribute at most once.
* HashKey comparison must be equality comparison only.
* Must not contain OR and NOT clauses.
* Must not contain nested operands.
"""

        let downloaded = new ResizeArray<_>()
        let returnConsumedCapacity, maybeReport = metricsOptions collector
        let consumedCapacity = new ResizeArray<ConsumedCapacity>()
        let emitMetrics () =
            match maybeReport with
            | None -> ()
            | Some r -> r Query (Seq.toList consumedCapacity) downloaded.Count
        let mutable lastEvaluatedKey : Dictionary<string,AttributeValue> option = None

        let rec aux last = async {
            let request = QueryRequest(tableName)
            keyCondition.IndexName |> Option.iter (fun name -> request.IndexName <- name)
            let writer = AttributeWriter(request.ExpressionAttributeNames, request.ExpressionAttributeValues)
            request.KeyConditionExpression <- keyCondition.Write writer

            match filterCondition with
            | None -> ()
            | Some fc -> request.FilterExpression <- fc.Write writer

            match projectionExpr with
            | None -> ()
            | Some pe -> request.ProjectionExpression <- pe.Write writer

            request.ReturnConsumedCapacity <- returnConsumedCapacity

            limit.GetCount() |> Option.iter (fun l -> request.Limit <- l - downloaded.Count)
            consistentRead |> Option.iter (fun cr -> request.ConsistentRead <- cr)
            scanIndexForward |> Option.iter (fun sif -> request.ScanIndexForward <- sif)
            last |> Option.iter (fun l -> request.ExclusiveStartKey <- l)

            let! ct = Async.CancellationToken
            let! response = client.QueryAsync(request, ct) |> Async.AwaitTaskCorrect
            if response.HttpStatusCode <> HttpStatusCode.OK then
                emitMetrics ()
                failwithf "Query request returned error %O" response.HttpStatusCode

            downloaded.AddRange response.Items
            consumedCapacity.Add response.ConsumedCapacity
            if response.LastEvaluatedKey.Count > 0 then
                lastEvaluatedKey <- Some response.LastEvaluatedKey
                if limit.IsDownloadIncomplete downloaded.Count then
                    do! aux lastEvaluatedKey
            else
                lastEvaluatedKey <- None
        }

        do! aux (exclusiveStartKey |> Option.map (fun k -> template.ToAttributeValues(k, keyCondition.KeyCondition.Value)))

        emitMetrics ()

        return (downloaded, lastEvaluatedKey |> Option.map (fun av -> template.ExtractIndexKey(keyCondition.KeyCondition.Value, av)))
    }

    let queryAsync (keyCondition : ConditionalExpr.ConditionalExpression)
                    (filterCondition : ConditionalExpr.ConditionalExpression option)
                    (projectionExpr : ProjectionExpr.ProjectionExpr option)
                    (limit: int option) (consistentRead : bool option) (scanIndexForward : bool option)
                    collector = async {

        let! (downloaded, _) = queryPaginatedAsync keyCondition filterCondition projectionExpr (LimitType.AllOrCount limit) None consistentRead scanIndexForward collector

        return downloaded
    }

    let scanPaginatedAsync (filterCondition : ConditionalExpr.ConditionalExpression option)
                            (projectionExpr : ProjectionExpr.ProjectionExpr option)
                            (limit : LimitType) (exclusiveStartKey : TableKey option) (consistentRead : bool option)
                            collector = async {

        let downloaded = new ResizeArray<_>()
        let returnConsumedCapacity, maybeReport = metricsOptions collector
        let consumedCapacity = new ResizeArray<ConsumedCapacity>()
        let emitMetrics () =
            match maybeReport with
            | None -> ()
            | Some r -> r Scan (Seq.toList consumedCapacity) downloaded.Count
        let mutable lastEvaluatedKey : Dictionary<string,AttributeValue> option = None
        let rec aux last = async {
            let request = ScanRequest(tableName)
            let writer = AttributeWriter(request.ExpressionAttributeNames, request.ExpressionAttributeValues)
            match filterCondition with
            | None -> ()
            | Some fc -> request.FilterExpression <- fc.Write writer

            match projectionExpr with
            | None -> ()
            | Some pe -> request.ProjectionExpression <- pe.Write writer

            request.ReturnConsumedCapacity <- returnConsumedCapacity

            limit.GetCount() |> Option.iter (fun l -> request.Limit <- l - downloaded.Count)
            consistentRead |> Option.iter (fun cr -> request.ConsistentRead <- cr)
            last |> Option.iter (fun l -> request.ExclusiveStartKey <- l)

            let! ct = Async.CancellationToken
            let! response = client.ScanAsync(request, ct) |> Async.AwaitTaskCorrect
            if response.HttpStatusCode <> HttpStatusCode.OK then
                emitMetrics ()
                failwithf "Scan request returned error %O" response.HttpStatusCode

            downloaded.AddRange response.Items
            consumedCapacity.Add response.ConsumedCapacity
            if response.LastEvaluatedKey.Count > 0 then
                lastEvaluatedKey <- Some response.LastEvaluatedKey
                if limit.IsDownloadIncomplete downloaded.Count then
                    do! aux lastEvaluatedKey
            else
                lastEvaluatedKey <- None
        }

        do! aux (exclusiveStartKey |> Option.map template.ToAttributeValues)

        emitMetrics ()

        return (downloaded, lastEvaluatedKey |> Option.map template.ExtractKey)
    }

    let scanAsync (filterCondition : ConditionalExpr.ConditionalExpression option)
                    (projectionExpr : ProjectionExpr.ProjectionExpr option)
                    (limit : int option) (consistentRead : bool option) collector = async {

        let! (downloaded, _) = scanPaginatedAsync filterCondition projectionExpr (LimitType.AllOrCount limit) None consistentRead collector

        return downloaded
    }

    /// DynamoDB client instance used for the table operations
    member __.Client = client
    /// DynamoDB table name targeted by the context
    member __.TableName = tableName
    /// Primary Key schema used by the current record/table
    member __.PrimaryKey = template.PrimaryKey
    /// Global Secondary indices specified by the table
    member __.GlobalSecondaryIndices = template.GlobalSecondaryIndices
    /// Local Secondary indices specified by the table
    member __.LocalSecondaryIndices = template.LocalSecondaryIndices
    /// Record-induced table template
    member __.Template = template

    /// Creates a new table context instance that uses
    /// a new F# record type. The new F# record type
    /// must define a compatible key schema.
    member __.WithRecordType<'TRecord2>() : TableContext<'TRecord2> =
        let rd = RecordTemplate.Define<'TRecord2>()
        if template.PrimaryKey <> rd.PrimaryKey then
            invalidArg (string typeof<'TRecord2>) "incompatible key schema."

        new TableContext<'TRecord2>(client, tableName, rd, defaultMetricsCollector)


    /// <summary>
    ///     Asynchronously puts a record item in the table.
    /// </summary>
    /// <param name="item">Item to be written.</param>
    /// <param name="precondition">Precondition to satisfy in case item already exists.</param>
    /// <param name="collector">Function to receive request metrics.</param>
    member __.PutItemAsync(item : 'TRecord, ?precondition : ConditionExpression<'TRecord>, ?collector) : Async<TableKey> = async {
        let attrValues = template.ToAttributeValues(item)
        let request = PutItemRequest(tableName, attrValues)
        request.ReturnValues <- ReturnValue.NONE
        let returnConsumedCapacity, maybeReport = metricsOptions collector
        request.ReturnConsumedCapacity <- returnConsumedCapacity
        match precondition with
        | Some pc ->
            let writer = AttributeWriter(request.ExpressionAttributeNames, request.ExpressionAttributeValues)
            request.ConditionExpression <- pc.Conditional.Write writer
        | _ -> ()

        let! ct = Async.CancellationToken
        let! response = client.PutItemAsync(request, ct) |> Async.AwaitTaskCorrect
        match maybeReport with None -> () | Some r -> r PutItem [ response.ConsumedCapacity ] 0
        if response.HttpStatusCode <> HttpStatusCode.OK then
            failwithf "PutItem request returned error %O" response.HttpStatusCode

        return template.ExtractKey item
    }

    /// <summary>
    ///     Asynchronously puts a record item in the table.
    /// </summary>
    /// <param name="item">Item to be written.</param>
    /// <param name="precondition">Precondition to satisfy in case item already exists.</param>
    /// <param name="collector">Function to receive request metrics.</param>
    member __.PutItemAsync(item : 'TRecord, precondition : Expr<'TRecord -> bool>, ?collector) = async {
        return! __.PutItemAsync(item, template.PrecomputeConditionalExpr precondition, ?collector = collector)
    }

    /// <summary>
    ///     Asynchronously puts a collection of items to the table as a batch write operation.
    ///     At most 25 items can be written in a single batch write operation.
    /// </summary>
    /// <returns>Any unprocessed items due to throttling.</returns>
    /// <param name="items">Items to be written.</param>
    /// <param name="collector">Function to receive request metrics.</param>
    member __.BatchPutItemsAsync(items : seq<'TRecord>, ?collector) : Async<'TRecord[]> = async {
        let mkWriteRequest (item : 'TRecord) =
            let attrValues = template.ToAttributeValues(item)
            let pr = PutRequest(attrValues)
            WriteRequest(pr)

        let items = Seq.toArray items
        if items.Length > 25 then invalidArg "items" "item length must be less than or equal to 25."
        let writeRequests = items |> Seq.map mkWriteRequest |> rlist
        let pbr = BatchWriteItemRequest()
        pbr.RequestItems.[tableName] <- writeRequests
        let returnConsumedCapacity, maybeReport = metricsOptions collector
        pbr.ReturnConsumedCapacity <- returnConsumedCapacity
        let! ct = Async.CancellationToken
        let! response = client.BatchWriteItemAsync(pbr, ct) |> Async.AwaitTaskCorrect
        match maybeReport with None -> () | Some r -> r BatchWriteItems (Seq.toList response.ConsumedCapacity) 0
        if response.HttpStatusCode <> HttpStatusCode.OK then
            failwithf "BatchWriteItem put request returned error %O" response.HttpStatusCode

        return unprocessedPutAttributeValues tableName response |> Array.map template.OfAttributeValues
    }


    /// <summary>
    ///     Asynchronously updates item with supplied key using provided update expression.
    /// </summary>
    /// <param name="key">Key of item to be updated.</param>
    /// <param name="updater">Table update expression.</param>
    /// <param name="precondition">Specifies a precondition expression that existing item should satisfy.</param>
    /// <param name="returnLatest">Specifies the operation should return the latest (true) or older (false) version of the item. Defaults to latest.</param>
    /// <param name="collector">Function to receive request metrics.</param>
    member __.UpdateItemAsync(key : TableKey, updater : UpdateExpression<'TRecord>,
                                ?precondition : ConditionExpression<'TRecord>, ?returnLatest : bool,
                                ?collector) : Async<'TRecord> = async {

        let kav = template.ToAttributeValues(key)
        let request = UpdateItemRequest(Key = kav, TableName = tableName)
        request.ReturnValues <-
            if defaultArg returnLatest true then ReturnValue.ALL_NEW
            else ReturnValue.ALL_OLD

        let returnConsumedCapacity, maybeReport = metricsOptions collector
        request.ReturnConsumedCapacity <- returnConsumedCapacity

        let writer = AttributeWriter(request.ExpressionAttributeNames, request.ExpressionAttributeValues)
        request.UpdateExpression <- updater.UpdateOps.Write(writer)

        match precondition with
        | Some pc -> request.ConditionExpression <- pc.Conditional.Write writer
        | _ -> ()

        let! ct = Async.CancellationToken
        let! response = client.UpdateItemAsync(request, ct) |> Async.AwaitTaskCorrect
        match maybeReport with None -> () | Some r -> r UpdateItem [ response.ConsumedCapacity ] 0
        if response.HttpStatusCode <> HttpStatusCode.OK then
            failwithf "UpdateItem request returned error %O" response.HttpStatusCode

        return template.OfAttributeValues response.Attributes
    }

    /// <summary>
    ///     Asynchronously updates item with supplied key using provided record update expression.
    /// </summary>
    /// <param name="key">Key of item to be updated.</param>
    /// <param name="updateExpr">Table update expression.</param>
    /// <param name="precondition">Specifies a precondition expression that existing item should satisfy.</param>
    /// <param name="returnLatest">Specifies the operation should return the latest (true) or older (false) version of the item. Defaults to latest.</param>
    /// <param name="collector">Function to receive request metrics.</param>
    member __.UpdateItemAsync(key : TableKey, updateExpr : Expr<'TRecord -> 'TRecord>,
                                ?precondition : Expr<'TRecord -> bool>, ?returnLatest : bool, ?collector) = async {
        let updater = template.PrecomputeUpdateExpr updateExpr
        let precondition = precondition |> Option.map template.PrecomputeConditionalExpr
        return! __.UpdateItemAsync(key, updater, ?returnLatest = returnLatest, ?precondition = precondition, ?collector = collector)
    }

    /// <summary>
    ///     Asynchronously updates item with supplied key using provided update operation expression.
    /// </summary>
    /// <param name="key">Key of item to be updated.</param>
    /// <param name="updateExpr">Table update expression.</param>
    /// <param name="precondition">Specifies a precondition expression that existing item should satisfy.</param>
    /// <param name="returnLatest">Specifies the operation should return the latest (true) or older (false) version of the item. Defaults to latest.</param>
    /// <param name="collector">Function to receive request metrics.</param>
    member __.UpdateItemAsync(key : TableKey, updateExpr : Expr<'TRecord -> UpdateOp>,
                                ?precondition : Expr<'TRecord -> bool>, ?returnLatest : bool, ?collector) = async {

        let updater = template.PrecomputeUpdateExpr updateExpr
        let precondition = precondition |> Option.map template.PrecomputeConditionalExpr
        return! __.UpdateItemAsync(key, updater, ?returnLatest = returnLatest, ?precondition = precondition, ?collector = collector)
    }


    /// <summary>
    ///     Asynchronously attempts to fetch item with given key from table. Returns None if no item with that key is present.
    /// </summary>
    /// <param name="key">Key of item to be fetched.</param>
    /// <param name="collector">Function to receive request metrics.</param>
    member __.TryGetItemAsync(key : TableKey, ?collector) : Async<'TRecord option> = async {
        let! response = tryGetItemAsync key None collector
        return response |> Option.map template.OfAttributeValues
    }


    /// <summary>
    ///     Asynchronously checks whether item of supplied key exists in table.
    /// </summary>
    /// <param name="key">Key to be checked.</param>
    /// <param name="collector">Function to receive request metrics.</param>
    member __.ContainsKeyAsync(key : TableKey, ?collector) : Async<bool> = async {
        let kav = template.ToAttributeValues(key)
        let request = GetItemRequest(tableName, kav)
        request.ExpressionAttributeNames.Add("#HKEY", template.PrimaryKey.HashKey.AttributeName)
        request.ProjectionExpression <- "#HKEY"
        let returnConsumedCapacity, maybeReport = metricsOptions collector
        request.ReturnConsumedCapacity <- returnConsumedCapacity
        let! ct = Async.CancellationToken
        let! response = client.GetItemAsync(request, ct) |> Async.AwaitTaskCorrect
        match maybeReport with None -> () | Some r -> r GetItem [ response.ConsumedCapacity ] 0
        return response.IsItemSet
    }


    /// <summary>
    ///     Asynchronously fetches item of given key from table.
    /// </summary>
    /// <param name="key">Key of item to be fetched.</param>
    /// <param name="collector">Function to receive request metrics.</param>
    member __.GetItemAsync(key : TableKey, ?collector) : Async<'TRecord> = async {
        let! item = getItemAsync key None collector
        return template.OfAttributeValues item
    }


    /// <summary>
    ///     Asynchronously fetches item of given key from table.
    ///     Uses supplied projection expression to narrow downloaded attributes.
    ///     Projection type must be a tuple of zero or more non-conflicting properties.
    /// </summary>
    /// <param name="key">Key of item to be fetched.</param>
    /// <param name="projection">Projection expression to be applied to item.</param>
    /// <param name="collector">Function to receive request metrics.</param>
    member __.GetItemProjectedAsync(key : TableKey, projection : ProjectionExpression<'TRecord, 'TProjection>, ?collector) : Async<'TProjection> = async {
        let! item = getItemAsync key (Some projection.ProjectionExpr) collector
        return projection.UnPickle item
    }

    /// <summary>
    ///     Asynchronously fetches item of given key from table.
    ///     Uses supplied projection expression to narrow downloaded attributes.
    ///     Projection type must be a tuple of zero or more non-conflicting properties.
    /// </summary>
    /// <param name="key">Key of item to be fetched.</param>
    /// <param name="projection">Projection expression to be applied to item.</param>
    /// <param name="collector">Function to receive request metrics.</param>
    member __.GetItemProjectedAsync(key : TableKey, projection : Expr<'TRecord -> 'TProjection>, ?collector) : Async<'TProjection> = async {
        return! __.GetItemProjectedAsync(key, template.PrecomputeProjectionExpr projection, ?collector = collector)
    }


    /// <summary>
    ///     Asynchronously performs a batch fetch of items with supplied keys.
    /// </summary>
    /// <param name="keys">Keys of items to be fetched.</param>
    /// <param name="consistentRead">Perform consistent read. Defaults to false.</param>
    /// <param name="collector">Function to receive request metrics.</param>
    member __.BatchGetItemsAsync(keys : seq<TableKey>, ?consistentRead : bool, ?collector) : Async<'TRecord[]> = async {
        let! response = batchGetItemsAsync keys consistentRead None collector
        return response |> Seq.map template.OfAttributeValues |> Seq.toArray
    }


    /// <summary>
    ///     Asynchronously performs a batch fetch of items with supplied keys.
    /// </summary>
    /// <param name="keys">Keys of items to be fetched.</param>
    /// <param name="projection">Projection expression to be applied to item.</param>
    /// <param name="consistentRead">Perform consistent read. Defaults to false.</param>
    /// <param name="collector">Function to receive request metrics.</param>
    member __.BatchGetItemsProjectedAsync<'TProjection>(keys : seq<TableKey>, projection : ProjectionExpression<'TRecord, 'TProjection>,
                                                        ?consistentRead : bool, ?collector) : Async<'TProjection[]> = async {

        let! response = batchGetItemsAsync keys consistentRead (Some projection.ProjectionExpr) collector
        return response |> Seq.map projection.UnPickle |> Seq.toArray
    }

    /// <summary>
    ///     Asynchronously performs a batch fetch of items with supplied keys.
    /// </summary>
    /// <param name="keys">Keys of items to be fetched.</param>
    /// <param name="projection">Projection expression to be applied to item.</param>
    /// <param name="consistentRead">Perform consistent read. Defaults to false.</param>
    /// <param name="collector">Function to receive request metrics.</param>
    member __.BatchGetItemsProjectedAsync<'TProjection>(keys : seq<TableKey>, projection : Expr<'TRecord -> 'TProjection>,
                                                        ?consistentRead : bool, ?collector) : Async<'TProjection[]> = async {
        return! __.BatchGetItemsProjectedAsync(keys, template.PrecomputeProjectionExpr projection, ?consistentRead = consistentRead, ?collector = collector)
    }


    /// <summary>
    ///     Asynchronously deletes item of given key from table.
    /// </summary>
    /// <returns>The deleted item, or None if the item didn’t exist.</returns>
    /// <param name="key">Key of item to be deleted.</param>
    /// <param name="precondition">Specifies a precondition expression that existing item should satisfy.</param>
    /// <param name="collector">Function to receive request metrics.</param>
    member __.DeleteItemAsync(key : TableKey, ?precondition : ConditionExpression<'TRecord>, ?collector) : Async<'TRecord option> = async {
        let kav = template.ToAttributeValues key
        let request = DeleteItemRequest(tableName, kav)
        match precondition with
        | Some pc ->
            let writer = AttributeWriter(request.ExpressionAttributeNames, request.ExpressionAttributeValues)
            request.ConditionExpression <- pc.Conditional.Write writer
        | None -> ()

        request.ReturnValues <- ReturnValue.ALL_OLD
        let returnConsumedCapacity, maybeReport = metricsOptions collector
        request.ReturnConsumedCapacity <- returnConsumedCapacity
        let! ct = Async.CancellationToken
        let! response = client.DeleteItemAsync(request, ct) |> Async.AwaitTaskCorrect
        match maybeReport with None -> () | Some r -> r DeleteItem [ response.ConsumedCapacity ] 0
        if response.HttpStatusCode <> HttpStatusCode.OK then
            failwithf "DeleteItem request returned error %O" response.HttpStatusCode

        if response.Attributes.Count = 0 then
            return None
        else
            return template.OfAttributeValues response.Attributes |> Some
    }

    /// <summary>
    ///     Asynchronously deletes item of given key from table.
    /// </summary>
    /// <returns>The deleted item, or None if the item didn’t exist.</returns>
    /// <param name="key">Key of item to be deleted.</param>
    /// <param name="precondition">Specifies a precondition expression that existing item should satisfy.</param>
    /// <param name="collector">Function to receive request metrics.</param>
    member __.DeleteItemAsync(key : TableKey, precondition : Expr<'TRecord -> bool>, ?collector) : Async<'TRecord option> = async {
        return! __.DeleteItemAsync(key, template.PrecomputeConditionalExpr precondition, ?collector = collector)
    }


    /// <summary>
    ///     Asynchronously performs batch delete operation on items of given keys.
    /// </summary>
    /// <returns>Any unprocessed keys due to throttling.</returns>
    /// <param name="keys">Keys of items to be deleted.</param>
    /// <param name="collector">Function to receive request metrics.</param>
    member __.BatchDeleteItemsAsync(keys : seq<TableKey>, ?collector) = async {
        let mkDeleteRequest (key : TableKey) =
            let kav = template.ToAttributeValues(key)
            let pr = DeleteRequest(kav)
            WriteRequest(pr)

        let keys = Seq.toArray keys
        if keys.Length > 25 then invalidArg "items" "key length must be less than or equal to 25."
        let request = BatchWriteItemRequest()
        let deleteRequests = keys |> Seq.map mkDeleteRequest |> rlist
        request.RequestItems.[tableName] <- deleteRequests
        let returnConsumedCapacity, maybeReport = metricsOptions collector
        request.ReturnConsumedCapacity <- returnConsumedCapacity

        let! ct = Async.CancellationToken
        let! response = client.BatchWriteItemAsync(request, ct) |> Async.AwaitTaskCorrect
        match maybeReport with None -> () | Some r -> r BatchWriteItems (Seq.toList response.ConsumedCapacity) 0
        if response.HttpStatusCode <> HttpStatusCode.OK then
            failwithf "BatchWriteItem deletion request returned error %O" response.HttpStatusCode

        return unprocessedDeleteAttributeValues tableName response |> Array.map template.ExtractKey
    }


    /// <summary>
    ///     Asynchronously queries table with given condition expressions.
    /// </summary>
    /// <param name="keyCondition">Key condition expression.</param>
    /// <param name="filterCondition">Filter condition expression.</param>
    /// <param name="limit">Maximum number of items to evaluate.</param>
    /// <param name="consistentRead">Specify whether to perform consistent read operation.</param>
    /// <param name="scanIndexForward">Specifies the order in which to evaluate results. Either ascending (true) or descending (false).</param>
    /// <param name="collector">Function to receive request metrics.</param>
    member __.QueryAsync(keyCondition : ConditionExpression<'TRecord>, ?filterCondition : ConditionExpression<'TRecord>,
                            ?limit: int, ?consistentRead : bool, ?scanIndexForward : bool, ?collector) : Async<'TRecord []> = async {

        let filterCondition = filterCondition |> Option.map (fun fc -> fc.Conditional)
        let! downloaded = queryAsync keyCondition.Conditional filterCondition None limit consistentRead scanIndexForward collector
        return downloaded |> Seq.map template.OfAttributeValues |> Seq.toArray
    }

    /// <summary>
    ///     Asynchronously queries table with given condition expressions.
    /// </summary>
    /// <param name="keyCondition">Key condition expression.</param>
    /// <param name="filterCondition">Filter condition expression.</param>
    /// <param name="limit">Maximum number of items to evaluate.</param>
    /// <param name="consistentRead">Specify whether to perform consistent read operation.</param>
    /// <param name="scanIndexForward">Specifies the order in which to evaluate results. Either ascending (true) or descending (false).</param>
    /// <param name="collector">Function to receive request metrics.</param>
    member __.QueryAsync(keyCondition : Expr<'TRecord -> bool>, ?filterCondition : Expr<'TRecord -> bool>,
                            ?limit : int, ?consistentRead : bool, ?scanIndexForward : bool, ?collector) : Async<'TRecord []> = async {

        let kc = template.PrecomputeConditionalExpr keyCondition
        let fc = filterCondition |> Option.map template.PrecomputeConditionalExpr
        return! __.QueryAsync(kc, ?filterCondition = fc, ?limit = limit, ?consistentRead = consistentRead, ?scanIndexForward = scanIndexForward, ?collector = collector)
    }


    /// <summary>
    ///     Asynchronously queries table with given condition expressions.
    ///     Uses supplied projection expression to narrow downloaded attributes.
    ///     Projection type must be a tuple of zero or more non-conflicting properties.
    /// </summary>
    /// <param name="keyCondition">Key condition expression.</param>
    /// <param name="projection">Projection expression.</param>
    /// <param name="filterCondition">Filter condition expression.</param>
    /// <param name="limit">Maximum number of items to evaluate.</param>
    /// <param name="consistentRead">Specify whether to perform consistent read operation.</param>
    /// <param name="scanIndexForward">Specifies the order in which to evaluate results. Either ascending (true) or descending (false).</param>
    /// <param name="collector">Function to receive request metrics.</param>
    member __.QueryProjectedAsync<'TProjection>(keyCondition : ConditionExpression<'TRecord>, projection : ProjectionExpression<'TRecord, 'TProjection>,
                                                ?filterCondition : ConditionExpression<'TRecord>,
                                                ?limit: int, ?consistentRead : bool, ?scanIndexForward : bool, ?collector) : Async<'TProjection []> = async {

        let filterCondition = filterCondition |> Option.map (fun fc -> fc.Conditional)
        let! downloaded = queryAsync keyCondition.Conditional filterCondition None limit consistentRead scanIndexForward collector
        return downloaded |> Seq.map projection.UnPickle |> Seq.toArray
    }

    /// <summary>
    ///     Asynchronously queries table with given condition expressions.
    ///     Uses supplied projection expression to narrow downloaded attributes.
    ///     Projection type must be a tuple of zero or more non-conflicting properties.
    /// </summary>
    /// <param name="keyCondition">Key condition expression.</param>
    /// <param name="projection">Projection expression.</param>
    /// <param name="filterCondition">Filter condition expression.</param>
    /// <param name="limit">Maximum number of items to evaluate.</param>
    /// <param name="consistentRead">Specify whether to perform consistent read operation.</param>
    /// <param name="scanIndexForward">Specifies the order in which to evaluate results. Either ascending (true) or descending (false).</param>
    /// <param name="collector">Function to receive request metrics.</param>
    member __.QueryProjectedAsync<'TProjection>(keyCondition : Expr<'TRecord -> bool>, projection : Expr<'TRecord -> 'TProjection>,
                                                ?filterCondition : Expr<'TRecord -> bool>,
                                                ?limit: int, ?consistentRead : bool, ?scanIndexForward : bool, ?collector) : Async<'TProjection []> = async {

        let filterCondition = filterCondition |> Option.map (fun fc -> template.PrecomputeConditionalExpr fc)
        return! __.QueryProjectedAsync(template.PrecomputeConditionalExpr keyCondition, template.PrecomputeProjectionExpr projection,
                                        ?filterCondition = filterCondition, ?limit = limit, ?consistentRead = consistentRead,
                                        ?scanIndexForward = scanIndexForward, ?collector = collector)
    }


    /// <summary>
    ///     Asynchronously queries table with given condition expressions.
    /// </summary>
    /// <param name="keyCondition">Key condition expression.</param>
    /// <param name="filterCondition">Filter condition expression.</param>
    /// <param name="limit">Maximum number of items per page - DynamoDB default is used if not specified.</param>
    /// <param name="exclusiveStartKey">LastEvaluatedKey from the previous page.</param>
    /// <param name="consistentRead">Specify whether to perform consistent read operation.</param>
    /// <param name="scanIndexForward">Specifies the order in which to evaluate results. Either ascending (true) or descending (false).</param>
    /// <param name="collector">Function to receive request metrics.</param>
    member __.QueryPaginatedAsync(keyCondition : ConditionExpression<'TRecord>, ?filterCondition : ConditionExpression<'TRecord>,
                            ?limit: int, ?exclusiveStartKey: IndexKey, ?consistentRead : bool, ?scanIndexForward : bool, ?collector) : Async<PaginatedResult<'TRecord, IndexKey>> = async {

        let filterCondition = filterCondition |> Option.map (fun fc -> fc.Conditional)
        let! (downloaded, lastEvaluatedKey) = queryPaginatedAsync keyCondition.Conditional filterCondition None (LimitType.DefaultOrCount limit) exclusiveStartKey consistentRead scanIndexForward collector
        return { Records = downloaded |> Seq.map template.OfAttributeValues |> Seq.toArray; LastEvaluatedKey = lastEvaluatedKey }
    }

    /// <summary>
    ///     Asynchronously queries table with given condition expressions.
    /// </summary>
    /// <param name="keyCondition">Key condition expression.</param>
    /// <param name="filterCondition">Filter condition expression.</param>
    /// <param name="limit">Maximum number of items per page - DynamoDB default is used if not specified.</param>
    /// <param name="exclusiveStartKey">LastEvaluatedKey from the previous page.</param>
    /// <param name="consistentRead">Specify whether to perform consistent read operation.</param>
    /// <param name="scanIndexForward">Specifies the order in which to evaluate results. Either ascending (true) or descending (false).</param>
    /// <param name="collector">Function to receive request metrics.</param>
    member __.QueryPaginatedAsync(keyCondition : Expr<'TRecord -> bool>, ?filterCondition : Expr<'TRecord -> bool>,
                            ?limit : int, ?exclusiveStartKey: IndexKey, ?consistentRead : bool, ?scanIndexForward : bool, ?collector) : Async<PaginatedResult<'TRecord, IndexKey>> = async {

        let kc = template.PrecomputeConditionalExpr keyCondition
        let fc = filterCondition |> Option.map template.PrecomputeConditionalExpr
        return! __.QueryPaginatedAsync(kc, ?filterCondition = fc, ?limit = limit, ?exclusiveStartKey = exclusiveStartKey, ?consistentRead = consistentRead, ?scanIndexForward = scanIndexForward, ?collector = collector)
    }


    /// <summary>
    ///     Asynchronously queries table with given condition expressions.
    ///     Uses supplied projection expression to narrow downloaded attributes.
    ///     Projection type must be a tuple of zero or more non-conflicting properties.
    /// </summary>
    /// <param name="keyCondition">Key condition expression.</param>
    /// <param name="projection">Projection expression.</param>
    /// <param name="filterCondition">Filter condition expression.</param>
    /// <param name="limit">Maximum number of items per page - DynamoDB default is used if not specified.</param>
    /// <param name="exclusiveStartKey">LastEvaluatedKey from the previous page.</param>
    /// <param name="consistentRead">Specify whether to perform consistent read operation.</param>
    /// <param name="scanIndexForward">Specifies the order in which to evaluate results. Either ascending (true) or descending (false).</param>
    /// <param name="collector">Function to receive request metrics.</param>
    member __.QueryProjectedPaginatedAsync<'TProjection>(keyCondition : ConditionExpression<'TRecord>, projection : ProjectionExpression<'TRecord, 'TProjection>,
                                                ?filterCondition : ConditionExpression<'TRecord>,
                                                ?limit: int, ?exclusiveStartKey: IndexKey, ?consistentRead : bool, ?scanIndexForward : bool, ?collector) : Async<PaginatedResult<'TProjection, IndexKey>> = async {

        let filterCondition = filterCondition |> Option.map (fun fc -> fc.Conditional)
        let! (downloaded, lastEvaluatedKey) = queryPaginatedAsync keyCondition.Conditional filterCondition None (LimitType.DefaultOrCount limit) exclusiveStartKey consistentRead scanIndexForward collector
        return { Records= downloaded |> Seq.map projection.UnPickle |> Seq.toArray; LastEvaluatedKey = lastEvaluatedKey }
    }

    /// <summary>
    ///     Asynchronously queries table with given condition expressions.
    ///     Uses supplied projection expression to narrow downloaded attributes.
    ///     Projection type must be a tuple of zero or more non-conflicting properties.
    /// </summary>
    /// <param name="keyCondition">Key condition expression.</param>
    /// <param name="projection">Projection expression.</param>
    /// <param name="filterCondition">Filter condition expression.</param>
    /// <param name="limit">Maximum number of items per page - DynamoDB default is used if not specified.</param>
    /// <param name="exclusiveStartKey">LastEvaluatedKey from the previous page.</param>
    /// <param name="consistentRead">Specify whether to perform consistent read operation.</param>
    /// <param name="scanIndexForward">Specifies the order in which to evaluate results. Either ascending (true) or descending (false).</param>
    /// <param name="collector">Function to receive request metrics.</param>
    member __.QueryProjectedPaginatedAsync<'TProjection>(keyCondition : Expr<'TRecord -> bool>, projection : Expr<'TRecord -> 'TProjection>,
                                                ?filterCondition : Expr<'TRecord -> bool>,
                                                ?limit: int, ?exclusiveStartKey: IndexKey, ?consistentRead : bool, ?scanIndexForward : bool, ?collector) : Async<PaginatedResult<'TProjection, IndexKey>> = async {

        let filterCondition = filterCondition |> Option.map (fun fc -> template.PrecomputeConditionalExpr fc)
        return! __.QueryProjectedPaginatedAsync(template.PrecomputeConditionalExpr keyCondition, template.PrecomputeProjectionExpr projection,
                                        ?filterCondition = filterCondition, ?limit = limit, ?exclusiveStartKey = exclusiveStartKey,
                                        ?consistentRead = consistentRead, ?scanIndexForward = scanIndexForward, ?collector = collector)
    }


    /// <summary>
    ///     Asynchronously scans table with given condition expressions.
    /// </summary>
    /// <param name="filterCondition">Filter condition expression.</param>
    /// <param name="limit">Maximum number of items to evaluate.</param>
    /// <param name="consistentRead">Specify whether to perform consistent read operation.</param>
    /// <param name="collector">Function to receive request metrics.</param>
    member __.ScanAsync(?filterCondition : ConditionExpression<'TRecord>, ?limit : int, ?consistentRead : bool, ?collector) : Async<'TRecord []> = async {
        let filterCondition = filterCondition |> Option.map (fun fc -> fc.Conditional)
        let! downloaded = scanAsync filterCondition None limit consistentRead collector
        return downloaded |> Seq.map template.OfAttributeValues |> Seq.toArray
    }

    /// <summary>
    ///     Asynchronously scans table with given condition expressions.
    /// </summary>
    /// <param name="filterCondition">Filter condition expression.</param>
    /// <param name="limit">Maximum number of items to evaluate.</param>
    /// <param name="consistentRead">Specify whether to perform consistent read operation.</param>
    /// <param name="collector">Function to receive request metrics.</param>
    member __.ScanAsync(filterCondition : Expr<'TRecord -> bool>, ?limit : int, ?consistentRead : bool, ?collector) : Async<'TRecord []> = async {
        let cond = template.PrecomputeConditionalExpr filterCondition
        return! __.ScanAsync(cond, ?limit = limit, ?consistentRead = consistentRead, ?collector = collector)
    }


    /// <summary>
    ///     Asynchronously scans table with given condition expressions.
    ///     Uses supplied projection expression to narrow downloaded attributes.
    ///     Projection type must be a tuple of zero or more non-conflicting properties.
    /// </summary>
    /// <param name="projection">Projection expression.</param>
    /// <param name="filterCondition">Filter condition expression.</param>
    /// <param name="limit">Maximum number of items to evaluate.</param>
    /// <param name="consistentRead">Specify whether to perform consistent read operation.</param>
    /// <param name="collector">Function to receive request metrics.</param>
    member __.ScanProjectedAsync<'TProjection>(projection : ProjectionExpression<'TRecord, 'TProjection>,
                                                ?filterCondition : ConditionExpression<'TRecord>,
                                                ?limit : int, ?consistentRead : bool, ?collector) : Async<'TProjection []> = async {
        let filterCondition = filterCondition |> Option.map (fun fc -> fc.Conditional)
        let! downloaded = scanAsync filterCondition (Some projection.ProjectionExpr) limit consistentRead collector
        return downloaded |> Seq.map projection.UnPickle |> Seq.toArray
    }

    /// <summary>
    ///     Asynchronously scans table with given condition expressions.
    ///     Uses supplied projection expression to narrow downloaded attributes.
    ///     Projection type must be a tuple of zero or more non-conflicting properties.
    /// </summary>
    /// <param name="projection">Projection expression.</param>
    /// <param name="filterCondition">Filter condition expression.</param>
    /// <param name="limit">Maximum number of items to evaluate.</param>
    /// <param name="consistentRead">Specify whether to perform consistent read operation.</param>
    /// <param name="collector">Function to receive request metrics.</param>
    member __.ScanProjectedAsync<'TProjection>(projection : Expr<'TRecord -> 'TProjection>,
                                                ?filterCondition : Expr<'TRecord -> bool>,
                                                ?limit : int, ?consistentRead : bool, ?collector) : Async<'TProjection []> = async {
        let filterCondition = filterCondition |> Option.map (fun fc -> template.PrecomputeConditionalExpr fc)
        return! __.ScanProjectedAsync(template.PrecomputeProjectionExpr projection, ?filterCondition = filterCondition,
                                        ?limit = limit, ?consistentRead = consistentRead, ?collector = collector)
    }


    /// <summary>
    ///     Asynchronously scans table with given condition expressions.
    /// </summary>
    /// <param name="filterCondition">Filter condition expression.</param>
    /// <param name="limit">Maximum number of items per page - DynamoDB default is used if not specified.</param>
    /// <param name="exclusiveStartKey">LastEvaluatedKey from the previous page.</param>
    /// <param name="consistentRead">Specify whether to perform consistent read operation.</param>
    /// <param name="collector">Function to receive request metrics.</param>
    member __.ScanPaginatedAsync(?filterCondition : ConditionExpression<'TRecord>, ?limit : int, ?exclusiveStartKey : TableKey, ?consistentRead : bool, ?collector) : Async<PaginatedResult<'TRecord, TableKey>> = async {
        let filterCondition = filterCondition |> Option.map (fun fc -> fc.Conditional)
        let! (downloaded, lastEvaluatedKey) = scanPaginatedAsync filterCondition None (LimitType.DefaultOrCount limit) exclusiveStartKey consistentRead collector
        return { Records = downloaded |> Seq.map template.OfAttributeValues |> Seq.toArray; LastEvaluatedKey = lastEvaluatedKey }
    }

    /// <summary>
    ///     Asynchronously scans table with given condition expressions.
    /// </summary>
    /// <param name="filterCondition">Filter condition expression.</param>
    /// <param name="limit">Maximum number of items per page - DynamoDB default is used if not specified.</param>
    /// <param name="exclusiveStartKey">LastEvaluatedKey from the previous page.</param>
    /// <param name="consistentRead">Specify whether to perform consistent read operation.</param>
    /// <param name="collector">Function to receive request metrics.</param>
    member __.ScanPaginatedAsync(filterCondition : Expr<'TRecord -> bool>, ?limit : int, ?exclusiveStartKey : TableKey, ?consistentRead : bool, ?collector) : Async<PaginatedResult<'TRecord, TableKey>> = async {
        let cond = template.PrecomputeConditionalExpr filterCondition
        return! __.ScanPaginatedAsync(cond, ?limit = limit, ?exclusiveStartKey = exclusiveStartKey, ?consistentRead = consistentRead, ?collector = collector)
    }


    /// <summary>
    ///     Asynchronously scans table with given condition expressions.
    ///     Uses supplied projection expression to narrow downloaded attributes.
    ///     Projection type must be a tuple of zero or more non-conflicting properties.
    /// </summary>
    /// <param name="projection">Projection expression.</param>
    /// <param name="filterCondition">Filter condition expression.</param>
    /// <param name="limit">Maximum number of items per page - DynamoDB default is used if not specified.</param>
    /// <param name="exclusiveStartKey">LastEvaluatedKey from the previous page.</param>
    /// <param name="consistentRead">Specify whether to perform consistent read operation.</param>
    /// <param name="collector">Function to receive request metrics.</param>
    member __.ScanProjectedPaginatedAsync<'TProjection>(projection : ProjectionExpression<'TRecord, 'TProjection>,
                                                ?filterCondition : ConditionExpression<'TRecord>,
                                                ?limit : int, ?exclusiveStartKey : TableKey, ?consistentRead : bool, ?collector) : Async<PaginatedResult<'TProjection, TableKey>> = async {
        let filterCondition = filterCondition |> Option.map (fun fc -> fc.Conditional)
        let! (downloaded, lastEvaluatedKey) = scanPaginatedAsync filterCondition (Some projection.ProjectionExpr) (LimitType.DefaultOrCount limit) exclusiveStartKey consistentRead collector
        return { Records = downloaded |> Seq.map projection.UnPickle |> Seq.toArray; LastEvaluatedKey = lastEvaluatedKey }
    }

    /// <summary>
    ///     Asynchronously scans table with given condition expressions.
    ///     Uses supplied projection expression to narrow downloaded attributes.
    ///     Projection type must be a tuple of zero or more non-conflicting properties.
    /// </summary>
    /// <param name="projection">Projection expression.</param>
    /// <param name="filterCondition">Filter condition expression.</param>
    /// <param name="limit">Maximum number of items per page - DynamoDB default is used if not specified.</param>
    /// <param name="exclusiveStartKey">LastEvaluatedKey from the previous page.</param>
    /// <param name="consistentRead">Specify whether to perform consistent read operation.</param>
    /// <param name="collector">Function to receive request metrics.</param>
    member __.ScanProjectedPaginatedAsync<'TProjection>(projection : Expr<'TRecord -> 'TProjection>,
                                                ?filterCondition : Expr<'TRecord -> bool>,
                                                ?limit : int, ?exclusiveStartKey : TableKey, ?consistentRead : bool, ?collector) : Async<PaginatedResult<'TProjection, TableKey>> = async {
        let filterCondition = filterCondition |> Option.map (fun fc -> template.PrecomputeConditionalExpr fc)
        return! __.ScanProjectedPaginatedAsync(template.PrecomputeProjectionExpr projection, ?filterCondition = filterCondition,
                                        ?limit = limit, ?exclusiveStartKey = exclusiveStartKey, ?consistentRead = consistentRead, ?collector = collector)
    }


    /// <summary>
    ///     Asynchronously updates the underlying table with supplied provisioned throughput.
    /// </summary>
    /// <param name="provisionedThroughput">Provisioned throughput to use on table.</param>
    member __.UpdateProvisionedThroughputAsync(provisionedThroughput : ProvisionedThroughput) : Async<unit> = async {
        let request = UpdateTableRequest(tableName, provisionedThroughput)
        let! ct = Async.CancellationToken
        let! _response = client.UpdateTableAsync(request, ct) |> Async.AwaitTaskCorrect
        return ()
    }


    /// <summary>
    ///     Asynchronously verify that the table exists and is compatible with record key schema.
    /// </summary>
    /// <param name="createIfNotExists">Create the table instance now instance if it does not exist. Defaults to false.</param>
    /// <param name="provisionedThroughput">Provisioned throughput for the table if newly created. Defaults to (10,10).</param>
    member __.VerifyTableAsync(?createIfNotExists : bool, ?provisionedThroughput : ProvisionedThroughput) : Async<unit> = async {
        let createIfNotExists = defaultArg createIfNotExists false
        let (|Conflict|_|) (e : exn) =
            match e with
            | :? AmazonDynamoDBException as e when e.StatusCode = HttpStatusCode.Conflict -> Some()
            | :? ResourceInUseException -> Some ()
            | _ -> None

        let rec verify lastExn retries = async {
            match lastExn with
            | Some e when retries = 0 -> do! Async.Raise e
            | _ -> ()

            let! ct = Async.CancellationToken
            let! response =
                client.DescribeTableAsync(tableName, ct)
                |> Async.AwaitTaskCorrect
                |> Async.Catch

            match response with
            | Choice1Of2 td ->
                if td.Table.TableStatus <> TableStatus.ACTIVE then
                    do! Async.Sleep 2000
                    // wait indefinitely if table is in transition state
                    return! verify None retries
                else

                let existingSchema = TableKeySchemata.OfTableDescription td.Table
                if existingSchema <> template.Info.Schemata then
                    sprintf "table '%s' exists with key schema %A, which is incompatible with record '%O'."
                        tableName existingSchema typeof<'TRecord>
                    |> invalidOp

            | Choice2Of2 (:? ResourceNotFoundException) when createIfNotExists ->
                let provisionedThroughput =
                    match provisionedThroughput with
                    | None -> ProvisionedThroughput(10L,10L)
                    | Some pt -> pt

                let ctr = template.Info.Schemata.CreateCreateTableRequest (tableName, provisionedThroughput)
                let! ct = Async.CancellationToken
                let! response =
                    client.CreateTableAsync(ctr, ct)
                    |> Async.AwaitTaskCorrect
                    |> Async.Catch

                match response with
                | Choice1Of2 _ -> return! verify None retries
                | Choice2Of2 (Conflict as e) ->
                    do! Async.Sleep 2000
                    return! verify (Some e) (retries - 1)

                | Choice2Of2 e -> do! Async.Raise e

            | Choice2Of2 (Conflict as e) ->
                do! Async.Sleep 2000
                return! verify (Some e) (retries - 1)

            | Choice2Of2 e -> do! Async.Raise e
        }

        do! verify None 10
    }

/// Table context factory methods
type TableContext =

    /// <summary>
    ///     Creates a DynamoDB client instance for given F# record and table name.
    /// </summary>
    /// <param name="client">DynamoDB client instance.</param>
    /// <param name="tableName">Table name to target.</param>
    /// <param name="verifyTable">Verify that the table exists and is compatible with supplied record schema. Defaults to true.</param>
    /// <param name="createIfNotExists">Create the table now instance if it does not exist. Defaults to false.</param>
    /// <param name="provisionedThroughput">Provisioned throughput for the table if newly created. Defaults to (10,10).</param>
    /// <param name="metricsCollector">Function to receive request metrics.</param>
    static member CreateAsync<'TRecord>(client : IAmazonDynamoDB, tableName : string, ?verifyTable : bool, ?createIfNotExists : bool,
                                                                    ?provisionedThroughput : ProvisionedThroughput, ?metricsCollector : RequestMetrics -> unit) : Async<TableContext<'TRecord>> = async {

        if not <| isValidTableName tableName then invalidArg "tableName" "unsupported DynamoDB table name."
        let verifyTable = defaultArg verifyTable true
        let createIfNotExists = defaultArg createIfNotExists false
        let context = new TableContext<'TRecord>(client, tableName, RecordTemplate.Define<'TRecord>(), metricsCollector)
        if verifyTable || createIfNotExists then
            do! context.VerifyTableAsync(createIfNotExists = createIfNotExists, ?provisionedThroughput = provisionedThroughput)

        return context
    }

/// <summary>
/// Sync-over-Async helpers that can be opted-into when working in scripting scenarios.
/// For normal usage, the <c>Async</c> versions of any given API is recommended, in order to ensure one
/// implements correct handling of the intrinsic asynchronous nature of the underlying interface
/// </summary>
module Scripting =

    type TableContext<'TRecord> with

        /// <summary>
        ///     Puts a record item in the table.
        /// </summary>
        /// <param name="item">Item to be written.</param>
        /// <param name="precondition">Precondition to satisfy in case item already exists.</param>
        member __.PutItem(item : 'TRecord, ?precondition : ConditionExpression<'TRecord>) =
            __.PutItemAsync(item, ?precondition = precondition) |> Async.RunSynchronously

        /// <summary>
        ///     Puts a record item in the table.
        /// </summary>
        /// <param name="item">Item to be written.</param>
        /// <param name="precondition">Precondition to satisfy in case item already exists.</param>
        member __.PutItem(item : 'TRecord, precondition : Expr<'TRecord -> bool>) =
            __.PutItemAsync(item, precondition) |> Async.RunSynchronously


        /// <summary>
        ///     Puts a collection of items to the table as a batch write operation.
        ///     At most 25 items can be written in a single batch write operation.
        /// </summary>
        /// <returns>Any unprocessed items due to throttling.</returns>
        /// <param name="items">Items to be written.</param>
        member __.BatchPutItems(items : seq<'TRecord>) =
            __.BatchPutItemsAsync(items) |> Async.RunSynchronously


        /// <summary>
        ///     Updates item with supplied key using provided update expression.
        /// </summary>
        /// <param name="key">Key of item to be updated.</param>
        /// <param name="updater">Table update expression.</param>
        /// <param name="precondition">Specifies a precondition expression that existing item should satisfy.</param>
        /// <param name="returnLatest">Specifies the operation should return the latest (true) or older (false) version of the item. Defaults to latest.</param>
        member __.UpdateItem(key : TableKey, updater : UpdateExpression<'TRecord>,
                                    ?precondition : ConditionExpression<'TRecord>, ?returnLatest : bool) =
            __.UpdateItemAsync(key, updater, ?precondition = precondition, ?returnLatest = returnLatest)
            |> Async.RunSynchronously

        /// <summary>
        ///     Updates item with supplied key using provided record update expression.
        /// </summary>
        /// <param name="key">Key of item to be updated.</param>
        /// <param name="updater">Table update expression.</param>
        /// <param name="precondition">Specifies a precondition expression that existing item should satisfy.</param>
        /// <param name="returnLatest">Specifies the operation should return the latest (true) or older (false) version of the item. Defaults to latest.</param>
        member __.UpdateItem(key : TableKey, updater : Expr<'TRecord -> 'TRecord>,
                                    ?precondition : Expr<'TRecord -> bool>, ?returnLatest : bool) =
            __.UpdateItemAsync(key, updater, ?precondition = precondition, ?returnLatest = returnLatest)
            |> Async.RunSynchronously

        /// <summary>
        ///     Updates item with supplied key using provided record update operation.
        /// </summary>
        /// <param name="key">Key of item to be updated.</param>
        /// <param name="updater">Table update expression.</param>
        /// <param name="precondition">Specifies a precondition expression that existing item should satisfy.</param>
        /// <param name="returnLatest">Specifies the operation should return the latest (true) or older (false) version of the item. Defaults to latest.</param>
        member __.UpdateItem(key : TableKey, updater : Expr<'TRecord -> UpdateOp>,
                                    ?precondition : Expr<'TRecord -> bool>, ?returnLatest : bool) =
            __.UpdateItemAsync(key, updater, ?precondition = precondition, ?returnLatest = returnLatest)
            |> Async.RunSynchronously


        /// <summary>
        ///     Checks whether item of supplied key exists in table.
        /// </summary>
        /// <param name="key">Key to be checked.</param>
        member __.ContainsKey(key : TableKey) =
            __.ContainsKeyAsync(key) |> Async.RunSynchronously

        /// <summary>
        ///     Fetches item of given key from table.
        /// </summary>
        /// <param name="key">Key of item to be fetched.</param>
        member __.GetItem(key : TableKey) = __.GetItemAsync(key) |> Async.RunSynchronously


        /// <summary>
        ///     Fetches item of given key from table.
        ///     Uses supplied projection expression to narrow downloaded attributes.
        ///     Projection type must be a tuple of zero or more non-conflicting properties.
        /// </summary>
        /// <param name="key">Key of item to be fetched.</param>
        /// <param name="projection">Projection expression to be applied to item.</param>
        member __.GetItemProjected(key : TableKey, projection : ProjectionExpression<'TRecord, 'TProjection>) : 'TProjection =
            __.GetItemProjectedAsync(key, projection) |> Async.RunSynchronously

        /// <summary>
        ///     Fetches item of given key from table.
        ///     Uses supplied projection expression to narrow downloaded attributes.
        ///     Projection type must be a tuple of zero or more non-conflicting properties.
        /// </summary>
        /// <param name="key">Key of item to be fetched.</param>
        /// <param name="projection">Projection expression to be applied to item.</param>
        member __.GetItemProjected(key : TableKey, projection : Expr<'TRecord -> 'TProjection>) : 'TProjection =
            // TOCONSIDER implement in terms of Async equivalent as per the rest
            __.GetItemProjected(key, __.Template.PrecomputeProjectionExpr projection)


        /// <summary>
        ///     Performs a batch fetch of items with supplied keys.
        /// </summary>
        /// <param name="keys">Keys of items to be fetched.</param>
        /// <param name="consistentRead">Perform consistent read. Defaults to false.</param>
        member __.BatchGetItems(keys : seq<TableKey>, ?consistentRead : bool) =
            __.BatchGetItemsAsync(keys, ?consistentRead = consistentRead)
            |> Async.RunSynchronously


        /// <summary>
        ///     Asynchronously performs a batch fetch of items with supplied keys.
        /// </summary>
        /// <param name="keys">Keys of items to be fetched.</param>
        /// <param name="projection">Projection expression to be applied to item.</param>
        /// <param name="consistentRead">Perform consistent read. Defaults to false.</param>
        member __.BatchGetItemsProjected<'TProjection>(keys : seq<TableKey>, projection : ProjectionExpression<'TRecord, 'TProjection>,
                                                            ?consistentRead : bool) : 'TProjection [] =
            __.BatchGetItemsProjectedAsync(keys, projection, ?consistentRead = consistentRead) |> Async.RunSynchronously


        /// <summary>
        ///     Asynchronously performs a batch fetch of items with supplied keys.
        /// </summary>
        /// <param name="keys">Keys of items to be fetched.</param>
        /// <param name="projection">Projection expression to be applied to item.</param>
        /// <param name="consistentRead">Perform consistent read. Defaults to false.</param>
        member __.BatchGetItemsProjected<'TProjection>(keys : seq<TableKey>, projection : Expr<'TRecord -> 'TProjection>,
                                                            ?consistentRead : bool) : 'TProjection [] =
            __.BatchGetItemsProjectedAsync(keys, projection, ?consistentRead = consistentRead) |> Async.RunSynchronously

        /// <summary>
        ///     Deletes item of given key from table.
        /// </summary>
        /// <param name="key">Key of item to be deleted.</param>
        /// <param name="precondition">Specifies a precondition expression that existing item should satisfy.</param>
        member __.DeleteItem(key : TableKey, ?precondition : ConditionExpression<'TRecord>) =
            __.DeleteItemAsync(key, ?precondition = precondition) |> Async.RunSynchronously

        /// <summary>
        ///     Deletes item of given key from table.
        /// </summary>
        /// <param name="key">Key of item to be deleted.</param>
        /// <param name="precondition">Specifies a precondition expression that existing item should satisfy.</param>
        member __.DeleteItem(key : TableKey, precondition : Expr<'TRecord -> bool>) =
            __.DeleteItemAsync(key, precondition) |> Async.RunSynchronously


        /// <summary>
        ///     Performs batch delete operation on items of given keys.
        /// </summary>
        /// <returns>Any unprocessed keys due to throttling.</returns>
        /// <param name="keys">Keys of items to be deleted.</param>
        member __.BatchDeleteItems(keys : seq<TableKey>) =
            __.BatchDeleteItemsAsync(keys) |> Async.RunSynchronously


        /// <summary>
        ///     Queries table with given condition expressions.
        /// </summary>
        /// <param name="keyCondition">Key condition expression.</param>
        /// <param name="filterCondition">Filter condition expression.</param>
        /// <param name="limit">Maximum number of items to evaluate.</param>
        /// <param name="consistentRead">Specify whether to perform consistent read operation.</param>
        /// <param name="scanIndexForward">Specifies the order in which to evaluate results. Either ascending (true) or descending (false).</param>
        member __.Query(keyCondition : ConditionExpression<'TRecord>, ?filterCondition : ConditionExpression<'TRecord>,
                                ?limit: int, ?consistentRead : bool, ?scanIndexForward : bool) : 'TRecord[] =
            __.QueryAsync(keyCondition, ?filterCondition = filterCondition, ?limit = limit,
                            ?consistentRead = consistentRead, ?scanIndexForward = scanIndexForward)
            |> Async.RunSynchronously

        /// <summary>
        ///     Queries table with given condition expressions.
        /// </summary>
        /// <param name="keyCondition">Key condition expression.</param>
        /// <param name="filterCondition">Filter condition expression.</param>
        /// <param name="limit">Maximum number of items to evaluate.</param>
        /// <param name="consistentRead">Specify whether to perform consistent read operation.</param>
        /// <param name="scanIndexForward">Specifies the order in which to evaluate results. Either ascending (true) or descending (false).</param>
        member __.Query(keyCondition : Expr<'TRecord -> bool>, ?filterCondition : Expr<'TRecord -> bool>,
                                ?limit: int, ?consistentRead : bool, ?scanIndexForward : bool) : 'TRecord[] =
            __.QueryAsync(keyCondition, ?filterCondition = filterCondition, ?limit = limit,
                            ?consistentRead = consistentRead, ?scanIndexForward = scanIndexForward)
            |> Async.RunSynchronously


        /// <summary>
        ///     Queries table with given condition expressions.
        ///     Uses supplied projection expression to narrow downloaded attributes.
        ///     Projection type must be a tuple of zero or more non-conflicting properties.
        /// </summary>
        /// <param name="keyCondition">Key condition expression.</param>
        /// <param name="projection">Projection expression.</param>
        /// <param name="filterCondition">Filter condition expression.</param>
        /// <param name="limit">Maximum number of items to evaluate.</param>
        /// <param name="consistentRead">Specify whether to perform consistent read operation.</param>
        /// <param name="scanIndexForward">Specifies the order in which to evaluate results. Either ascending (true) or descending (false).</param>
        member __.QueryProjected<'TProjection>(keyCondition : ConditionExpression<'TRecord>, projection : ProjectionExpression<'TRecord, 'TProjection>,
                                                    ?filterCondition : ConditionExpression<'TRecord>,
                                                    ?limit: int, ?consistentRead : bool, ?scanIndexForward : bool) : 'TProjection [] =

            __.QueryProjectedAsync(keyCondition, projection, ?filterCondition = filterCondition, ?limit = limit,
                                    ?consistentRead = consistentRead, ?scanIndexForward = scanIndexForward)
            |> Async.RunSynchronously

        /// <summary>
        ///     Queries table with given condition expressions.
        ///     Uses supplied projection expression to narrow downloaded attributes.
        ///     Projection type must be a tuple of zero or more non-conflicting properties.
        /// </summary>
        /// <param name="keyCondition">Key condition expression.</param>
        /// <param name="projection">Projection expression.</param>
        /// <param name="filterCondition">Filter condition expression.</param>
        /// <param name="limit">Maximum number of items to evaluate.</param>
        /// <param name="consistentRead">Specify whether to perform consistent read operation.</param>
        /// <param name="scanIndexForward">Specifies the order in which to evaluate results. Either ascending (true) or descending (false).</param>
        member __.QueryProjected<'TProjection>(keyCondition : Expr<'TRecord -> bool>, projection : Expr<'TRecord -> 'TProjection>,
                                                    ?filterCondition : Expr<'TRecord -> bool>,
                                                    ?limit: int, ?consistentRead : bool, ?scanIndexForward : bool) : 'TProjection [] =

            __.QueryProjectedAsync(keyCondition, projection, ?filterCondition = filterCondition, ?limit = limit,
                                    ?consistentRead = consistentRead, ?scanIndexForward = scanIndexForward)
            |> Async.RunSynchronously


        /// <summary>
        ///     Queries table with given condition expressions.
        /// </summary>
        /// <param name="keyCondition">Key condition expression.</param>
        /// <param name="filterCondition">Filter condition expression.</param>
        /// <param name="limit">Maximum number of items per page - DynamoDB default is used if not specified.</param>
        /// <param name="exclusiveStartKey">LastEvaluatedKey from the previous page.</param>
        /// <param name="consistentRead">Specify whether to perform consistent read operation.</param>
        /// <param name="scanIndexForward">Specifies the order in which to evaluate results. Either ascending (true) or descending (false).</param>
        member __.QueryPaginated(keyCondition : ConditionExpression<'TRecord>, ?filterCondition : ConditionExpression<'TRecord>,
                                ?limit: int, ?exclusiveStartKey: IndexKey, ?consistentRead : bool, ?scanIndexForward : bool) : PaginatedResult<'TRecord, IndexKey> =
            __.QueryPaginatedAsync(keyCondition, ?filterCondition = filterCondition, ?limit = limit, ?exclusiveStartKey = exclusiveStartKey,
                            ?consistentRead = consistentRead, ?scanIndexForward = scanIndexForward)
            |> Async.RunSynchronously

        /// <summary>
        ///     Queries table with given condition expressions.
        /// </summary>
        /// <param name="keyCondition">Key condition expression.</param>
        /// <param name="filterCondition">Filter condition expression.</param>
        /// <param name="limit">Maximum number of items per page - DynamoDB default is used if not specified.</param>
        /// <param name="exclusiveStartKey">LastEvaluatedKey from the previous page.</param>
        /// <param name="consistentRead">Specify whether to perform consistent read operation.</param>
        /// <param name="scanIndexForward">Specifies the order in which to evaluate results. Either ascending (true) or descending (false).</param>
        member __.QueryPaginated(keyCondition : Expr<'TRecord -> bool>, ?filterCondition : Expr<'TRecord -> bool>,
                                ?limit: int, ?exclusiveStartKey: IndexKey, ?consistentRead : bool, ?scanIndexForward : bool) : PaginatedResult<'TRecord, IndexKey> =
            __.QueryPaginatedAsync(keyCondition, ?filterCondition = filterCondition, ?limit = limit, ?exclusiveStartKey = exclusiveStartKey,
                            ?consistentRead = consistentRead, ?scanIndexForward = scanIndexForward)
            |> Async.RunSynchronously


        /// <summary>
        ///     Queries table with given condition expressions.
        ///     Uses supplied projection expression to narrow downloaded attributes.
        ///     Projection type must be a tuple of zero or more non-conflicting properties.
        /// </summary>
        /// <param name="keyCondition">Key condition expression.</param>
        /// <param name="projection">Projection expression.</param>
        /// <param name="filterCondition">Filter condition expression.</param>
        /// <param name="limit">Maximum number of items per page - DynamoDB default is used if not specified.</param>
        /// <param name="exclusiveStartKey">LastEvaluatedKey from the previous page.</param>
        /// <param name="consistentRead">Specify whether to perform consistent read operation.</param>
        /// <param name="scanIndexForward">Specifies the order in which to evaluate results. Either ascending (true) or descending (false).</param>
        member __.QueryProjectedPaginated<'TProjection>(keyCondition : ConditionExpression<'TRecord>, projection : ProjectionExpression<'TRecord, 'TProjection>,
                                                    ?filterCondition : ConditionExpression<'TRecord>,
                                                    ?limit: int, ?exclusiveStartKey: IndexKey, ?consistentRead : bool, ?scanIndexForward : bool) : PaginatedResult<'TProjection, IndexKey> =

            __.QueryProjectedPaginatedAsync(keyCondition, projection, ?filterCondition = filterCondition, ?limit = limit, ?exclusiveStartKey = exclusiveStartKey,
                                    ?consistentRead = consistentRead, ?scanIndexForward = scanIndexForward)
            |> Async.RunSynchronously

        /// <summary>
        ///     Queries table with given condition expressions.
        ///     Uses supplied projection expression to narrow downloaded attributes.
        ///     Projection type must be a tuple of zero or more non-conflicting properties.
        /// </summary>
        /// <param name="keyCondition">Key condition expression.</param>
        /// <param name="projection">Projection expression.</param>
        /// <param name="filterCondition">Filter condition expression.</param>
        /// <param name="limit">Maximum number of items per page - DynamoDB default is used if not specified.</param>
        /// <param name="exclusiveStartKey">LastEvaluatedKey from the previous page.</param>
        /// <param name="consistentRead">Specify whether to perform consistent read operation.</param>
        /// <param name="scanIndexForward">Specifies the order in which to evaluate results. Either ascending (true) or descending (false).</param>
        member __.QueryProjectedPaginated<'TProjection>(keyCondition : Expr<'TRecord -> bool>, projection : Expr<'TRecord -> 'TProjection>,
                                                    ?filterCondition : Expr<'TRecord -> bool>,
                                                    ?limit: int, ?exclusiveStartKey: IndexKey, ?consistentRead : bool, ?scanIndexForward : bool) : PaginatedResult<'TProjection, IndexKey> =

            __.QueryProjectedPaginatedAsync(keyCondition, projection, ?filterCondition = filterCondition, ?limit = limit, ?exclusiveStartKey = exclusiveStartKey,
                                    ?consistentRead = consistentRead, ?scanIndexForward = scanIndexForward)
            |> Async.RunSynchronously


        /// <summary>
        ///     Scans table with given condition expressions.
        /// </summary>
        /// <param name="filterCondition">Filter condition expression.</param>
        /// <param name="limit">Maximum number of items to evaluate.</param>
        /// <param name="consistentRead">Specify whether to perform consistent read operation.</param>
        member __.Scan(?filterCondition : ConditionExpression<'TRecord>, ?limit : int, ?consistentRead : bool) : 'TRecord [] =
            __.ScanAsync(?filterCondition = filterCondition, ?limit = limit, ?consistentRead = consistentRead)
            |> Async.RunSynchronously

        /// <summary>
        ///     Scans table with given condition expressions.
        /// </summary>
        /// <param name="filterCondition">Filter condition expression.</param>
        /// <param name="limit">Maximum number of items to evaluate.</param>
        /// <param name="consistentRead">Specify whether to perform consistent read operation.</param>
        member __.Scan(filterCondition : Expr<'TRecord -> bool>, ?limit : int, ?consistentRead : bool) : 'TRecord [] =
            __.ScanAsync(filterCondition, ?limit = limit, ?consistentRead = consistentRead)
            |> Async.RunSynchronously


        /// <summary>
        ///     Scans table with given condition expressions.
        ///     Uses supplied projection expression to narrow downloaded attributes.
        ///     Projection type must be a tuple of zero or more non-conflicting properties.
        /// </summary>
        /// <param name="projection">Projection expression.</param>
        /// <param name="filterCondition">Filter condition expression.</param>
        /// <param name="limit">Maximum number of items to evaluate.</param>
        /// <param name="consistentRead">Specify whether to perform consistent read operation.</param>
        member __.ScanProjected<'TProjection>(projection : ProjectionExpression<'TRecord, 'TProjection>,
                                                    ?filterCondition : ConditionExpression<'TRecord>,
                                                    ?limit : int, ?consistentRead : bool) : 'TProjection [] =
            __.ScanProjectedAsync(projection, ?filterCondition = filterCondition,
                                    ?limit = limit, ?consistentRead = consistentRead)
            |> Async.RunSynchronously

        /// <summary>
        ///     Scans table with given condition expressions.
        ///     Uses supplied projection expression to narrow downloaded attributes.
        ///     Projection type must be a tuple of zero or more non-conflicting properties.
        /// </summary>
        /// <param name="projection">Projection expression.</param>
        /// <param name="filterCondition">Filter condition expression.</param>
        /// <param name="limit">Maximum number of items to evaluate.</param>
        /// <param name="consistentRead">Specify whether to perform consistent read operation.</param>
        member __.ScanProjected<'TProjection>(projection : Expr<'TRecord -> 'TProjection>,
                                                    ?filterCondition : Expr<'TRecord -> bool>,
                                                    ?limit : int, ?consistentRead : bool) : 'TProjection [] =
            __.ScanProjectedAsync(projection, ?filterCondition = filterCondition,
                                    ?limit = limit, ?consistentRead = consistentRead)
            |> Async.RunSynchronously

        /// <summary>
        ///     Scans table with given condition expressions.
        /// </summary>
        /// <param name="filterCondition">Filter condition expression.</param>
        /// <param name="limit">Maximum number of items per page - DynamoDB default is used if not specified.</param>
        /// <param name="exclusiveStartKey">LastEvaluatedKey from the previous page.</param>
        /// <param name="consistentRead">Specify whether to perform consistent read operation.</param>
        member __.ScanPaginated(?filterCondition : ConditionExpression<'TRecord>, ?limit : int, ?exclusiveStartKey : TableKey, ?consistentRead : bool) : PaginatedResult<'TRecord, TableKey> =
            __.ScanPaginatedAsync(?filterCondition = filterCondition, ?limit = limit, ?exclusiveStartKey = exclusiveStartKey, ?consistentRead = consistentRead)
            |> Async.RunSynchronously


        /// <summary>
        ///     Scans table with given condition expressions.
        /// </summary>
        /// <param name="filterCondition">Filter condition expression.</param>
        /// <param name="limit">Maximum number of items per page - DynamoDB default is used if not specified.</param>
        /// <param name="exclusiveStartKey">LastEvaluatedKey from the previous page.</param>
        /// <param name="consistentRead">Specify whether to perform consistent read operation.</param>
        member __.ScanPaginated(filterCondition : Expr<'TRecord -> bool>, ?limit : int, ?exclusiveStartKey : TableKey, ?consistentRead : bool) : PaginatedResult<'TRecord, TableKey> =
            __.ScanPaginatedAsync(filterCondition, ?limit = limit, ?exclusiveStartKey = exclusiveStartKey, ?consistentRead = consistentRead)
            |> Async.RunSynchronously


        /// <summary>
        ///     Scans table with given condition expressions.
        ///     Uses supplied projection expression to narrow downloaded attributes.
        ///     Projection type must be a tuple of zero or more non-conflicting properties.
        /// </summary>
        /// <param name="projection">Projection expression.</param>
        /// <param name="filterCondition">Filter condition expression.</param>
        /// <param name="limit">Maximum number of items per page - DynamoDB default is used if not specified.</param>
        /// <param name="exclusiveStartKey">LastEvaluatedKey from the previous page.</param>
        /// <param name="consistentRead">Specify whether to perform consistent read operation.</param>
        member __.ScanProjectedPaginated<'TProjection>(projection : ProjectionExpression<'TRecord, 'TProjection>,
                                                    ?filterCondition : ConditionExpression<'TRecord>,
                                                    ?limit : int, ?exclusiveStartKey : TableKey, ?consistentRead : bool) : PaginatedResult<'TProjection, TableKey> =
            __.ScanProjectedPaginatedAsync(projection, ?filterCondition = filterCondition,
                                    ?limit = limit, ?exclusiveStartKey = exclusiveStartKey, ?consistentRead = consistentRead)
            |> Async.RunSynchronously

        /// <summary>
        ///     Scans table with given condition expressions.
        ///     Uses supplied projection expression to narrow downloaded attributes.
        ///     Projection type must be a tuple of zero or more non-conflicting properties.
        /// </summary>
        /// <param name="projection">Projection expression.</param>
        /// <param name="filterCondition">Filter condition expression.</param>
        /// <param name="limit">Maximum number of items per page - DynamoDB default is used if not specified.</param>
        /// <param name="exclusiveStartKey">LastEvaluatedKey from the previous page.</param>
        /// <param name="consistentRead">Specify whether to perform consistent read operation.</param>
        member __.ScanProjectedPaginated<'TProjection>(projection : Expr<'TRecord -> 'TProjection>,
                                                    ?filterCondition : Expr<'TRecord -> bool>,
                                                    ?limit : int, ?exclusiveStartKey : TableKey, ?consistentRead : bool) : PaginatedResult<'TProjection, TableKey> =
            __.ScanProjectedPaginatedAsync(projection, ?filterCondition = filterCondition,
                                    ?limit = limit, ?exclusiveStartKey = exclusiveStartKey, ?consistentRead = consistentRead)
            |> Async.RunSynchronously


        /// <summary>
        ///     Updates the underlying table with supplied provisioned throughput.
        /// </summary>
        /// <param name="provisionedThroughput">Provisioned throughput to use on table.</param>
        member __.UpdateProvisionedThroughput(provisionedThroughput : ProvisionedThroughput) =
            __.UpdateProvisionedThroughputAsync(provisionedThroughput) |> Async.RunSynchronously


        /// <summary>
        ///     Asynchronously verify that the table exists and is compatible with record key schema.
        /// </summary>
        /// <param name="createIfNotExists">Create the table instance now if it does not exist. Defaults to false.</param>
        /// <param name="provisionedThroughput">Provisioned throughput for the table if newly created.</param>
        member __.VerifyTable(?createIfNotExists : bool, ?provisionedThroughput : ProvisionedThroughput) =
            __.VerifyTableAsync(?createIfNotExists = createIfNotExists, ?provisionedThroughput = provisionedThroughput)
            |> Async.RunSynchronously

  type TableContext with

        /// <summary>
        ///     Creates a DynamoDB client instance for given F# record and table name.
        /// </summary>
        /// <param name="client">DynamoDB client instance.</param>
        /// <param name="tableName">Table name to target.</param>
        /// <param name="verifyTable">Verify that the table exists and is compatible with supplied record schema. Defaults to true.</param>
        /// <param name="createIfNotExists">Create the table now instance if it does not exist. Defaults to false.</param>
        /// <param name="provisionedThroughput">Provisioned throughput for the table if newly created.</param>
        /// <param name="metricsCollector">Function to receive request metrics.</param>
        static member Create<'TRecord>(client : IAmazonDynamoDB, tableName : string, ?verifyTable : bool, ?createIfNotExists : bool,
                                            ?provisionedThroughput : ProvisionedThroughput, ?metricsCollector : RequestMetrics -> unit) =
            TableContext.CreateAsync<'TRecord>(client, tableName, ?verifyTable = verifyTable, ?createIfNotExists = createIfNotExists,
                                                    ?provisionedThroughput = provisionedThroughput, ?metricsCollector = metricsCollector)
            |> Async.RunSynchronously
