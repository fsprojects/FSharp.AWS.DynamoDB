namespace FSharp.AWS.DynamoDB

open System.Collections.Generic
open System.Net

open Microsoft.FSharp.Quotations

open Amazon.DynamoDBv2
open Amazon.DynamoDBv2.Model

open FSharp.AWS.DynamoDB.ExprCommon

/// Exception raised by DynamoDB in case where write preconditions are not satisfied
type ConditionalCheckFailedException = Amazon.DynamoDBv2.Model.ConditionalCheckFailedException

/// Exception raised by DynamoDB in case where resources are not found
type ResourceNotFoundException = Amazon.DynamoDBv2.Model.ResourceNotFoundException

/// Represents the provisioned throughput for given table or index
type ProvisionedThroughput = Amazon.DynamoDBv2.Model.ProvisionedThroughput

/// Represents the throughput configuration for a Table
[<RequireQualifiedAccess>]
type Throughput =
    | Provisioned of ProvisionedThroughput
    | OnDemand
module internal Throughput =
    let applyToCreateRequest (req : CreateTableRequest) = function
        | Throughput.Provisioned t ->
            req.ProvisionedThroughput <- t
            for gsi in req.GlobalSecondaryIndexes do
                gsi.ProvisionedThroughput <- t
        | Throughput.OnDemand ->
            req.BillingMode <- BillingMode.PAY_PER_REQUEST
    let hasChanged (desc : TableDescription) = function
        | Throughput.Provisioned t ->
            let current = desc.ProvisionedThroughput
            desc.BillingModeSummary.BillingMode <> BillingMode.PROVISIONED
            || t.ReadCapacityUnits <> current.ReadCapacityUnits
            || t.WriteCapacityUnits <> current.WriteCapacityUnits
        | Throughput.OnDemand ->
            desc.BillingModeSummary.BillingMode <> BillingMode.PAY_PER_REQUEST
    let applyToUpdateRequest (req : UpdateTableRequest) = function
        | Throughput.Provisioned t ->
            req.BillingMode <- BillingMode.PROVISIONED
            req.ProvisionedThroughput <- t
        | Throughput.OnDemand ->
            req.BillingMode <- BillingMode.PAY_PER_REQUEST

/// Represents the streaming configuration for a Table
type Streaming =
    | Enabled of StreamViewType
    | Disabled
module internal Streaming =
    let private (|Spec|) = function
        | Streaming.Enabled svt -> StreamSpecification(StreamEnabled = true, StreamViewType = svt)
        | Streaming.Disabled -> StreamSpecification(StreamEnabled = false)
    let applyToCreateRequest (req : CreateTableRequest) (Spec spec) =
        req.StreamSpecification <- spec
    let hasChanged (desc : TableDescription) = function
        | Streaming.Disabled -> desc.StreamSpecification.StreamEnabled
        | Streaming.Enabled svt ->
            not desc.StreamSpecification.StreamEnabled
            || desc.StreamSpecification.StreamViewType <> svt
    let applyToUpdateRequest (req : UpdateTableRequest) (Spec spec) =
        req.StreamSpecification <- spec

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
        metricsCollector : (RequestMetrics -> unit) option) =

    let reportMetrics collector (operation : Operation) (consumedCapacity : ConsumedCapacity list) (itemCount : int) =
        collector { TableName = tableName; Operation = operation; ConsumedCapacity = consumedCapacity; ItemCount = itemCount }
    let returnConsumedCapacity, maybeReport =
        match metricsCollector with
        | Some sink -> ReturnConsumedCapacity.INDEXES, Some (reportMetrics sink)
        | None -> ReturnConsumedCapacity.NONE, None

    let tryGetItemAsync (key : TableKey) (proj : ProjectionExpr.ProjectionExpr option) = async {
        let kav = template.ToAttributeValues(key)
        let request = GetItemRequest(tableName, kav, ReturnConsumedCapacity = returnConsumedCapacity)
        match proj with
        | None -> ()
        | Some proj ->
            let aw = AttributeWriter(request.ExpressionAttributeNames, null)
            request.ProjectionExpression <- proj.Write aw

        let! ct = Async.CancellationToken
        let! response = client.GetItemAsync(request, ct) |> Async.AwaitTaskCorrect
        maybeReport |> Option.iter (fun r -> r GetItem [ response.ConsumedCapacity ] (if response.IsItemSet then 1 else 0))
        if response.HttpStatusCode <> HttpStatusCode.OK then
            failwithf "GetItem request returned error %O" response.HttpStatusCode

        if response.IsItemSet then return Some response.Item
        else return None
    }

    let getItemAsync (key : TableKey) (proj : ProjectionExpr.ProjectionExpr option) = async {
        match! tryGetItemAsync key proj with
        | Some item -> return item
        | None -> return raise <| ResourceNotFoundException(sprintf "could not find item %O" key)
    }

    let batchGetItemsAsync (keys : seq<TableKey>) (consistentRead : bool option)
                            (projExpr : ProjectionExpr.ProjectionExpr option) = async {

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

        let request = BatchGetItemRequest(ReturnConsumedCapacity = returnConsumedCapacity)
        request.RequestItems.[tableName] <- kna

        let! ct = Async.CancellationToken
        let! response = client.BatchGetItemAsync(request, ct) |> Async.AwaitTaskCorrect
        maybeReport |> Option.iter (fun r -> r BatchGetItems (List.ofSeq response.ConsumedCapacity) response.Responses.[tableName].Count)

        if response.HttpStatusCode <> HttpStatusCode.OK then
            failwithf "BatchGetItem request returned error %O" response.HttpStatusCode

        return response.Responses.[tableName]
    }

    let queryPaginatedAsync (keyCondition : ConditionalExpr.ConditionalExpression)
                    (filterCondition : ConditionalExpr.ConditionalExpression option)
                    (projectionExpr : ProjectionExpr.ProjectionExpr option)
                    (limit: LimitType) (exclusiveStartKey : IndexKey option)
                    (consistentRead : bool option) (scanIndexForward : bool option) = async {

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
        let consumedCapacity = new ResizeArray<ConsumedCapacity>()
        let emitMetrics () =
            maybeReport |> Option.iter (fun r -> r Query (Seq.toList consumedCapacity) downloaded.Count)
        let mutable lastEvaluatedKey : Dictionary<string,AttributeValue> option = None

        let rec aux last = async {
            let request = QueryRequest(tableName, ReturnConsumedCapacity = returnConsumedCapacity)
            keyCondition.IndexName |> Option.iter (fun name -> request.IndexName <- name)
            let writer = AttributeWriter(request.ExpressionAttributeNames, request.ExpressionAttributeValues)
            request.KeyConditionExpression <- keyCondition.Write writer

            match filterCondition with
            | None -> ()
            | Some fc -> request.FilterExpression <- fc.Write writer

            match projectionExpr with
            | None -> ()
            | Some pe -> request.ProjectionExpression <- pe.Write writer

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
                    (limit: int option)
                    (consistentRead : bool option)
                    (scanIndexForward : bool option) = async {

        let! (downloaded, _) = queryPaginatedAsync keyCondition filterCondition projectionExpr (LimitType.AllOrCount limit) None consistentRead scanIndexForward

        return downloaded
    }

    let scanPaginatedAsync (filterCondition : ConditionalExpr.ConditionalExpression option)
                            (projectionExpr : ProjectionExpr.ProjectionExpr option)
                            (limit : LimitType) (exclusiveStartKey : TableKey option) (consistentRead : bool option) = async {

        let downloaded = new ResizeArray<_>()
        let consumedCapacity = new ResizeArray<ConsumedCapacity>()
        let emitMetrics () =
            maybeReport |> Option.iter (fun r -> r Scan (Seq.toList consumedCapacity) downloaded.Count)
        let mutable lastEvaluatedKey : Dictionary<string,AttributeValue> option = None
        let rec aux last = async {
            let request = ScanRequest(tableName, ReturnConsumedCapacity = returnConsumedCapacity)
            let writer = AttributeWriter(request.ExpressionAttributeNames, request.ExpressionAttributeValues)
            match filterCondition with
            | None -> ()
            | Some fc -> request.FilterExpression <- fc.Write writer

            match projectionExpr with
            | None -> ()
            | Some pe -> request.ProjectionExpression <- pe.Write writer

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
                    (limit : int option) (consistentRead : bool option) = async {

        let! (downloaded, _) = scanPaginatedAsync filterCondition projectionExpr (LimitType.AllOrCount limit) None consistentRead

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


    /// <summary>
    ///     Creates a DynamoDB client instance for given F# record and table name.<br/>
    ///     For creating, provisioning or verification, see <c>InitializeTableAsync</c> and <c>VerifyTableAsync</c>.
    /// </summary>
    /// <param name="client">DynamoDB client instance.</param>
    /// <param name="tableName">Table name to target.</param>
    /// <param name="metricsCollector">Function to receive request metrics.</param>
    new (client : IAmazonDynamoDB, tableName : string, ?metricsCollector : RequestMetrics -> unit) =
        if not <| isValidTableName tableName then invalidArg "tableName" "unsupported DynamoDB table name."
        TableContext<'TRecord>(client, tableName, RecordTemplate.Define<'TRecord>(), metricsCollector)


    /// Creates a new table context instance that uses
    /// a new F# record type. The new F# record type
    /// must define a compatible key schema.
    member __.WithRecordType<'TRecord2>() : TableContext<'TRecord2> =
        let rd = RecordTemplate.Define<'TRecord2>()
        if template.PrimaryKey <> rd.PrimaryKey then
            invalidArg (string typeof<'TRecord2>) "incompatible key schema."

        new TableContext<'TRecord2>(client, tableName, rd, metricsCollector)

    /// Creates an identical table context with the specified metricsCollector callback replacing any previously specified one
    member __.WithMetricsCollector(collector : (RequestMetrics -> unit)) : TableContext<'TRecord> =
        new TableContext<'TRecord>(client, tableName, template, Some collector)

    /// <summary>
    ///     Asynchronously puts a record item in the table.
    /// </summary>
    /// <param name="item">Item to be written.</param>
    /// <param name="precondition">Precondition to satisfy in case item already exists.</param>
    member __.PutItemAsync(item : 'TRecord, ?precondition : ConditionExpression<'TRecord>) : Async<TableKey> = async {
        let attrValues = template.ToAttributeValues(item)
        let request = PutItemRequest(tableName, attrValues, ReturnValues = ReturnValue.NONE, ReturnConsumedCapacity = returnConsumedCapacity)
        match precondition with
        | Some pc ->
            let writer = AttributeWriter(request.ExpressionAttributeNames, request.ExpressionAttributeValues)
            request.ConditionExpression <- pc.Conditional.Write writer
        | _ -> ()

        let! ct = Async.CancellationToken
        let! response = client.PutItemAsync(request, ct) |> Async.AwaitTaskCorrect
        maybeReport |> Option.iter (fun r -> r PutItem [ response.ConsumedCapacity ] 1)
        if response.HttpStatusCode <> HttpStatusCode.OK then
            failwithf "PutItem request returned error %O" response.HttpStatusCode

        return template.ExtractKey item
    }

    /// <summary>
    ///     Asynchronously puts a record item in the table.
    /// </summary>
    /// <param name="item">Item to be written.</param>
    /// <param name="precondition">Precondition to satisfy in case item already exists.</param>
    member __.PutItemAsync(item : 'TRecord, precondition : Expr<'TRecord -> bool>) =
        __.PutItemAsync(item, template.PrecomputeConditionalExpr precondition)

    /// <summary>
    ///     Asynchronously puts a collection of items to the table as a batch write operation.
    ///     At most 25 items can be written in a single batch write operation.
    /// </summary>
    /// <returns>Any unprocessed items due to throttling.</returns>
    /// <param name="items">Items to be written.</param>
    member __.BatchPutItemsAsync(items : seq<'TRecord>) : Async<'TRecord[]> = async {
        let mkWriteRequest (item : 'TRecord) =
            let attrValues = template.ToAttributeValues(item)
            let pr = PutRequest(attrValues)
            WriteRequest(pr)

        let items = Seq.toArray items
        if items.Length > 25 then invalidArg "items" "item length must be less than or equal to 25."
        let writeRequests = items |> Seq.map mkWriteRequest |> rlist
        let pbr = BatchWriteItemRequest(ReturnConsumedCapacity = returnConsumedCapacity)
        pbr.RequestItems.[tableName] <- writeRequests
        let! ct = Async.CancellationToken
        let! response = client.BatchWriteItemAsync(pbr, ct) |> Async.AwaitTaskCorrect
        let unprocessed =
            match response.UnprocessedItems.TryGetValue tableName with
            | (true, reqs) ->
                reqs |> Seq.choose (fun r -> r.PutRequest |> Option.ofObj)
                        |> Seq.map (fun w -> w.Item)
                        |> Seq.toArray
            | (false, _) -> [||]
        maybeReport |> Option.iter (fun r -> r BatchWriteItems (Seq.toList response.ConsumedCapacity) (items.Length - unprocessed.Length))
        if response.HttpStatusCode <> HttpStatusCode.OK then
            failwithf "BatchWriteItem put request returned error %O" response.HttpStatusCode

        return unprocessed |> Array.map template.OfAttributeValues
    }


    /// <summary>
    ///     Asynchronously updates item with supplied key using provided update expression.
    /// </summary>
    /// <param name="key">Key of item to be updated.</param>
    /// <param name="updater">Table update expression.</param>
    /// <param name="precondition">Specifies a precondition expression that existing item should satisfy.</param>
    /// <param name="returnLatest">Specifies the operation should return the latest (true) or older (false) version of the item. Defaults to latest.</param>
    member __.UpdateItemAsync(key : TableKey, updater : UpdateExpression<'TRecord>,
                                ?precondition : ConditionExpression<'TRecord>,
                                ?returnLatest : bool) : Async<'TRecord> = async {

        let kav = template.ToAttributeValues(key)
        let request = UpdateItemRequest(Key = kav, TableName = tableName, ReturnConsumedCapacity = returnConsumedCapacity)
        request.ReturnValues <-
            if defaultArg returnLatest true then ReturnValue.ALL_NEW
            else ReturnValue.ALL_OLD

        let writer = AttributeWriter(request.ExpressionAttributeNames, request.ExpressionAttributeValues)
        request.UpdateExpression <- updater.UpdateOps.Write(writer)

        match precondition with
        | Some pc -> request.ConditionExpression <- pc.Conditional.Write writer
        | _ -> ()

        let! ct = Async.CancellationToken
        let! response = client.UpdateItemAsync(request, ct) |> Async.AwaitTaskCorrect
        maybeReport |> Option.iter (fun r -> r UpdateItem [ response.ConsumedCapacity ] 1)
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
    member __.UpdateItemAsync(key : TableKey, updateExpr : Expr<'TRecord -> 'TRecord>,
                                ?precondition : Expr<'TRecord -> bool>,
                                ?returnLatest : bool) = async {
        let updater = template.PrecomputeUpdateExpr updateExpr
        let precondition = precondition |> Option.map template.PrecomputeConditionalExpr
        return! __.UpdateItemAsync(key, updater, ?returnLatest = returnLatest, ?precondition = precondition)
    }

    /// <summary>
    ///     Asynchronously updates item with supplied key using provided update operation expression.
    /// </summary>
    /// <param name="key">Key of item to be updated.</param>
    /// <param name="updateExpr">Table update expression.</param>
    /// <param name="precondition">Specifies a precondition expression that existing item should satisfy.</param>
    /// <param name="returnLatest">Specifies the operation should return the latest (true) or older (false) version of the item. Defaults to latest.</param>
    member __.UpdateItemAsync(key : TableKey, updateExpr : Expr<'TRecord -> UpdateOp>,
                                ?precondition : Expr<'TRecord -> bool>, ?returnLatest : bool) = async {

        let updater = template.PrecomputeUpdateExpr updateExpr
        let precondition = precondition |> Option.map template.PrecomputeConditionalExpr
        return! __.UpdateItemAsync(key, updater, ?returnLatest = returnLatest, ?precondition = precondition)
    }


    /// <summary>
    ///     Asynchronously attempts to fetch item with given key from table. Returns None if no item with that key is present.
    /// </summary>
    /// <param name="key">Key of item to be fetched.</param>
    member __.TryGetItemAsync(key : TableKey) : Async<'TRecord option> = async {
        let! response = tryGetItemAsync key None
        return response |> Option.map template.OfAttributeValues
    }


    /// <summary>
    ///     Asynchronously checks whether item of supplied key exists in table.
    /// </summary>
    /// <param name="key">Key to be checked.</param>
    member __.ContainsKeyAsync(key : TableKey) : Async<bool> = async {
        let kav = template.ToAttributeValues(key)
        let request = GetItemRequest(tableName, kav, ReturnConsumedCapacity = returnConsumedCapacity)
        request.ExpressionAttributeNames.Add("#HKEY", template.PrimaryKey.HashKey.AttributeName)
        request.ProjectionExpression <- "#HKEY"
        let! ct = Async.CancellationToken
        let! response = client.GetItemAsync(request, ct) |> Async.AwaitTaskCorrect
        maybeReport |> Option.iter (fun r -> r GetItem [ response.ConsumedCapacity ] 1)
        return response.IsItemSet
    }


    /// <summary>
    ///     Asynchronously fetches item of given key from table.<br/>
    ///     Throws <c>ResourceNotFoundException</c> if no item found (NOTE while that specific exception is misleading, fixing it is a breaking change).<br/>
    ///     See <c>TryGetItemAsync</c> if you need to implement fallback logic in the case where it is not found
    /// </summary>
    /// <param name="key">Key of item to be fetched.</param>
    member __.GetItemAsync(key : TableKey) : Async<'TRecord> = async {
        let! item = getItemAsync key None
        return template.OfAttributeValues item
    }


    /// <summary>
    ///     Asynchronously fetches item of given key from table.
    ///     Uses supplied projection expression to narrow downloaded attributes.
    ///     Projection type must be a tuple of zero or more non-conflicting properties.
    /// </summary>
    /// <param name="key">Key of item to be fetched.</param>
    /// <param name="projection">Projection expression to be applied to item.</param>
    member __.GetItemProjectedAsync(key : TableKey, projection : ProjectionExpression<'TRecord, 'TProjection>) : Async<'TProjection> = async {
        let! item = getItemAsync key (Some projection.ProjectionExpr)
        return projection.UnPickle item
    }

    /// <summary>
    ///     Asynchronously fetches item of given key from table.
    ///     Uses supplied projection expression to narrow downloaded attributes.
    ///     Projection type must be a tuple of zero or more non-conflicting properties.
    /// </summary>
    /// <param name="key">Key of item to be fetched.</param>
    /// <param name="projection">Projection expression to be applied to item.</param>
    member __.GetItemProjectedAsync(key : TableKey, projection : Expr<'TRecord -> 'TProjection>) : Async<'TProjection> =
        __.GetItemProjectedAsync(key, template.PrecomputeProjectionExpr projection)

    /// <summary>
    ///     Asynchronously performs a batch fetch of items with supplied keys.
    /// </summary>
    /// <param name="keys">Keys of items to be fetched.</param>
    /// <param name="consistentRead">Perform consistent read. Defaults to false.</param>
    member __.BatchGetItemsAsync(keys : seq<TableKey>, ?consistentRead : bool) : Async<'TRecord[]> = async {
        let! response = batchGetItemsAsync keys consistentRead None
        return response |> Seq.map template.OfAttributeValues |> Seq.toArray
    }


    /// <summary>
    ///     Asynchronously performs a batch fetch of items with supplied keys.
    /// </summary>
    /// <param name="keys">Keys of items to be fetched.</param>
    /// <param name="projection">Projection expression to be applied to item.</param>
    /// <param name="consistentRead">Perform consistent read. Defaults to false.</param>
    member __.BatchGetItemsProjectedAsync<'TProjection>(keys : seq<TableKey>, projection : ProjectionExpression<'TRecord, 'TProjection>,
                                                        ?consistentRead : bool) : Async<'TProjection[]> = async {

        let! response = batchGetItemsAsync keys consistentRead (Some projection.ProjectionExpr)
        return response |> Seq.map projection.UnPickle |> Seq.toArray
    }

    /// <summary>
    ///     Asynchronously performs a batch fetch of items with supplied keys.
    /// </summary>
    /// <param name="keys">Keys of items to be fetched.</param>
    /// <param name="projection">Projection expression to be applied to item.</param>
    /// <param name="consistentRead">Perform consistent read. Defaults to false.</param>
    member __.BatchGetItemsProjectedAsync<'TProjection>(keys : seq<TableKey>, projection : Expr<'TRecord -> 'TProjection>,
                                                        ?consistentRead : bool) : Async<'TProjection[]> =
        __.BatchGetItemsProjectedAsync(keys, template.PrecomputeProjectionExpr projection, ?consistentRead = consistentRead)


    /// <summary>
    ///     Asynchronously deletes item of given key from table.
    /// </summary>
    /// <returns>The deleted item, or None if the item didn’t exist.</returns>
    /// <param name="key">Key of item to be deleted.</param>
    /// <param name="precondition">Specifies a precondition expression that existing item should satisfy.</param>
    member __.DeleteItemAsync(key : TableKey, ?precondition : ConditionExpression<'TRecord>) : Async<'TRecord option> = async {
        let kav = template.ToAttributeValues key
        let request = DeleteItemRequest(tableName, kav, ReturnValues = ReturnValue.ALL_OLD, ReturnConsumedCapacity = returnConsumedCapacity)
        match precondition with
        | Some pc ->
            let writer = AttributeWriter(request.ExpressionAttributeNames, request.ExpressionAttributeValues)
            request.ConditionExpression <- pc.Conditional.Write writer
        | None -> ()

        let! ct = Async.CancellationToken
        let! response = client.DeleteItemAsync(request, ct) |> Async.AwaitTaskCorrect
        maybeReport |> Option.iter (fun r -> r DeleteItem [ response.ConsumedCapacity ] 1)
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
    member __.DeleteItemAsync(key : TableKey, precondition : Expr<'TRecord -> bool>) : Async<'TRecord option> =
        __.DeleteItemAsync(key, template.PrecomputeConditionalExpr precondition)


    /// <summary>
    ///     Asynchronously performs batch delete operation on items of given keys.
    /// </summary>
    /// <returns>Any unprocessed keys due to throttling.</returns>
    /// <param name="keys">Keys of items to be deleted.</param>
    member __.BatchDeleteItemsAsync(keys : seq<TableKey>) = async {
        let mkDeleteRequest (key : TableKey) =
            let kav = template.ToAttributeValues(key)
            let pr = DeleteRequest(kav)
            WriteRequest(pr)

        let keys = Seq.toArray keys
        if keys.Length > 25 then invalidArg "items" "key length must be less than or equal to 25."
        let request = BatchWriteItemRequest(ReturnConsumedCapacity = returnConsumedCapacity)
        let deleteRequests = keys |> Seq.map mkDeleteRequest |> rlist
        request.RequestItems.[tableName] <- deleteRequests

        let! ct = Async.CancellationToken
        let! response = client.BatchWriteItemAsync(request, ct) |> Async.AwaitTaskCorrect
        let unprocessed =
            match response.UnprocessedItems.TryGetValue tableName with
            | (true, reqs) ->
                reqs |> Seq.choose (fun r -> r.DeleteRequest |> Option.ofObj)
                        |> Seq.map (fun d -> d.Key)
                        |> Seq.toArray
            | (false, _) -> [||]
        maybeReport |> Option.iter (fun r -> r BatchWriteItems (Seq.toList response.ConsumedCapacity) (keys.Length - unprocessed.Length))
        if response.HttpStatusCode <> HttpStatusCode.OK then
            failwithf "BatchWriteItem deletion request returned error %O" response.HttpStatusCode

        return unprocessed |> Array.map template.ExtractKey
    }


    /// <summary>
    ///     Asynchronously queries table with given condition expressions.
    /// </summary>
    /// <param name="keyCondition">Key condition expression.</param>
    /// <param name="filterCondition">Filter condition expression.</param>
    /// <param name="limit">Maximum number of items to evaluate.</param>
    /// <param name="consistentRead">Specify whether to perform consistent read operation.</param>
    /// <param name="scanIndexForward">Specifies the order in which to evaluate results. Either ascending (true) or descending (false).</param>
    member __.QueryAsync(keyCondition : ConditionExpression<'TRecord>, ?filterCondition : ConditionExpression<'TRecord>,
                            ?limit: int, ?consistentRead : bool, ?scanIndexForward : bool) : Async<'TRecord []> = async {

        let filterCondition = filterCondition |> Option.map (fun fc -> fc.Conditional)
        let! downloaded = queryAsync keyCondition.Conditional filterCondition None limit consistentRead scanIndexForward
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
    member __.QueryAsync(keyCondition : Expr<'TRecord -> bool>, ?filterCondition : Expr<'TRecord -> bool>,
                            ?limit : int, ?consistentRead : bool, ?scanIndexForward : bool) : Async<'TRecord []> = async {

        let kc = template.PrecomputeConditionalExpr keyCondition
        let fc = filterCondition |> Option.map template.PrecomputeConditionalExpr
        return! __.QueryAsync(kc, ?filterCondition = fc, ?limit = limit, ?consistentRead = consistentRead, ?scanIndexForward = scanIndexForward)
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
    member __.QueryProjectedAsync<'TProjection>(keyCondition : ConditionExpression<'TRecord>, projection : ProjectionExpression<'TRecord, 'TProjection>,
                                                ?filterCondition : ConditionExpression<'TRecord>,
                                                ?limit: int, ?consistentRead : bool, ?scanIndexForward : bool) : Async<'TProjection []> = async {

        let filterCondition = filterCondition |> Option.map (fun fc -> fc.Conditional)
        let! downloaded = queryAsync keyCondition.Conditional filterCondition None limit consistentRead scanIndexForward
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
    member __.QueryProjectedAsync<'TProjection>(keyCondition : Expr<'TRecord -> bool>, projection : Expr<'TRecord -> 'TProjection>,
                                                ?filterCondition : Expr<'TRecord -> bool>,
                                                ?limit: int, ?consistentRead : bool, ?scanIndexForward : bool) : Async<'TProjection []> = async {

        let filterCondition = filterCondition |> Option.map (fun fc -> template.PrecomputeConditionalExpr fc)
        return! __.QueryProjectedAsync(template.PrecomputeConditionalExpr keyCondition, template.PrecomputeProjectionExpr projection,
                                        ?filterCondition = filterCondition, ?limit = limit, ?consistentRead = consistentRead,
                                        ?scanIndexForward = scanIndexForward)
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
    member __.QueryPaginatedAsync(keyCondition : ConditionExpression<'TRecord>, ?filterCondition : ConditionExpression<'TRecord>,
                            ?limit: int, ?exclusiveStartKey: IndexKey, ?consistentRead : bool, ?scanIndexForward : bool) : Async<PaginatedResult<'TRecord, IndexKey>> = async {

        let filterCondition = filterCondition |> Option.map (fun fc -> fc.Conditional)
        let! (downloaded, lastEvaluatedKey) = queryPaginatedAsync keyCondition.Conditional filterCondition None (LimitType.DefaultOrCount limit) exclusiveStartKey consistentRead scanIndexForward
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
    member __.QueryPaginatedAsync(keyCondition : Expr<'TRecord -> bool>, ?filterCondition : Expr<'TRecord -> bool>,
                            ?limit : int, ?exclusiveStartKey: IndexKey, ?consistentRead : bool, ?scanIndexForward : bool) : Async<PaginatedResult<'TRecord, IndexKey>> = async {

        let kc = template.PrecomputeConditionalExpr keyCondition
        let fc = filterCondition |> Option.map template.PrecomputeConditionalExpr
        return! __.QueryPaginatedAsync(kc, ?filterCondition = fc, ?limit = limit, ?exclusiveStartKey = exclusiveStartKey, ?consistentRead = consistentRead, ?scanIndexForward = scanIndexForward)
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
    member __.QueryProjectedPaginatedAsync<'TProjection>(keyCondition : ConditionExpression<'TRecord>, projection : ProjectionExpression<'TRecord, 'TProjection>,
                                                ?filterCondition : ConditionExpression<'TRecord>,
                                                ?limit: int, ?exclusiveStartKey: IndexKey, ?consistentRead : bool, ?scanIndexForward : bool) : Async<PaginatedResult<'TProjection, IndexKey>> = async {

        let filterCondition = filterCondition |> Option.map (fun fc -> fc.Conditional)
        let! (downloaded, lastEvaluatedKey) = queryPaginatedAsync keyCondition.Conditional filterCondition None (LimitType.DefaultOrCount limit) exclusiveStartKey consistentRead scanIndexForward
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
    member __.QueryProjectedPaginatedAsync<'TProjection>(keyCondition : Expr<'TRecord -> bool>, projection : Expr<'TRecord -> 'TProjection>,
                                                ?filterCondition : Expr<'TRecord -> bool>,
                                                ?limit: int, ?exclusiveStartKey: IndexKey, ?consistentRead : bool, ?scanIndexForward : bool) : Async<PaginatedResult<'TProjection, IndexKey>> = async {

        let filterCondition = filterCondition |> Option.map (fun fc -> template.PrecomputeConditionalExpr fc)
        return! __.QueryProjectedPaginatedAsync(template.PrecomputeConditionalExpr keyCondition, template.PrecomputeProjectionExpr projection,
                                        ?filterCondition = filterCondition, ?limit = limit, ?exclusiveStartKey = exclusiveStartKey,
                                        ?consistentRead = consistentRead, ?scanIndexForward = scanIndexForward)
    }


    /// <summary>
    ///     Asynchronously scans table with given condition expressions.
    /// </summary>
    /// <param name="filterCondition">Filter condition expression.</param>
    /// <param name="limit">Maximum number of items to evaluate.</param>
    /// <param name="consistentRead">Specify whether to perform consistent read operation.</param>
    member __.ScanAsync(?filterCondition : ConditionExpression<'TRecord>, ?limit : int, ?consistentRead : bool) : Async<'TRecord []> = async {
        let filterCondition = filterCondition |> Option.map (fun fc -> fc.Conditional)
        let! downloaded = scanAsync filterCondition None limit consistentRead
        return downloaded |> Seq.map template.OfAttributeValues |> Seq.toArray
    }

    /// <summary>
    ///     Asynchronously scans table with given condition expressions.
    /// </summary>
    /// <param name="filterCondition">Filter condition expression.</param>
    /// <param name="limit">Maximum number of items to evaluate.</param>
    /// <param name="consistentRead">Specify whether to perform consistent read operation.</param>
    member __.ScanAsync(filterCondition : Expr<'TRecord -> bool>, ?limit : int, ?consistentRead : bool) : Async<'TRecord []> = async {
        let cond = template.PrecomputeConditionalExpr filterCondition
        return! __.ScanAsync(cond, ?limit = limit, ?consistentRead = consistentRead)
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
    member __.ScanProjectedAsync<'TProjection>(projection : ProjectionExpression<'TRecord, 'TProjection>,
                                                ?filterCondition : ConditionExpression<'TRecord>,
                                                ?limit : int, ?consistentRead : bool) : Async<'TProjection []> = async {
        let filterCondition = filterCondition |> Option.map (fun fc -> fc.Conditional)
        let! downloaded = scanAsync filterCondition (Some projection.ProjectionExpr) limit consistentRead
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
    member __.ScanProjectedAsync<'TProjection>(projection : Expr<'TRecord -> 'TProjection>,
                                                ?filterCondition : Expr<'TRecord -> bool>,
                                                ?limit : int, ?consistentRead : bool) : Async<'TProjection []> = async {
        let filterCondition = filterCondition |> Option.map (fun fc -> template.PrecomputeConditionalExpr fc)
        return! __.ScanProjectedAsync(template.PrecomputeProjectionExpr projection, ?filterCondition = filterCondition,
                                        ?limit = limit, ?consistentRead = consistentRead)
    }


    /// <summary>
    ///     Asynchronously scans table with given condition expressions.
    /// </summary>
    /// <param name="filterCondition">Filter condition expression.</param>
    /// <param name="limit">Maximum number of items per page - DynamoDB default is used if not specified.</param>
    /// <param name="exclusiveStartKey">LastEvaluatedKey from the previous page.</param>
    /// <param name="consistentRead">Specify whether to perform consistent read operation.</param>
    member __.ScanPaginatedAsync(?filterCondition : ConditionExpression<'TRecord>, ?limit : int, ?exclusiveStartKey : TableKey, ?consistentRead : bool) : Async<PaginatedResult<'TRecord, TableKey>> = async {
        let filterCondition = filterCondition |> Option.map (fun fc -> fc.Conditional)
        let! (downloaded, lastEvaluatedKey) = scanPaginatedAsync filterCondition None (LimitType.DefaultOrCount limit) exclusiveStartKey consistentRead
        return { Records = downloaded |> Seq.map template.OfAttributeValues |> Seq.toArray; LastEvaluatedKey = lastEvaluatedKey }
    }

    /// <summary>
    ///     Asynchronously scans table with given condition expressions.
    /// </summary>
    /// <param name="filterCondition">Filter condition expression.</param>
    /// <param name="limit">Maximum number of items per page - DynamoDB default is used if not specified.</param>
    /// <param name="exclusiveStartKey">LastEvaluatedKey from the previous page.</param>
    /// <param name="consistentRead">Specify whether to perform consistent read operation.</param>
    member __.ScanPaginatedAsync(filterCondition : Expr<'TRecord -> bool>, ?limit : int, ?exclusiveStartKey : TableKey, ?consistentRead : bool) : Async<PaginatedResult<'TRecord, TableKey>> = async {
        let cond = template.PrecomputeConditionalExpr filterCondition
        return! __.ScanPaginatedAsync(cond, ?limit = limit, ?exclusiveStartKey = exclusiveStartKey, ?consistentRead = consistentRead)
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
    member __.ScanProjectedPaginatedAsync<'TProjection>(projection : ProjectionExpression<'TRecord, 'TProjection>,
                                                ?filterCondition : ConditionExpression<'TRecord>,
                                                ?limit : int, ?exclusiveStartKey : TableKey, ?consistentRead : bool) : Async<PaginatedResult<'TProjection, TableKey>> = async {
        let filterCondition = filterCondition |> Option.map (fun fc -> fc.Conditional)
        let! (downloaded, lastEvaluatedKey) = scanPaginatedAsync filterCondition (Some projection.ProjectionExpr) (LimitType.DefaultOrCount limit) exclusiveStartKey consistentRead
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
    member __.ScanProjectedPaginatedAsync<'TProjection>(projection : Expr<'TRecord -> 'TProjection>,
                                                ?filterCondition : Expr<'TRecord -> bool>,
                                                ?limit : int, ?exclusiveStartKey : TableKey, ?consistentRead : bool) : Async<PaginatedResult<'TProjection, TableKey>> = async {
        let filterCondition = filterCondition |> Option.map (fun fc -> template.PrecomputeConditionalExpr fc)
        return! __.ScanProjectedPaginatedAsync(template.PrecomputeProjectionExpr projection, ?filterCondition = filterCondition,
                                        ?limit = limit, ?exclusiveStartKey = exclusiveStartKey, ?consistentRead = consistentRead)
    }


    /// <summary>
    ///     Asynchronously updates the underlying table with supplied configuration.<br />
    ///     Will throw if <c>customize</c> does not apply any alterations.
    /// </summary>
    /// <param name="customize">Callback to apply any options desired.</param>
    member _.UpdateTableAsync(customize) : Async<unit> = async {
        let request = UpdateTableRequest(TableName = tableName)
        customize request
        let! ct = Async.CancellationToken
        let! _response = client.UpdateTableAsync(request, ct) |> Async.AwaitTaskCorrect in ()
    }

    /// <summary>
    ///     Asynchronously updates the underlying table with supplied provisioned throughput.
    /// </summary>
    /// <param name="provisionedThroughput">Provisioned throughput to use on table.</param>
    [<System.Obsolete("Please replace with either 1. UpdateTableAsync or 2. ProvisionTableAsync")>]
    member t.UpdateProvisionedThroughputAsync(provisionedThroughput : ProvisionedThroughput) : Async<unit> =
        t.UpdateTableAsync(fun req -> Throughput.applyToUpdateRequest req (Throughput.Provisioned provisionedThroughput))

    member internal _.InternalDescribe() : Async<TableDescription> =
        let rec wait () = async {
            let! ct = Async.CancellationToken
            let! td = client.DescribeTableAsync(tableName, ct) |> Async.AwaitTaskCorrect
            if td.Table.TableStatus = TableStatus.ACTIVE then
                do! Async.Sleep 2000
                // wait indefinitely if table is in transition state
                return! wait ()
            else

            return td.Table
        }
        wait ()

    member internal __.InternalProvision(?makeCreateTableRequest) : Async<TableDescription> =
        let (|Conflict|_|) (e : exn) =
            match e with
            | :? AmazonDynamoDBException as e when e.StatusCode = HttpStatusCode.Conflict -> Some()
            | :? ResourceInUseException -> Some ()
            | _ -> None

        let rec checkOrCreate retries = async {
            match! __.InternalDescribe() |> Async.Catch with
            | Choice1Of2 desc ->
                let existingSchema = TableKeySchemata.OfTableDescription desc
                if existingSchema <> template.Info.Schemata then
                    sprintf "table '%s' exists with key schema %A, which is incompatible with record '%O'."
                        tableName existingSchema typeof<'TRecord>
                    |> invalidOp
                return desc

            | Choice2Of2 (:? ResourceNotFoundException) when Option.isSome makeCreateTableRequest ->
                let! ct = Async.CancellationToken
                let! response =
                    client.CreateTableAsync(makeCreateTableRequest.Value (), ct)
                    |> Async.AwaitTaskCorrect
                    |> Async.Catch

                match response with
                | Choice1Of2 _ -> return! checkOrCreate retries
                | Choice2Of2 Conflict when retries > 0 ->
                    do! Async.Sleep 2000
                    return! checkOrCreate (retries - 1)

                | Choice2Of2 e -> return! Async.Raise e

            | Choice2Of2 Conflict when retries > 0 ->
                do! Async.Sleep 2000
                return! checkOrCreate (retries - 1)

            | Choice2Of2 e -> return! Async.Raise e
        }

        checkOrCreate 9 // up to 9 retries, i.e. 10 attempts before we let exception propagate

    member internal _.InternalCreateCreateTableRequest(?throughput, ?streaming) =
        let req = template.Info.Schemata.CreateCreateTableRequest(tableName)
        throughput |> Option.iter (Throughput.applyToCreateRequest req)
        streaming |> Option.iter (Streaming.applyToCreateRequest req)
        req

    member internal t.InternalCreateOrValidateTableAsync(?throughput, ?streaming) =
        t.InternalProvision(fun () -> t.InternalCreateCreateTableRequest(?throughput = throughput, ?streaming = streaming))

    /// <summary>
    /// Asynchronously verify that the table exists and is compatible with record key schema, or throw.<br/>
    /// See also <c>InitializeTableAsync</c>, which performs the same check, but can create or re-provision the Table if required.
    /// </summary>
    member __.VerifyTableAsync() : Async<unit> =
        __.InternalProvision() |> Async.Ignore

    /// <summary>
    /// Asynchronously verifies that the table exists and is compatible with record key schema, throwing if it is incompatible.<br/>
    /// If the table is not present, it is provisioned, with the specified <c>throughput</c>.<br/>
    /// See also <c>VerifyTableAsync</c>, which only verifies the Table is present and correct.
    /// </summary>
    /// <param name="throughput">Throughput configuration to use for the table.</param>
    /// <param name="streaming">Optional Streaming configuration to use for the table. Default: Disabled.</param>
    member t.InitializeTableAsync(throughput : Throughput, ?streaming) : Async<unit> =
        t.InternalCreateOrValidateTableAsync(throughput, defaultArg streaming Streaming.Disabled) |> Async.Ignore

    /// <summary>
    /// Asynchronously verifies that the table exists and is compatible with record key schema, throwing if it is incompatible.<br/>
    /// If the table is not present, it is provisioned, with the specified <c>throughput</c> and optionally <c>streaming</c>.<br/>
    /// If it is present, and the <c>throughput</c> or <c>streaming</c> are not as specified, uses <c>UpdateTableAsync</c> to adjust.<br/>
    /// </summary>
    /// <param name="throughput">Throughput configuration to use for the table.</param>
    /// <param name="streaming">Optional streaming configuration to apply for the table. Default (if creating): Disabled. Default: (if existing) do not change.</param>
    member t.ProvisionTableAsync(throughput : Throughput, ?streaming) : Async<unit> = async {
        let! tableDescription = t.InternalCreateOrValidateTableAsync(throughput, defaultArg streaming Streaming.Disabled)
        let tc = throughput |> Throughput.hasChanged tableDescription
        let sc = streaming |> Option.exists (Streaming.hasChanged tableDescription)
        if tc || sc then
            let apply req =
                if tc then Throughput.applyToUpdateRequest req throughput
                if sc then streaming |> Option.iter (Streaming.applyToUpdateRequest req)
            do! t.UpdateTableAsync(apply) }

    /// <summary>
    ///     Asynchronously verify that the table exists and is compatible with record key schema.
    /// </summary>
    /// <param name="createIfNotExists">Create the table instance now instance if it does not exist. Defaults to false.</param>
    /// <param name="provisionedThroughput">Provisioned throughput for the table if newly created. Defaults to (10,10).</param>
    [<System.Obsolete("Please replace with either 1. VerifyTableAsync or 2. InitializeTableAsync")>]
    member t.VerifyTableAsync(?createIfNotExists : bool, ?provisionedThroughput : ProvisionedThroughput) : Async<unit> =
        if createIfNotExists = Some true then
            let throughput = match provisionedThroughput with Some p -> p | None -> ProvisionedThroughput(10L, 10L)
            t.InitializeTableAsync(Throughput.Provisioned throughput)
         else
            t.VerifyTableAsync()

// Deprecated factory method, to be removed. Replaced with
// 1. TableContext<'T> ctor (synchronous)
// 2. InitializeTableAsync OR VerifyTableAsync (explicitly async to signify that verification/creation is a costly and/or privileged operation)
type TableContext internal () =

    /// <summary>
    ///     Creates a DynamoDB client instance for given F# record and table name.
    /// </summary>
    /// <param name="client">DynamoDB client instance.</param>
    /// <param name="tableName">Table name to target.</param>
    /// <param name="verifyTable">Verify that the table exists and is compatible with supplied record schema. Defaults to true.</param>
    /// <param name="createIfNotExists">Create the table now if it does not exist. Defaults to false.</param>
    /// <param name="provisionedThroughput">Provisioned throughput for the table if newly created. Default: 10 RCU, 10 WCU</param>
    /// <param name="metricsCollector">Function to receive request metrics.</param>
    [<System.Obsolete(@"Creation with synchronous verification has been deprecated. Please use either
                        1. TableContext constructor (optionally followed by VerifyTableAsync or InitializeTableAsync) OR
                        2. (for scripting scenarios) Scripting.TableContext.Initialize")>]
    static member Create<'TRecord>
        (   client : IAmazonDynamoDB, tableName : string, ?verifyTable : bool,
            ?createIfNotExists : bool, ?provisionedThroughput : ProvisionedThroughput,
            ?metricsCollector : RequestMetrics -> unit) = async {
        let context = TableContext<'TRecord>(client, tableName, ?metricsCollector = metricsCollector)
        if createIfNotExists = Some true then
            let throughput = match provisionedThroughput with Some p -> p | None -> ProvisionedThroughput(10L, 10L)
            do! context.InitializeTableAsync(Throughput.Provisioned throughput)
        elif verifyTable <> Some false then
            do! context.VerifyTableAsync()
        return context }

/// <summary>
/// Sync-over-Async helpers that can be opted-into when working in scripting scenarios.
/// For normal usage, the <c>Async</c> versions of any given API is recommended, in order to ensure one
/// implements correct handling of the intrinsic asynchronous nature of the underlying interface
/// </summary>
module Scripting =

    /// Factory method that allows one to include auto-initialization easily for scripting scenarios
    type TableContext internal () =

        /// <summary>Creates a DynamoDB client instance for the specified F# record type, client and table name.</summary>
        /// <param name="client">DynamoDB client instance.</param>
        /// <param name="tableName">Table name to target.</param>
        /// <param name="throughput">Optional throughput to configure if the Table does not yet exist.</param>
        /// <param name="metricsCollector">Function to receive request metrics.</param>
        static member Initialize<'TRecord>
            (   client : IAmazonDynamoDB, tableName : string, ?throughput,
                ?metricsCollector : RequestMetrics -> unit) : TableContext<'TRecord> =
            let context = TableContext<'TRecord>(client, tableName, ?metricsCollector = metricsCollector)
            match throughput with
            | None -> context.VerifyTableAsync() |> Async.RunSynchronously
            | Some t -> context.InitializeTableAsync(t) |> Async.RunSynchronously
            context

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
        member t.UpdateProvisionedThroughput(provisionedThroughput : ProvisionedThroughput) =
            let spec = Throughput.Provisioned provisionedThroughput
            t.UpdateTableAsync(fun req -> Throughput.applyToUpdateRequest req spec) |> Async.RunSynchronously
