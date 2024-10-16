namespace FSharp.AWS.DynamoDB

open System.Collections.Generic
open System.Net

open Microsoft.FSharp.Quotations
open FSharp.Control

open Amazon.DynamoDBv2
open Amazon.DynamoDBv2.Model

open FSharp.AWS.DynamoDB.ExprCommon


/// Exception raised by DynamoDB in case where write preconditions are not satisfied
type ConditionalCheckFailedException = Amazon.DynamoDBv2.Model.ConditionalCheckFailedException

/// Exception raised by DynamoDB in case where resources are not found
type ResourceNotFoundException = Amazon.DynamoDBv2.Model.ResourceNotFoundException

/// Represents the provisioned throughput for a Table or Global Secondary Index
type ProvisionedThroughput = Amazon.DynamoDBv2.Model.ProvisionedThroughput

/// Represents the throughput configuration for a Table
[<RequireQualifiedAccess>]
type Throughput =
    | Provisioned of ProvisionedThroughput
    | OnDemand
module internal Throughput =
    let applyToCreateRequest (req: CreateTableRequest) =
        function
        | Throughput.Provisioned t ->
            req.BillingMode <- BillingMode.PROVISIONED
            req.ProvisionedThroughput <- t
            for gsi in req.GlobalSecondaryIndexes do
                gsi.ProvisionedThroughput <- t
        | Throughput.OnDemand -> req.BillingMode <- BillingMode.PAY_PER_REQUEST
    let requiresUpdate (desc: TableDescription) =
        function
        | Throughput.Provisioned t ->
            let current = desc.ProvisionedThroughput
            match desc.BillingModeSummary with
            | null -> false // can happen if initial create did not explicitly specify a BillingMode when creating
            | bms -> bms.BillingMode <> BillingMode.PROVISIONED
            || t.ReadCapacityUnits <> current.ReadCapacityUnits
            || t.WriteCapacityUnits <> current.WriteCapacityUnits
        | Throughput.OnDemand ->
            match desc.BillingModeSummary with
            | null -> true // CreateTable without setting BillingMode is equivalent to it being BullingMode.PROVISIONED
            | bms -> bms.BillingMode <> BillingMode.PAY_PER_REQUEST
    let applyToUpdateRequest (req: UpdateTableRequest) =
        function
        | Throughput.Provisioned t ->
            req.BillingMode <- BillingMode.PROVISIONED
            req.ProvisionedThroughput <- t
        | Throughput.OnDemand -> req.BillingMode <- BillingMode.PAY_PER_REQUEST

/// Represents the streaming configuration for a Table
[<RequireQualifiedAccess>]
type Streaming =
    | Enabled of StreamViewType
    | Disabled
module internal Streaming =
    let private (|Spec|) =
        function
        | Streaming.Enabled svt -> StreamSpecification(StreamEnabled = true, StreamViewType = svt)
        | Streaming.Disabled -> StreamSpecification(StreamEnabled = false)
    let applyToCreateRequest (req: CreateTableRequest) (Spec spec) = req.StreamSpecification <- spec
    let requiresUpdate (desc: TableDescription) =
        function
        | Streaming.Disabled ->
            match desc.StreamSpecification with
            | null -> false
            | s -> s.StreamEnabled
        | Streaming.Enabled svt ->
            match desc.StreamSpecification with
            | null -> true
            | s -> not s.StreamEnabled || s.StreamViewType <> svt
    let applyToUpdateRequest (req: UpdateTableRequest) (Spec spec) = req.StreamSpecification <- spec

module internal CreateTableRequest =

    let create (tableName, template: RecordTemplate<'TRecord>) throughput streaming customize =
        let req = CreateTableRequest(TableName = tableName)
        template.Info.Schemata.ApplyToCreateTableRequest req // NOTE needs to precede the throughput application as that walks the GSIs list
        throughput |> Option.iter (Throughput.applyToCreateRequest req) // NOTE needs to succeed Schemata.ApplyToCreateTableRequest
        streaming |> Option.iter (Streaming.applyToCreateRequest req)
        customize |> Option.iter (fun c -> c req)
        req

    let execute (client: IAmazonDynamoDB) request : Async<CreateTableResponse> = async {
        let! ct = Async.CancellationToken
        return! client.CreateTableAsync(request, ct) |> Async.AwaitTaskCorrect
    }

module internal UpdateTableRequest =

    let create tableName = UpdateTableRequest(TableName = tableName)

    let apply throughput streaming request =
        throughput |> Option.iter (Throughput.applyToUpdateRequest request)
        streaming |> Option.iter (Streaming.applyToUpdateRequest request)

    // Yields a request only if throughput, streaming or customize determine the update is warranted
    let createIfRequired tableName tableDescription throughput streaming customize : UpdateTableRequest option =
        let request = create tableName
        let tc = throughput |> Option.filter (Throughput.requiresUpdate tableDescription)
        let sc = streaming |> Option.filter (Streaming.requiresUpdate tableDescription)
        match tc, sc, customize with
        | None, None, None -> None
        | Some _ as tc, sc, None
        | tc, (Some _ as sc), None ->
            apply tc sc request
            Some request
        | tc, sc, Some customize ->
            apply tc sc request
            if customize request then Some request else None

    let execute (client: IAmazonDynamoDB) request : Async<TableDescription> = async {
        let! ct = Async.CancellationToken
        let! response = client.UpdateTableAsync(request, ct) |> Async.AwaitTaskCorrect
        return response.TableDescription
    }

module internal Provisioning =

    let tryDescribe (client: IAmazonDynamoDB, tableName: string) : Async<TableDescription option> = async {
        let! ct = Async.CancellationToken
        let! td = client.DescribeTableAsync(tableName, ct) |> Async.AwaitTaskCorrect
        return
            match td.Table with
            | t when t.TableStatus = TableStatus.ACTIVE -> Some t
            | _ -> None
    }

    let private waitForActive (client: IAmazonDynamoDB, tableName: string) : Async<TableDescription> =
        let rec wait () = async {
            match! tryDescribe (client, tableName) with
            | Some t -> return t
            | None ->
                do! Async.Sleep 1000
                // wait indefinitely if table is in transition state
                return! wait ()
        }
        wait ()

    let (|Conflict|_|) (e: exn) =
        match e with
        | :? AmazonDynamoDBException as e when e.StatusCode = HttpStatusCode.Conflict -> Some()
        | :? ResourceInUseException -> Some()
        | _ -> None

    let private checkOrCreate (client, tableName) validateDescription maybeMakeCreateTableRequest : Async<TableDescription> =
        let rec aux retries = async {
            match! waitForActive (client, tableName) |> Async.Catch with
            | Choice1Of2 desc ->
                validateDescription desc
                return desc

            | Choice2Of2(:? ResourceNotFoundException) when Option.isSome maybeMakeCreateTableRequest ->
                let req = maybeMakeCreateTableRequest.Value()
                match! CreateTableRequest.execute client req |> Async.Catch with
                | Choice1Of2 _ -> return! aux retries
                | Choice2Of2 Conflict when retries > 0 ->
                    do! Async.Sleep 2000
                    return! aux (retries - 1)

                | Choice2Of2 e -> return! Async.Raise e

            | Choice2Of2 Conflict when retries > 0 ->
                do! Async.Sleep 2000
                return! aux (retries - 1)

            | Choice2Of2 e -> return! Async.Raise e
        }
        aux 9 // up to 9 retries, i.e. 10 attempts before we let exception propagate

    let private validateDescription (tableName, template: RecordTemplate<'TRecord>) desc =
        let existingSchema = TableKeySchemata.OfTableDescription desc
        if existingSchema <> template.Info.Schemata then
            sprintf
                "table '%s' exists with key schema %A, which is incompatible with record '%O'."
                tableName
                existingSchema
                typeof<'TRecord>
            |> invalidOp

    let private run (client, tableName, template) maybeMakeCreateRequest : Async<TableDescription> =
        let validate = validateDescription (tableName, template)
        checkOrCreate (client, tableName) validate maybeMakeCreateRequest

    let verifyOrCreate (client, tableName, template) throughput streaming customize : Async<TableDescription> =
        let generateCreateRequest () = CreateTableRequest.create (tableName, template) throughput streaming customize
        run (client, tableName, template) (Some generateCreateRequest)

    let validateOnly (client, tableName, template) : Async<unit> = run (client, tableName, template) None |> Async.Ignore

/// Represents the operation performed on the table, for metrics collection purposes
type Operation =
    | GetItem
    | PutItem
    | UpdateItem
    | DeleteItem
    | BatchGetItems
    | BatchWriteItems
    | TransactWriteItems
    | Scan
    | Query

/// Represents metrics returned by the table operation, for plugging in to an observability framework
type RequestMetrics =
    { TableName: string
      Operation: Operation
      ConsumedCapacity: ConsumedCapacity list
      ItemCount: int }

/// Scan/query limit type (internal only)
type private LimitType =
    | All
    | Default
    | Count of int

    member x.GetCount() =
        match x with
        | Count l -> Some l
        | _ -> None
    member x.IsDownloadIncomplete(count: int) =
        match x with
        | Count l -> count < l
        | All -> true
        | Default -> false
    static member AllOrCount(l: int option) = l |> Option.map Count |> Option.defaultValue All
    static member DefaultOrCount(l: int option) = l |> Option.map Count |> Option.defaultValue Default

/// Helpers for identifying Failed Precondition check outcomes emanating from <c>PutItem</c>, <c>UpdateItem</c> or <c>DeleteItem</c>
module Precondition =
    /// <summary>Exception filter to identify whether an individual (non-transactional) <c>PutItem</c>, <c>UpdateItem</c> or <c>DeleteItem</c> call's <c>precondition</c> check failing.</summary>
    let (|CheckFailed|_|): exn -> unit option =
        function
        | :? ConditionalCheckFailedException -> Some()
        | _ -> None


/// DynamoDB client object for performing table operations in the context of given F# record representations
[<Sealed; AutoSerializable(false)>]
type TableContext<'TRecord>
    internal
    (
        client: IAmazonDynamoDB,
        tableName: string,
        template: RecordTemplate<'TRecord>,
        metricsCollector: (RequestMetrics -> unit) option
    ) =

    let reportMetrics collector (operation: Operation) (consumedCapacity: ConsumedCapacity list) (itemCount: int) =
        collector
            { TableName = tableName
              Operation = operation
              ConsumedCapacity = consumedCapacity
              ItemCount = itemCount }
    let returnConsumedCapacity, maybeReport =
        match metricsCollector with
        | Some sink -> ReturnConsumedCapacity.INDEXES, Some(reportMetrics sink)
        | None -> ReturnConsumedCapacity.NONE, None

    let tryGetItemAsync (key: TableKey) (consistentRead: bool option) (proj: ProjectionExpr.ProjectionExpr option) = async {
        let kav = template.ToAttributeValues(key)
        let request = GetItemRequest(tableName, kav, ReturnConsumedCapacity = returnConsumedCapacity)
        match proj with
        | None -> ()
        | Some proj ->
            let aw = AttributeWriter(request.ExpressionAttributeNames, null)
            request.ProjectionExpression <- proj.Write aw

        match consistentRead with
        | None -> ()
        | Some c -> request.ConsistentRead <- c

        let! ct = Async.CancellationToken
        let! response = client.GetItemAsync(request, ct) |> Async.AwaitTaskCorrect
        maybeReport
        |> Option.iter (fun r -> r GetItem [ response.ConsumedCapacity ] (if response.IsItemSet then 1 else 0))
        if response.HttpStatusCode <> HttpStatusCode.OK then
            failwithf "GetItem request returned error %O" response.HttpStatusCode

        if response.IsItemSet then
            return Some response.Item
        else
            return None
    }

    let getItemAsync (key: TableKey) (consistentRead: bool option) (proj: ProjectionExpr.ProjectionExpr option) = async {
        match! tryGetItemAsync key consistentRead proj with
        | Some item -> return item
        | None -> return raise <| ResourceNotFoundException(sprintf "could not find item %O" key)
    }

    let batchGetItemsAsync (keys: seq<TableKey>) (consistentRead: bool option) (projExpr: ProjectionExpr.ProjectionExpr option) = async {

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
        request.RequestItems[tableName] <- kna

        let! ct = Async.CancellationToken
        let! response = client.BatchGetItemAsync(request, ct) |> Async.AwaitTaskCorrect
        maybeReport
        |> Option.iter (fun r -> r BatchGetItems (List.ofSeq response.ConsumedCapacity) response.Responses[tableName].Count)
        if response.HttpStatusCode <> HttpStatusCode.OK then
            failwithf "BatchGetItem request returned error %O" response.HttpStatusCode

        return response.Responses[tableName]
    }

    let queryPaginatedAsync
        (keyCondition: ConditionalExpr.ConditionalExpression)
        (filterCondition: ConditionalExpr.ConditionalExpression option)
        (projectionExpr: ProjectionExpr.ProjectionExpr option)
        (limit: LimitType)
        (exclusiveStartKey: IndexKey option)
        (consistentRead: bool option)
        (scanIndexForward: bool option)
        =
        async {

            if not keyCondition.IsKeyConditionCompatible then
                invalidArg
                    "keyCondition"
                    """key conditions must satisfy the following constraints:
* Must only reference HashKey & RangeKey attributes.
* Must reference HashKey attribute exactly once.
* Must reference RangeKey attribute at most once.
* HashKey comparison must be equality comparison only.
* Must not contain OR and NOT clauses.
* Must not contain nested operands.
"""

            let downloaded = ResizeArray<_>()
            let consumedCapacity = ResizeArray<ConsumedCapacity>()
            let emitMetrics () = maybeReport |> Option.iter (fun r -> r Query (Seq.toList consumedCapacity) downloaded.Count)
            let mutable lastEvaluatedKey: Dictionary<string, AttributeValue> option = None

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
                consumedCapacity.Add response.ConsumedCapacity
                if response.HttpStatusCode <> HttpStatusCode.OK then
                    emitMetrics ()
                    failwithf "Query request returned error %O" response.HttpStatusCode

                downloaded.AddRange response.Items
                if response.LastEvaluatedKey.Count > 0 then
                    lastEvaluatedKey <- Some response.LastEvaluatedKey
                    if limit.IsDownloadIncomplete downloaded.Count then
                        do! aux lastEvaluatedKey
                else
                    lastEvaluatedKey <- None
            }

            do!
                aux (
                    exclusiveStartKey
                    |> Option.map (fun k -> template.ToAttributeValues(k, keyCondition.KeyCondition.Value))
                )

            emitMetrics ()

            return
                (downloaded,
                 lastEvaluatedKey
                 |> Option.map (fun av -> template.ExtractIndexKey(keyCondition.KeyCondition.Value, av)))
        }

    let queryAsync
        (keyCondition: ConditionalExpr.ConditionalExpression)
        (filterCondition: ConditionalExpr.ConditionalExpression option)
        (projectionExpr: ProjectionExpr.ProjectionExpr option)
        (limit: int option)
        (consistentRead: bool option)
        (scanIndexForward: bool option)
        =
        async {

            let! downloaded, _ =
                queryPaginatedAsync
                    keyCondition
                    filterCondition
                    projectionExpr
                    (LimitType.AllOrCount limit)
                    None
                    consistentRead
                    scanIndexForward

            return downloaded
        }

    let scanPaginatedAsync
        (filterCondition: ConditionalExpr.ConditionalExpression option)
        (projectionExpr: ProjectionExpr.ProjectionExpr option)
        (limit: LimitType)
        (exclusiveStartKey: TableKey option)
        (consistentRead: bool option)
        =
        async {

            let downloaded = ResizeArray<_>()
            let consumedCapacity = ResizeArray<ConsumedCapacity>()
            let emitMetrics () = maybeReport |> Option.iter (fun r -> r Scan (Seq.toList consumedCapacity) downloaded.Count)
            let mutable lastEvaluatedKey: Dictionary<string, AttributeValue> option = None
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

    let scanAsync
        (filterCondition: ConditionalExpr.ConditionalExpression option)
        (projectionExpr: ProjectionExpr.ProjectionExpr option)
        (limit: int option)
        (consistentRead: bool option)
        =
        async {

            let! downloaded, _ = scanPaginatedAsync filterCondition projectionExpr (LimitType.AllOrCount limit) None consistentRead

            return downloaded
        }

 
    /// DynamoDB client instance used for the table operations
    member _.Client = client
    /// DynamoDB table name targeted by the context
    member _.TableName = tableName
    /// Primary Key schema used by the current record/table
    member _.PrimaryKey = template.PrimaryKey
    /// Global Secondary indices specified by the table
    member _.GlobalSecondaryIndices = template.GlobalSecondaryIndices
    /// Local Secondary indices specified by the table
    member _.LocalSecondaryIndices = template.LocalSecondaryIndices
    /// Record-induced table template
    member _.Template = template

    /// <summary>
    ///     Creates a DynamoDB client instance for given F# record and table name.<br/>
    ///     For creating, provisioning or verification, see <c>VerifyOrCreateTableAsync</c> and <c>VerifyTableAsync</c>.
    /// </summary>
    /// <param name="client">DynamoDB client instance.</param>
    /// <param name="tableName">Table name to target.</param>
    /// <param name="metricsCollector">Function to receive request metrics.</param>
    new(client: IAmazonDynamoDB, tableName: string, ?metricsCollector: RequestMetrics -> unit) =
        if not <| isValidTableName tableName then
            invalidArg "tableName" "unsupported DynamoDB table name."
        TableContext<'TRecord>(client, tableName, RecordTemplate.Define<'TRecord>(), metricsCollector)


    /// Creates a new table context instance that uses
    /// a new F# record type. The new F# record type
    /// must define a compatible key schema.
    member _.WithRecordType<'TRecord2>() : TableContext<'TRecord2> =
        let rd = RecordTemplate.Define<'TRecord2>()
        if template.PrimaryKey <> rd.PrimaryKey then
            invalidArg (string typeof<'TRecord2>) "incompatible key schema."

        new TableContext<'TRecord2>(client, tableName, rd, metricsCollector)

    /// Creates an identical table context with the specified metricsCollector callback replacing any previously specified one
    member _.WithMetricsCollector(collector: RequestMetrics -> unit) : TableContext<'TRecord> =
        new TableContext<'TRecord>(client, tableName, template, Some collector)

    /// <summary>Asynchronously puts a record item in the table.</summary>
    /// <param name="item">Item to be written.</param>
    /// <param name="precondition">Precondition to satisfy where item already exists. Use <c>Precondition.CheckFailed</c> to identify Precondition Check failures.</param>
    member _.PutItemAsync(item: 'TRecord, ?precondition: ConditionExpression<'TRecord>) : Async<TableKey> = async {
        let attrValues = template.ToAttributeValues(item)
        let request =
            PutItemRequest(tableName, attrValues, ReturnValues = ReturnValue.NONE, ReturnConsumedCapacity = returnConsumedCapacity)
        match precondition with
        | Some pc ->
            let writer = AttributeWriter(request.ExpressionAttributeNames, request.ExpressionAttributeValues)
            request.ConditionExpression <- pc.Conditional.Write writer
            request.ReturnValuesOnConditionCheckFailure <- ReturnValuesOnConditionCheckFailure.ALL_OLD
        | _ -> ()

        let! ct = Async.CancellationToken
        let! response = client.PutItemAsync(request, ct) |> Async.AwaitTaskCorrect
        maybeReport |> Option.iter (fun r -> r PutItem [ response.ConsumedCapacity ] 1)
        if response.HttpStatusCode <> HttpStatusCode.OK then
            failwithf "PutItem request returned error %O" response.HttpStatusCode

        return template.ExtractKey item
    }

    /// <summary>Asynchronously puts a record item in the table.</summary>
    /// <param name="item">Item to be written.</param>
    /// <param name="precondition">Precondition to satisfy where item already exists. Use <c>Precondition.CheckFailed</c> to identify Precondition Check failures.</param>
    member t.PutItemAsync(item: 'TRecord, precondition: Expr<'TRecord -> bool>) =
        t.PutItemAsync(item, template.PrecomputeConditionalExpr precondition)

    /// <summary>
    ///     Asynchronously puts a collection of items to the table as a batch write operation.
    ///     At most 25 items can be written in a single batch write operation.
    /// </summary>
    /// <returns>Any unprocessed items due to throttling.</returns>
    /// <param name="items">Items to be written.</param>
    member _.BatchPutItemsAsync(items: seq<'TRecord>) : Async<'TRecord[]> = async {
        let mkWriteRequest (item: 'TRecord) =
            let attrValues = template.ToAttributeValues(item)
            let pr = PutRequest(attrValues)
            WriteRequest(pr)

        let items = Seq.toArray items
        if items.Length > 25 then
            invalidArg "items" "item length must be less than or equal to 25."
        let writeRequests = items |> Seq.map mkWriteRequest |> rlist
        let pbr = BatchWriteItemRequest(ReturnConsumedCapacity = returnConsumedCapacity)
        pbr.RequestItems[tableName] <- writeRequests
        let! ct = Async.CancellationToken
        let! response = client.BatchWriteItemAsync(pbr, ct) |> Async.AwaitTaskCorrect
        let unprocessed =
            match response.UnprocessedItems.TryGetValue tableName with
            | true, reqs ->
                reqs
                |> Seq.choose (fun r -> r.PutRequest |> Option.ofObj)
                |> Seq.map (fun w -> w.Item)
                |> Seq.toArray
            | false, _ -> [||]
        maybeReport
        |> Option.iter (fun r -> r BatchWriteItems (Seq.toList response.ConsumedCapacity) (items.Length - unprocessed.Length))
        if response.HttpStatusCode <> HttpStatusCode.OK then
            failwithf "BatchWriteItem put request returned error %O" response.HttpStatusCode

        return unprocessed |> Array.map template.OfAttributeValues
    }


    /// <summary>Asynchronously updates item with supplied key using provided update expression.</summary>
    /// <param name="key">Key of item to be updated.</param>
    /// <param name="updater">Table update expression.</param>
    /// <param name="precondition">Specifies a precondition expression that any existing item should satisfy. Use <c>Precondition.CheckFailed</c> to identify Precondition Check failures.</param>
    /// <param name="returnLatest">Specifies the operation should return the latest (true) or older (false) version of the item. Defaults to latest.</param>
    member _.UpdateItemAsync
        (
            key: TableKey,
            updater: UpdateExpression<'TRecord>,
            ?precondition: ConditionExpression<'TRecord>,
            ?returnLatest: bool
        ) : Async<'TRecord> =
        async {

            let kav = template.ToAttributeValues(key)
            let request = UpdateItemRequest(Key = kav, TableName = tableName, ReturnConsumedCapacity = returnConsumedCapacity)
            request.ReturnValues <-
                if defaultArg returnLatest true then
                    ReturnValue.ALL_NEW
                else
                    ReturnValue.ALL_OLD

            let writer = AttributeWriter(request.ExpressionAttributeNames, request.ExpressionAttributeValues)
            request.UpdateExpression <- updater.UpdateOps.Write(writer)

            match precondition with
            | Some pc ->
                request.ConditionExpression <- pc.Conditional.Write writer
                request.ReturnValuesOnConditionCheckFailure <- ReturnValuesOnConditionCheckFailure.ALL_OLD
            | _ -> ()

            let! ct = Async.CancellationToken
            let! response = client.UpdateItemAsync(request, ct) |> Async.AwaitTaskCorrect
            maybeReport |> Option.iter (fun r -> r UpdateItem [ response.ConsumedCapacity ] 1)
            if response.HttpStatusCode <> HttpStatusCode.OK then
                failwithf "UpdateItem request returned error %O" response.HttpStatusCode

            return template.OfAttributeValues response.Attributes
        }

    /// <summary>Asynchronously updates item with supplied key using provided record update expression.</summary>
    /// <param name="key">Key of item to be updated.</param>
    /// <param name="updateExpr">Table update expression.</param>
    /// <param name="precondition">Specifies a precondition expression that any existing item should satisfy. Use <c>Precondition.CheckFailed</c> to identify Precondition Check failures.</param>
    /// <param name="returnLatest">Specifies the operation should return the latest (true) or older (false) version of the item. Defaults to latest.</param>
    member t.UpdateItemAsync
        (
            key: TableKey,
            updateExpr: Expr<'TRecord -> 'TRecord>,
            ?precondition: Expr<'TRecord -> bool>,
            ?returnLatest: bool
        ) =
        let updater = template.PrecomputeUpdateExpr updateExpr
        let precondition = precondition |> Option.map template.PrecomputeConditionalExpr
        t.UpdateItemAsync(key, updater, ?returnLatest = returnLatest, ?precondition = precondition)

    /// <summary>Asynchronously updates item with supplied key using provided update operation expression.</summary>
    /// <param name="key">Key of item to be updated.</param>
    /// <param name="updateExpr">Table update expression.</param>
    /// <param name="precondition">Specifies a precondition expression that any existing item should satisfy. Use <c>Precondition.CheckFailed</c> to identify Precondition Check failures.</param>
    /// <param name="returnLatest">Specifies the operation should return the latest (true) or older (false) version of the item. Defaults to latest.</param>
    member t.UpdateItemAsync
        (
            key: TableKey,
            updateExpr: Expr<'TRecord -> UpdateOp>,
            ?precondition: Expr<'TRecord -> bool>,
            ?returnLatest: bool
        ) =
        let updater = template.PrecomputeUpdateExpr updateExpr
        let precondition = precondition |> Option.map template.PrecomputeConditionalExpr
        t.UpdateItemAsync(key, updater, ?returnLatest = returnLatest, ?precondition = precondition)


    /// <summary>
    ///     Asynchronously attempts to fetch item with given key from table. Returns None if no item with that key is present.
    /// </summary>
    /// <param name="key">Key of item to be fetched.</param>
    /// <param name="consistentRead">Specify whether to perform consistent read operation.</param>
    member _.TryGetItemAsync(key: TableKey, ?consistentRead: bool) : Async<'TRecord option> = async {
        let! response = tryGetItemAsync key consistentRead None
        return response |> Option.map template.OfAttributeValues
    }


    /// <summary>
    ///     Asynchronously checks whether item of supplied key exists in table.
    /// </summary>
    /// <param name="key">Key to be checked.</param>
    member _.ContainsKeyAsync(key: TableKey) : Async<bool> = async {
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
    /// <param name="consistentRead">Specify whether to perform consistent read operation.</param>
    member _.GetItemAsync(key: TableKey, ?consistentRead: bool) : Async<'TRecord> = async {
        let! item = getItemAsync key consistentRead None
        return template.OfAttributeValues item
    }


    /// <summary>
    ///     Asynchronously fetches item of given key from table.
    ///     Uses supplied projection expression to narrow downloaded attributes.
    ///     Projection type must be a tuple of zero or more non-conflicting properties.
    /// </summary>
    /// <param name="key">Key of item to be fetched.</param>
    /// <param name="projection">Projection expression to be applied to item.</param>
    /// <param name="consistentRead">Specify whether to perform consistent read operation.</param>
    member _.GetItemProjectedAsync
        (
            key: TableKey,
            projection: ProjectionExpression<'TRecord, 'TProjection>,
            ?consistentRead: bool
        ) : Async<'TProjection> =
        async {
            let! item = getItemAsync key consistentRead (Some projection.ProjectionExpr)
            return projection.UnPickle item
        }

    /// <summary>
    ///     Asynchronously fetches item of given key from table.
    ///     Uses supplied projection expression to narrow downloaded attributes.
    ///     Projection type must be a tuple of zero or more non-conflicting properties.
    /// </summary>
    /// <param name="key">Key of item to be fetched.</param>
    /// <param name="projection">Projection expression to be applied to item.</param>
    /// <param name="consistentRead">Specify whether to perform consistent read operation.</param>
    member t.GetItemProjectedAsync(key: TableKey, projection: Expr<'TRecord -> 'TProjection>, ?consistentRead: bool) : Async<'TProjection> =
        t.GetItemProjectedAsync(key, (template.PrecomputeProjectionExpr projection), ?consistentRead = consistentRead)

    /// <summary>
    ///     Asynchronously performs a batch fetch of items with supplied keys.
    /// </summary>
    /// <param name="keys">Keys of items to be fetched.</param>
    /// <param name="consistentRead">Perform consistent read. Defaults to false.</param>
    member _.BatchGetItemsAsync(keys: seq<TableKey>, ?consistentRead: bool) : Async<'TRecord[]> = async {
        let! response = batchGetItemsAsync keys consistentRead None
        return response |> Seq.map template.OfAttributeValues |> Seq.toArray
    }


    /// <summary>
    ///     Asynchronously performs a batch fetch of items with supplied keys.
    /// </summary>
    /// <param name="keys">Keys of items to be fetched.</param>
    /// <param name="projection">Projection expression to be applied to item.</param>
    /// <param name="consistentRead">Perform consistent read. Defaults to false.</param>
    member _.BatchGetItemsProjectedAsync<'TProjection>
        (
            keys: seq<TableKey>,
            projection: ProjectionExpression<'TRecord, 'TProjection>,
            ?consistentRead: bool
        ) : Async<'TProjection[]> =
        async {

            let! response = batchGetItemsAsync keys consistentRead (Some projection.ProjectionExpr)
            return response |> Seq.map projection.UnPickle |> Seq.toArray
        }

    /// <summary>
    ///     Asynchronously performs a batch fetch of items with supplied keys.
    /// </summary>
    /// <param name="keys">Keys of items to be fetched.</param>
    /// <param name="projection">Projection expression to be applied to item.</param>
    /// <param name="consistentRead">Perform consistent read. Defaults to false.</param>
    member t.BatchGetItemsProjectedAsync<'TProjection>
        (
            keys: seq<TableKey>,
            projection: Expr<'TRecord -> 'TProjection>,
            ?consistentRead: bool
        ) : Async<'TProjection[]> =
        t.BatchGetItemsProjectedAsync(keys, template.PrecomputeProjectionExpr projection, ?consistentRead = consistentRead)


    /// <summary>Asynchronously deletes item of given key from table.</summary>
    /// <returns>The deleted item, or None if the item didn’t exist.</returns>
    /// <param name="key">Key of item to be deleted.</param>
    /// <param name="precondition">Specifies a precondition expression that existing item should satisfy. Use <c>Precondition.CheckFailed</c> to identify Precondition Check failures.</param>
    member _.DeleteItemAsync(key: TableKey, ?precondition: ConditionExpression<'TRecord>) : Async<'TRecord option> = async {
        let kav = template.ToAttributeValues key
        let request = DeleteItemRequest(tableName, kav, ReturnValues = ReturnValue.ALL_OLD, ReturnConsumedCapacity = returnConsumedCapacity)
        match precondition with
        | Some pc ->
            let writer = AttributeWriter(request.ExpressionAttributeNames, request.ExpressionAttributeValues)
            request.ConditionExpression <- pc.Conditional.Write writer
            request.ReturnValuesOnConditionCheckFailure <- ReturnValuesOnConditionCheckFailure.ALL_OLD
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

    /// <summary>Asynchronously deletes item of given key from table.</summary>
    /// <returns>The deleted item, or None if the item didn’t exist.</returns>
    /// <param name="key">Key of item to be deleted.</param>
    /// <param name="precondition">Specifies a precondition expression that existing item should satisfy. Use <c>Precondition.CheckFailed</c> to identify Precondition Check failures.</param>
    member t.DeleteItemAsync(key: TableKey, precondition: Expr<'TRecord -> bool>) : Async<'TRecord option> =
        t.DeleteItemAsync(key, template.PrecomputeConditionalExpr precondition)


    /// <summary>
    ///     Asynchronously performs batch delete operation on items of given keys.
    /// </summary>
    /// <returns>Any unprocessed keys due to throttling.</returns>
    /// <param name="keys">Keys of items to be deleted.</param>
    member _.BatchDeleteItemsAsync(keys: seq<TableKey>) = async {
        let mkDeleteRequest (key: TableKey) =
            let kav = template.ToAttributeValues(key)
            let pr = DeleteRequest(kav)
            WriteRequest(pr)

        let keys = Seq.toArray keys
        if keys.Length > 25 then
            invalidArg "items" "key length must be less than or equal to 25."
        let request = BatchWriteItemRequest(ReturnConsumedCapacity = returnConsumedCapacity)
        let deleteRequests = keys |> Seq.map mkDeleteRequest |> rlist
        request.RequestItems[tableName] <- deleteRequests

        let! ct = Async.CancellationToken
        let! response = client.BatchWriteItemAsync(request, ct) |> Async.AwaitTaskCorrect
        let unprocessed =
            match response.UnprocessedItems.TryGetValue tableName with
            | true, reqs ->
                reqs
                |> Seq.choose (fun r -> r.DeleteRequest |> Option.ofObj)
                |> Seq.map (fun d -> d.Key)
                |> Seq.toArray
            | false, _ -> [||]
        maybeReport
        |> Option.iter (fun r -> r BatchWriteItems (Seq.toList response.ConsumedCapacity) (keys.Length - unprocessed.Length))
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
    member _.QueryAsync
        (
            keyCondition: ConditionExpression<'TRecord>,
            ?filterCondition: ConditionExpression<'TRecord>,
            ?limit: int,
            ?consistentRead: bool,
            ?scanIndexForward: bool
        ) : Async<'TRecord[]> =
        async {

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
    member t.QueryAsync
        (
            keyCondition: Expr<'TRecord -> bool>,
            ?filterCondition: Expr<'TRecord -> bool>,
            ?limit: int,
            ?consistentRead: bool,
            ?scanIndexForward: bool
        ) : Async<'TRecord[]> =

        let kc = template.PrecomputeConditionalExpr keyCondition
        let fc = filterCondition |> Option.map template.PrecomputeConditionalExpr
        t.QueryAsync(kc, ?filterCondition = fc, ?limit = limit, ?consistentRead = consistentRead, ?scanIndexForward = scanIndexForward)


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
    member _.QueryProjectedAsync<'TProjection>
        (
            keyCondition: ConditionExpression<'TRecord>,
            projection: ProjectionExpression<'TRecord, 'TProjection>,
            ?filterCondition: ConditionExpression<'TRecord>,
            ?limit: int,
            ?consistentRead: bool,
            ?scanIndexForward: bool
        ) : Async<'TProjection[]> =
        async {

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
    member t.QueryProjectedAsync<'TProjection>
        (
            keyCondition: Expr<'TRecord -> bool>,
            projection: Expr<'TRecord -> 'TProjection>,
            ?filterCondition: Expr<'TRecord -> bool>,
            ?limit: int,
            ?consistentRead: bool,
            ?scanIndexForward: bool
        ) : Async<'TProjection[]> =

        let filterCondition = filterCondition |> Option.map template.PrecomputeConditionalExpr
        t.QueryProjectedAsync(
            template.PrecomputeConditionalExpr keyCondition,
            template.PrecomputeProjectionExpr projection,
            ?filterCondition = filterCondition,
            ?limit = limit,
            ?consistentRead = consistentRead,
            ?scanIndexForward = scanIndexForward
        )


    /// <summary>
    ///     Asynchronously queries table with given condition expressions.
    /// </summary>
    /// <param name="keyCondition">Key condition expression.</param>
    /// <param name="filterCondition">Filter condition expression.</param>
    /// <param name="limit">Maximum number of items per page - DynamoDB default is used if not specified.</param>
    /// <param name="exclusiveStartKey">LastEvaluatedKey from the previous page.</param>
    /// <param name="consistentRead">Specify whether to perform consistent read operation.</param>
    /// <param name="scanIndexForward">Specifies the order in which to evaluate results. Either ascending (true) or descending (false).</param>
    member _.QueryPaginatedAsync
        (
            keyCondition: ConditionExpression<'TRecord>,
            ?filterCondition: ConditionExpression<'TRecord>,
            ?limit: int,
            ?exclusiveStartKey: IndexKey,
            ?consistentRead: bool,
            ?scanIndexForward: bool
        ) : Async<PaginatedResult<'TRecord, IndexKey>> =
        async {

            let filterCondition = filterCondition |> Option.map (fun fc -> fc.Conditional)
            let! downloaded, lastEvaluatedKey =
                queryPaginatedAsync
                    keyCondition.Conditional
                    filterCondition
                    None
                    (LimitType.DefaultOrCount limit)
                    exclusiveStartKey
                    consistentRead
                    scanIndexForward
            return
                { Records = downloaded |> Seq.map template.OfAttributeValues |> Seq.toArray
                  LastEvaluatedKey = lastEvaluatedKey }
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
    member t.QueryPaginatedAsync
        (
            keyCondition: Expr<'TRecord -> bool>,
            ?filterCondition: Expr<'TRecord -> bool>,
            ?limit: int,
            ?exclusiveStartKey: IndexKey,
            ?consistentRead: bool,
            ?scanIndexForward: bool
        ) : Async<PaginatedResult<'TRecord, IndexKey>> =
        let kc = template.PrecomputeConditionalExpr keyCondition
        let fc = filterCondition |> Option.map template.PrecomputeConditionalExpr
        t.QueryPaginatedAsync(
            kc,
            ?filterCondition = fc,
            ?limit = limit,
            ?exclusiveStartKey = exclusiveStartKey,
            ?consistentRead = consistentRead,
            ?scanIndexForward = scanIndexForward
        )


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
    member _.QueryProjectedPaginatedAsync<'TProjection>
        (
            keyCondition: ConditionExpression<'TRecord>,
            projection: ProjectionExpression<'TRecord, 'TProjection>,
            ?filterCondition: ConditionExpression<'TRecord>,
            ?limit: int,
            ?exclusiveStartKey: IndexKey,
            ?consistentRead: bool,
            ?scanIndexForward: bool
        ) : Async<PaginatedResult<'TProjection, IndexKey>> =
        async {

            let filterCondition = filterCondition |> Option.map (fun fc -> fc.Conditional)
            let! downloaded, lastEvaluatedKey =
                queryPaginatedAsync
                    keyCondition.Conditional
                    filterCondition
                    None
                    (LimitType.DefaultOrCount limit)
                    exclusiveStartKey
                    consistentRead
                    scanIndexForward
            return
                { Records = downloaded |> Seq.map projection.UnPickle |> Seq.toArray
                  LastEvaluatedKey = lastEvaluatedKey }
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
    member t.QueryProjectedPaginatedAsync<'TProjection>
        (
            keyCondition: Expr<'TRecord -> bool>,
            projection: Expr<'TRecord -> 'TProjection>,
            ?filterCondition: Expr<'TRecord -> bool>,
            ?limit: int,
            ?exclusiveStartKey: IndexKey,
            ?consistentRead: bool,
            ?scanIndexForward: bool
        ) : Async<PaginatedResult<'TProjection, IndexKey>> =
        let filterCondition = filterCondition |> Option.map template.PrecomputeConditionalExpr
        t.QueryProjectedPaginatedAsync(
            template.PrecomputeConditionalExpr keyCondition,
            template.PrecomputeProjectionExpr projection,
            ?filterCondition = filterCondition,
            ?limit = limit,
            ?exclusiveStartKey = exclusiveStartKey,
            ?consistentRead = consistentRead,
            ?scanIndexForward = scanIndexForward
        )


    /// <summary>
    ///     Asynchronously scans table with given condition expressions.
    /// </summary>
    /// <param name="filterCondition">Filter condition expression.</param>
    /// <param name="limit">Maximum number of items to evaluate.</param>
    /// <param name="consistentRead">Specify whether to perform consistent read operation.</param>
    member _.ScanAsync(?filterCondition: ConditionExpression<'TRecord>, ?limit: int, ?consistentRead: bool) : Async<'TRecord[]> = async {
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
    member t.ScanAsync(filterCondition: Expr<'TRecord -> bool>, ?limit: int, ?consistentRead: bool) : Async<'TRecord[]> =
        let cond = template.PrecomputeConditionalExpr filterCondition
        t.ScanAsync(cond, ?limit = limit, ?consistentRead = consistentRead)


    /// <summary>
    ///     Asynchronously scans table with given condition expressions.
    ///     Uses supplied projection expression to narrow downloaded attributes.
    ///     Projection type must be a tuple of zero or more non-conflicting properties.
    /// </summary>
    /// <param name="projection">Projection expression.</param>
    /// <param name="filterCondition">Filter condition expression.</param>
    /// <param name="limit">Maximum number of items to evaluate.</param>
    /// <param name="consistentRead">Specify whether to perform consistent read operation.</param>
    member _.ScanProjectedAsync<'TProjection>
        (
            projection: ProjectionExpression<'TRecord, 'TProjection>,
            ?filterCondition: ConditionExpression<'TRecord>,
            ?limit: int,
            ?consistentRead: bool
        ) : Async<'TProjection[]> =
        async {
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
    member t.ScanProjectedAsync<'TProjection>
        (
            projection: Expr<'TRecord -> 'TProjection>,
            ?filterCondition: Expr<'TRecord -> bool>,
            ?limit: int,
            ?consistentRead: bool
        ) : Async<'TProjection[]> =
        let filterCondition = filterCondition |> Option.map template.PrecomputeConditionalExpr
        t.ScanProjectedAsync(
            template.PrecomputeProjectionExpr projection,
            ?filterCondition = filterCondition,
            ?limit = limit,
            ?consistentRead = consistentRead
        )


    /// <summary>
    ///     Asynchronously scans table with given condition expressions.
    /// </summary>
    /// <param name="filterCondition">Filter condition expression.</param>
    /// <param name="limit">Maximum number of items per page - DynamoDB default is used if not specified.</param>
    /// <param name="exclusiveStartKey">LastEvaluatedKey from the previous page.</param>
    /// <param name="consistentRead">Specify whether to perform consistent read operation.</param>
    member _.ScanPaginatedAsync
        (
            ?filterCondition: ConditionExpression<'TRecord>,
            ?limit: int,
            ?exclusiveStartKey: TableKey,
            ?consistentRead: bool
        ) : Async<PaginatedResult<'TRecord, TableKey>> =
        async {
            let filterCondition = filterCondition |> Option.map (fun fc -> fc.Conditional)
            let! downloaded, lastEvaluatedKey =
                scanPaginatedAsync filterCondition None (LimitType.DefaultOrCount limit) exclusiveStartKey consistentRead
            return
                { Records = downloaded |> Seq.map template.OfAttributeValues |> Seq.toArray
                  LastEvaluatedKey = lastEvaluatedKey }
        }

    /// <summary>
    ///     Asynchronously scans table with given condition expressions.
    /// </summary>
    /// <param name="filterCondition">Filter condition expression.</param>
    /// <param name="limit">Maximum number of items per page - DynamoDB default is used if not specified.</param>
    /// <param name="exclusiveStartKey">LastEvaluatedKey from the previous page.</param>
    /// <param name="consistentRead">Specify whether to perform consistent read operation.</param>
    member t.ScanPaginatedAsync
        (
            filterCondition: Expr<'TRecord -> bool>,
            ?limit: int,
            ?exclusiveStartKey: TableKey,
            ?consistentRead: bool
        ) : Async<PaginatedResult<'TRecord, TableKey>> =
        let cond = template.PrecomputeConditionalExpr filterCondition
        t.ScanPaginatedAsync(cond, ?limit = limit, ?exclusiveStartKey = exclusiveStartKey, ?consistentRead = consistentRead)


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
    member _.ScanProjectedPaginatedAsync<'TProjection>
        (
            projection: ProjectionExpression<'TRecord, 'TProjection>,
            ?filterCondition: ConditionExpression<'TRecord>,
            ?limit: int,
            ?exclusiveStartKey: TableKey,
            ?consistentRead: bool
        ) : Async<PaginatedResult<'TProjection, TableKey>> =
        async {
            let filterCondition = filterCondition |> Option.map (fun fc -> fc.Conditional)
            let! downloaded, lastEvaluatedKey =
                scanPaginatedAsync
                    filterCondition
                    (Some projection.ProjectionExpr)
                    (LimitType.DefaultOrCount limit)
                    exclusiveStartKey
                    consistentRead
            return
                { Records = downloaded |> Seq.map projection.UnPickle |> Seq.toArray
                  LastEvaluatedKey = lastEvaluatedKey }
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
    member t.ScanProjectedPaginatedAsync<'TProjection>
        (
            projection: Expr<'TRecord -> 'TProjection>,
            ?filterCondition: Expr<'TRecord -> bool>,
            ?limit: int,
            ?exclusiveStartKey: TableKey,
            ?consistentRead: bool
        ) : Async<PaginatedResult<'TProjection, TableKey>> =
        let filterCondition = filterCondition |> Option.map template.PrecomputeConditionalExpr
        t.ScanProjectedPaginatedAsync(
            template.PrecomputeProjectionExpr projection,
            ?filterCondition = filterCondition,
            ?limit = limit,
            ?exclusiveStartKey = exclusiveStartKey,
            ?consistentRead = consistentRead
        )


    /// <summary>
    /// Asynchronously verifies that the table exists and is compatible with record key schema, throwing if it is incompatible.<br/>
    /// If the table is not present, it is created, with the specified <c>throughput</c> (and optionally <c>streaming</c>) configuration.<br/>
    /// See also <c>VerifyTableAsync</c>, which only verifies the Table is present and correct.<br/>
    /// See also <c>UpdateTableIfRequiredAsync</c>, which will adjust throughput and streaming if they are not as specified.
    /// </summary>
    /// <param name="throughput">Throughput configuration to use for the table.</param>
    /// <param name="streaming">Optional streaming configuration to apply for the table. Default: Disabled..</param>
    /// <param name="customize">Callback to post-process the <c>CreateTableRequest</c>.</param>
    member _.VerifyOrCreateTableAsync(throughput: Throughput, ?streaming, ?customize) : Async<TableDescription> =
        Provisioning.verifyOrCreate (client, tableName, template) (Some throughput) streaming customize

    /// <summary>
    /// Asynchronously verify that the table exists and is compatible with record key schema, or throw.<br/>
    /// See also <c>VerifyOrCreateTableAsync</c>, which performs the same check, but can create or re-provision the Table if required.
    /// </summary>
    member _.VerifyTableAsync() : Async<unit> = Provisioning.validateOnly (client, tableName, template)

    /// <summary>
    /// Adjusts the Table's configuration via <c>UpdateTable</c> if the <c>throughput</c> or <c>streaming</c> are not as specified.<br/>
    /// NOTE: The underlying API can throw if a change is currently in progress; see the DynamoDB <a href="https://docs.aws.amazon.com/amazondynamodb/latest/APIReference/API_UpdateTable.html"><c>UpdateTable</c> API documentation</a>.<br/>
    /// NOTE: Throws <c>InvalidOperationException</c> if the table is not yet <c>Active</c>. It is recommended to ensure the Table is prepared via <c>VerifyTableAsync</c> or <c>VerifyOrCreateTableAsync</c> to guard against the potential for this state.
    /// </summary>
    /// <param name="throughput">Throughput configuration to use for the table. Always applied via either <c>CreateTable</c> or <c>UpdateTable</c>.</param>
    /// <param name="streaming">Optional streaming configuration to apply for the table. Default (if creating): Disabled. Default: (if existing) do not change.</param>
    /// <param name="custom">Callback to post-process the <c>UpdateTableRequest</c>. <c>UpdateTable</c> is inhibited if it returns <c>false</c> and no other configuration requires a change.</param>
    /// <param name="currentTableDescription">Current table configuration, if known. Retrieved via <c>DescribeTable</c> if not supplied.</param>
    member _.UpdateTableIfRequiredAsync(?throughput: Throughput, ?streaming, ?custom, ?currentTableDescription) : Async<TableDescription> = async {
        let! tableDescription = async {
            match currentTableDescription with
            | Some d -> return d
            | None ->
                match! Provisioning.tryDescribe (client, tableName) with
                | Some d -> return d
                | None ->
                    return
                        invalidOp
                            "Table is not currently Active. Please use VerifyTableAsync or VerifyOrCreateTableAsync to guard against this state."
        }
        match UpdateTableRequest.createIfRequired tableName tableDescription throughput streaming custom with
        | None -> return tableDescription
        | Some request -> return! UpdateTableRequest.execute client request
    }

    /// <summary>Asynchronously updates the underlying table with supplied provisioned throughput.</summary>
    /// <param name="provisionedThroughput">Provisioned throughput to use on table.</param>
    [<System.Obsolete("Please replace with UpdateTableIfRequiredAsync")>]
    member t.UpdateProvisionedThroughputAsync(provisionedThroughput: ProvisionedThroughput) : Async<unit> =
        t.UpdateTableIfRequiredAsync(Throughput.Provisioned provisionedThroughput) |> Async.Ignore

    /// <summary>Asynchronously verify that the table exists and is compatible with record key schema.</summary>
    /// <param name="createIfNotExists">Create the table instance now instance if it does not exist. Defaults to false.</param>
    /// <param name="provisionedThroughput">Provisioned throughput for the table if newly created. Defaults to (10,10).</param>
    [<System.Obsolete("Please replace with either 1. VerifyTableAsync or 2. VerifyOrCreateTableAsync")>]
    member t.VerifyTableAsync(?createIfNotExists: bool, ?provisionedThroughput: ProvisionedThroughput) : Async<unit> =
        if createIfNotExists = Some true then
            let throughput =
                match provisionedThroughput with
                | Some p -> p
                | None -> ProvisionedThroughput(10L, 10L)
            t.VerifyOrCreateTableAsync(Throughput.Provisioned throughput) |> Async.Ignore
        else
            t.VerifyTableAsync()

    member t.Transaction() =
        match metricsCollector with
        | Some metricsCollector -> Transaction(metricsCollector = metricsCollector)
        | None -> Transaction()

/// <summary>
///    Represents a transactional set of operations to be applied atomically to a arbitrary number of DynamoDB tables.
/// </summary>
/// <param name="metricsCollector">Function to receive request metrics.</param>
and Transaction(?metricsCollector: (RequestMetrics -> unit)) =
    let transactionItems = ResizeArray<TransactWriteItem>()
    let mutable (dynamoDbClient: IAmazonDynamoDB) = null

    let setClient client =
        if dynamoDbClient = null then
            dynamoDbClient <- client

    let reportMetrics collector (tableName: string) (operation: Operation) (consumedCapacity: ConsumedCapacity list) (itemCount: int) =
        collector
            { TableName = tableName
              Operation = operation
              ConsumedCapacity = consumedCapacity
              ItemCount = itemCount }

    let returnConsumedCapacity, maybeReport =
        match metricsCollector with
        | Some sink -> ReturnConsumedCapacity.INDEXES, Some(reportMetrics sink)
        | None -> ReturnConsumedCapacity.NONE, None

    /// <summary>
    ///    Adds a Put operation to the transaction.
    /// </summary>
    /// <param name="tableContext">Table context to operate on.</param>
    /// <param name="item">Item to be put.</param>
    /// <param name="precondition">Optional precondition expression.</param>
    member this.Put<'TRecord>
        (
            tableContext: TableContext<'TRecord>,
            item: 'TRecord,
            ?precondition: ConditionExpression<'TRecord>
        ) : Transaction =
        setClient tableContext.Client
        let req = Put(TableName = tableContext.TableName, Item = tableContext.Template.ToAttributeValues item)
        precondition
        |> Option.iter (fun cond ->
            let writer = AttributeWriter(req.ExpressionAttributeNames, req.ExpressionAttributeValues)
            req.ConditionExpression <- cond.Conditional.Write writer)
        transactionItems.Add(TransactWriteItem(Put = req))
        this

    /// <summary>
    ///   Adds a ConditionCheck operation to the transaction.
    /// </summary>
    /// <param name="tableContext">Table context to operate on.</param>
    /// <param name="key">Key of item to check.</param>
    /// <param name="condition">Condition to check.</param>
    member this.Check(tableContext: TableContext<'TRecord>, key: TableKey, condition: ConditionExpression<'TRecord>) : Transaction =
        setClient tableContext.Client

        let req = ConditionCheck(TableName = tableContext.TableName, Key = tableContext.Template.ToAttributeValues key)
        let writer = AttributeWriter(req.ExpressionAttributeNames, req.ExpressionAttributeValues)
        req.ConditionExpression <- condition.Conditional.Write writer
        transactionItems.Add(TransactWriteItem(ConditionCheck = req))
        this

    /// <summary>
    ///   Adds an Update operation to the transaction.
    /// </summary>
    /// <param name="tableContext">Table context to operate on.</param>
    /// <param name="key">Key of item to update.</param>
    /// <param name="updater">Update expression.</param>
    /// <param name="precondition">Optional precondition expression.</param>
    member this.Update
        (
            tableContext: TableContext<'TRecord>,
            key: TableKey,
            updater: UpdateExpression<'TRecord>,
            ?precondition: ConditionExpression<'TRecord>

        ) : Transaction =
        setClient tableContext.Client

        let req = Update(TableName = tableContext.TableName, Key = tableContext.Template.ToAttributeValues key)
        let writer = AttributeWriter(req.ExpressionAttributeNames, req.ExpressionAttributeValues)
        req.UpdateExpression <- updater.UpdateOps.Write(writer)
        precondition |> Option.iter (fun cond -> req.ConditionExpression <- cond.Conditional.Write writer)
        transactionItems.Add(TransactWriteItem(Update = req))
        this

    /// <summary>
    ///  Adds a Delete operation to the transaction.
    /// </summary>
    /// <param name="tableContext">Table context to operate on.</param>
    /// <param name="key">Key of item to delete.</param>
    /// <param name="precondition">Optional precondition expression.</param>
    member this.Delete
        (
            tableContext: TableContext<'TRecord>,
            key: TableKey,
            precondition: option<ConditionExpression<'TRecord>>
        ) : Transaction =
        setClient tableContext.Client

        let req = Delete(TableName = tableContext.TableName, Key = tableContext.Template.ToAttributeValues key)
        precondition
        |> Option.iter (fun cond ->
            let writer = AttributeWriter(req.ExpressionAttributeNames, req.ExpressionAttributeValues)
            req.ConditionExpression <- cond.Conditional.Write writer)
        transactionItems.Add(TransactWriteItem(Delete = req))
        this

    /// <summary>
    ///     Atomically applies a set of 1-100 operations to the table.<br/>
    ///     NOTE requests are charged at twice the normal rate in Write Capacity Units.
    ///     See the DynamoDB <a href="https://docs.aws.amazon.com/amazondynamodb/latest/APIReference/API_TransactWriteItems.html"><c>TransactWriteItems</c> API documentation</a> for full details of semantics and charges.<br/>
    /// </summary>
    /// <param name="clientRequestToken">The <c>ClientRequestToken</c> to supply as an idempotency key (10 minute window).</param>
    member _.TransactWriteItems(?clientRequestToken) : Async<unit> = async {
        if (Seq.length transactionItems) = 0 || (Seq.length transactionItems) > 100 then
            raise
            <| System.ArgumentOutOfRangeException(nameof transactionItems, "must be between 1 and 100 items.")
        let req = TransactWriteItemsRequest(ReturnConsumedCapacity = returnConsumedCapacity, TransactItems = (ResizeArray transactionItems))
        clientRequestToken |> Option.iter (fun x -> req.ClientRequestToken <- x)
        let! ct = Async.CancellationToken
        let! response = dynamoDbClient.TransactWriteItemsAsync(req, ct) |> Async.AwaitTaskCorrect
        maybeReport
        |> Option.iter (fun r ->
            response.ConsumedCapacity
            |> Seq.groupBy (fun x -> x.TableName)
            |> Seq.iter (fun (tableName, consumedCapacity) ->
                r tableName Operation.TransactWriteItems (Seq.toList consumedCapacity) (Seq.length transactionItems)))
        if response.HttpStatusCode <> HttpStatusCode.OK then
            failwithf "TransactWriteItems request returned error %O" response.HttpStatusCode
    }

// Deprecated factory method, to be removed. Replaced with
// 1. TableContext<'T> ctor (synchronous)
// 2. VerifyOrCreateTableAsync OR VerifyTableAsync (explicitly async to signify that verification/creation is a costly and/or privileged operation)
type TableContext internal () =

    static member private CreateAsyncImpl<'TRecord>
        (
            client: IAmazonDynamoDB,
            tableName: string,
            ?verifyTable: bool,
            ?createIfNotExists: bool,
            ?provisionedThroughput: ProvisionedThroughput,
            ?metricsCollector: RequestMetrics -> unit
        ) =
        async {
            let context = TableContext<'TRecord>(client, tableName, ?metricsCollector = metricsCollector)
            if createIfNotExists = Some true then
                let throughput =
                    match provisionedThroughput with
                    | Some p -> p
                    | None -> ProvisionedThroughput(10L, 10L)
                do! context.VerifyOrCreateTableAsync(Throughput.Provisioned throughput) |> Async.Ignore
            elif verifyTable <> Some false then
                do! context.VerifyTableAsync()
            return context
        }

    /// <summary>
    ///     Creates a DynamoDB client instance for given F# record and table name.
    /// </summary>
    /// <param name="client">DynamoDB client instance.</param>
    /// <param name="tableName">Table name to target.</param>
    /// <param name="verifyTable">Verify that the table exists and is compatible with supplied record schema. Defaults to true.</param>
    /// <param name="createIfNotExists">Create the table now if it does not exist. Defaults to false.</param>
    /// <param name="provisionedThroughput">Provisioned throughput for the table if newly created. Default: 10 RCU, 10 WCU</param>
    /// <param name="metricsCollector">Function to receive request metrics.</param>
    [<System.Obsolete(@"This method has been deprecated. Please use TableContext constructor
                        (optionally followed by VerifyTableAsync or VerifyOrCreateTableAsync)")>]
    static member CreateAsync<'TRecord>
        (
            client: IAmazonDynamoDB,
            tableName: string,
            ?verifyTable: bool,
            ?createIfNotExists: bool,
            ?provisionedThroughput: ProvisionedThroughput,
            ?metricsCollector: RequestMetrics -> unit
        ) =
        TableContext.CreateAsyncImpl<'TRecord>(
            client,
            tableName,
            ?verifyTable = verifyTable,
            ?createIfNotExists = createIfNotExists,
            ?provisionedThroughput = provisionedThroughput,
            ?metricsCollector = metricsCollector
        )

    /// <summary>
    ///     Creates a DynamoDB client instance for given F# record and table name.
    /// </summary>
    /// <param name="client">DynamoDB client instance.</param>
    /// <param name="tableName">Table name to target.</param>
    /// <param name="verifyTable">Verify that the table exists and is compatible with supplied record schema. Defaults to true.</param>
    /// <param name="createIfNotExists">Create the table now instance if it does not exist. Defaults to false.</param>
    /// <param name="provisionedThroughput">Provisioned throughput for the table if newly created.</param>
    /// <param name="metricsCollector">Function to receive request metrics.</param>
    [<System.Obsolete(@"Creation with synchronous verification has been deprecated. Please use either
                        1. TableContext constructor (optionally followed by VerifyTableAsync or VerifyOrCreateTableAsync) OR
                        2. (for scripting scenarios) Scripting.TableContext.Initialize")>]
    static member Create<'TRecord>
        (
            client: IAmazonDynamoDB,
            tableName: string,
            ?verifyTable: bool,
            ?createIfNotExists: bool,
            ?provisionedThroughput: ProvisionedThroughput,
            ?metricsCollector: RequestMetrics -> unit
        ) =
        TableContext.CreateAsyncImpl<'TRecord>(
            client,
            tableName,
            ?verifyTable = verifyTable,
            ?createIfNotExists = createIfNotExists,
            ?provisionedThroughput = provisionedThroughput,
            ?metricsCollector = metricsCollector
        )
        |> Async.RunSynchronously



/// <summary>
/// Sync-over-Async helpers that can be opted-into when working in scripting scenarios.
/// For normal usage, the <c>Async</c> versions of any given API is recommended, in order to ensure one
/// implements correct handling of the intrinsic asynchronous nature of the underlying interface
/// </summary>
module Scripting =

    /// Factory methods for scripting scenarios
    type TableContext internal () =

        /// <summary>
        /// Creates a DynamoDB client instance for the specified F# record type, client and table name.<br/>
        /// Validates the table exists, and has the correct schema as per <c>VerifyTableAsync</c>.<br/>
        /// See other overload for <c>VerifyOrCreateTableAsync</c> semantics.
        /// </summary>
        /// <param name="client">DynamoDB client instance.</param>
        /// <param name="tableName">Table name to target.</param>
        static member Initialize<'TRecord>(client: IAmazonDynamoDB, tableName: string) : TableContext<'TRecord> =
            let context = TableContext<'TRecord>(client, tableName)
            context.VerifyTableAsync() |> Async.RunSynchronously
            context

        /// Creates a DynamoDB client instance for the specified F# record type, client and table name.<br/>
        /// Either validates the table exists and has the correct schema, or creates a fresh one, as per <c>VerifyOrCreateTableAsync</c>.<br/>
        /// See other overload for <c>VerifyTableAsync</c> semantics.
        /// <param name="client">DynamoDB client instance.</param>
        /// <param name="tableName">Table name to target.</param>
        /// <param name="throughput">Throughput to configure if the Table does not yet exist.</param>
        static member Initialize<'TRecord>(client: IAmazonDynamoDB, tableName: string, throughput) : TableContext<'TRecord> =
            let context = TableContext<'TRecord>(client, tableName)
            let _desc = context.VerifyOrCreateTableAsync(throughput) |> Async.RunSynchronously
            context

        /// Creates a DynamoDB client instance for the specified F# record type, client and table name.<br/>
        /// Either validates the table exists and has the correct schema, or creates a fresh one, as per <c>VerifyOrCreateTableAsync</c>.<br/>
        /// See other overload for <c>VerifyTableAsync</c> semantics.
        /// <param name="client">DynamoDB client instance.</param>
        /// <param name="tableName">Table name to target.</param>
        /// <param name="throughput">Throughput to configure if the table does not yet exist.</param>
        /// <param name="streaming">Streaming configuration applied if the table does not yet exist.</param>
        static member Initialize<'TRecord>(client: IAmazonDynamoDB, tableName: string, throughput, streaming) : TableContext<'TRecord> =
            let context = TableContext<'TRecord>(client, tableName)
            let _desc = context.VerifyOrCreateTableAsync(throughput, streaming) |> Async.RunSynchronously
            context

    type TableContext<'TRecord> with

        /// <summary>
        ///     Puts a record item in the table.
        /// </summary>
        /// <param name="item">Item to be written.</param>
        /// <param name="precondition">Precondition to satisfy where item already exists. Use <c>Precondition.CheckFailed</c> to identify Precondition Check failures.</param>
        member t.PutItem(item: 'TRecord, ?precondition: ConditionExpression<'TRecord>) =
            t.PutItemAsync(item, ?precondition = precondition) |> Async.RunSynchronously

        /// <summary>
        ///     Puts a record item in the table.
        /// </summary>
        /// <param name="item">Item to be written.</param>
        /// <param name="precondition">Precondition to satisfy where item already exists. Use <c>Precondition.CheckFailed</c> to identify Precondition Check failures.</param>
        member t.PutItem(item: 'TRecord, precondition: Expr<'TRecord -> bool>) =
            t.PutItemAsync(item, precondition) |> Async.RunSynchronously


        /// <summary>
        ///     Puts a collection of items to the table as a batch write operation.
        ///     At most 25 items can be written in a single batch write operation.
        /// </summary>
        /// <returns>Any unprocessed items due to throttling.</returns>
        /// <param name="items">Items to be written.</param>
        member t.BatchPutItems(items: seq<'TRecord>) = t.BatchPutItemsAsync(items) |> Async.RunSynchronously


        /// <summary>
        ///     Updates item with supplied key using provided update expression.
        /// </summary>
        /// <param name="key">Key of item to be updated.</param>
        /// <param name="updater">Table update expression.</param>
        /// <param name="precondition">Precondition to satisfy where item already exists. Use <c>Precondition.CheckFailed</c> to identify Precondition Check failures.</param>
        /// <param name="returnLatest">Specifies the operation should return the latest (true) or older (false) version of the item. Defaults to latest.</param>
        member t.UpdateItem
            (
                key: TableKey,
                updater: UpdateExpression<'TRecord>,
                ?precondition: ConditionExpression<'TRecord>,
                ?returnLatest: bool
            ) =
            t.UpdateItemAsync(key, updater, ?precondition = precondition, ?returnLatest = returnLatest)
            |> Async.RunSynchronously

        /// <summary>
        ///     Updates item with supplied key using provided record update expression.
        /// </summary>
        /// <param name="key">Key of item to be updated.</param>
        /// <param name="updater">Table update expression.</param>
        /// <param name="precondition">Precondition to satisfy where item already exists. Use <c>Precondition.CheckFailed</c> to identify Precondition Check failures.</param>
        /// <param name="returnLatest">Specifies the operation should return the latest (true) or older (false) version of the item. Defaults to latest.</param>
        member t.UpdateItem
            (
                key: TableKey,
                updater: Expr<'TRecord -> 'TRecord>,
                ?precondition: Expr<'TRecord -> bool>,
                ?returnLatest: bool
            ) =
            t.UpdateItemAsync(key, updater, ?precondition = precondition, ?returnLatest = returnLatest)
            |> Async.RunSynchronously

        /// <summary>
        ///     Updates item with supplied key using provided record update operation.
        /// </summary>
        /// <param name="key">Key of item to be updated.</param>
        /// <param name="updater">Table update expression.</param>
        /// <param name="precondition">Precondition to satisfy where item already exists. Use <c>Precondition.CheckFailed</c> to identify Precondition Check failures.</param>
        /// <param name="returnLatest">Specifies the operation should return the latest (true) or older (false) version of the item. Defaults to latest.</param>
        member t.UpdateItem
            (
                key: TableKey,
                updater: Expr<'TRecord -> UpdateOp>,
                ?precondition: Expr<'TRecord -> bool>,
                ?returnLatest: bool
            ) =
            t.UpdateItemAsync(key, updater, ?precondition = precondition, ?returnLatest = returnLatest)
            |> Async.RunSynchronously


        /// <summary>
        ///     Checks whether item of supplied key exists in table.
        /// </summary>
        /// <param name="key">Key to be checked.</param>
        member t.ContainsKey(key: TableKey) = t.ContainsKeyAsync(key) |> Async.RunSynchronously

        /// <summary>
        ///     Fetches item of given key from table.
        /// </summary>
        /// <param name="key">Key of item to be fetched.</param>
        member r.GetItem(key: TableKey) = r.GetItemAsync(key) |> Async.RunSynchronously


        /// <summary>
        ///     Fetches item of given key from table.
        ///     Uses supplied projection expression to narrow downloaded attributes.
        ///     Projection type must be a tuple of zero or more non-conflicting properties.
        /// </summary>
        /// <param name="key">Key of item to be fetched.</param>
        /// <param name="projection">Projection expression to be applied to item.</param>
        member t.GetItemProjected(key: TableKey, projection: ProjectionExpression<'TRecord, 'TProjection>) : 'TProjection =
            t.GetItemProjectedAsync(key, projection) |> Async.RunSynchronously

        /// <summary>
        ///     Fetches item of given key from table.
        ///     Uses supplied projection expression to narrow downloaded attributes.
        ///     Projection type must be a tuple of zero or more non-conflicting properties.
        /// </summary>
        /// <param name="key">Key of item to be fetched.</param>
        /// <param name="projection">Projection expression to be applied to item.</param>
        member t.GetItemProjected(key: TableKey, projection: Expr<'TRecord -> 'TProjection>) : 'TProjection =
            // TOCONSIDER implement in terms of Async equivalent as per the rest
            t.GetItemProjected(key, t.Template.PrecomputeProjectionExpr projection)


        /// <summary>
        ///     Performs a batch fetch of items with supplied keys.
        /// </summary>
        /// <param name="keys">Keys of items to be fetched.</param>
        /// <param name="consistentRead">Perform consistent read. Defaults to false.</param>
        member t.BatchGetItems(keys: seq<TableKey>, ?consistentRead: bool) =
            t.BatchGetItemsAsync(keys, ?consistentRead = consistentRead) |> Async.RunSynchronously


        /// <summary>
        ///     Asynchronously performs a batch fetch of items with supplied keys.
        /// </summary>
        /// <param name="keys">Keys of items to be fetched.</param>
        /// <param name="projection">Projection expression to be applied to item.</param>
        /// <param name="consistentRead">Perform consistent read. Defaults to false.</param>
        member t.BatchGetItemsProjected<'TProjection>
            (
                keys: seq<TableKey>,
                projection: ProjectionExpression<'TRecord, 'TProjection>,
                ?consistentRead: bool
            ) : 'TProjection[] =
            t.BatchGetItemsProjectedAsync(keys, projection, ?consistentRead = consistentRead)
            |> Async.RunSynchronously


        /// <summary>
        ///     Asynchronously performs a batch fetch of items with supplied keys.
        /// </summary>
        /// <param name="keys">Keys of items to be fetched.</param>
        /// <param name="projection">Projection expression to be applied to item.</param>
        /// <param name="consistentRead">Perform consistent read. Defaults to false.</param>
        member t.BatchGetItemsProjected<'TProjection>
            (
                keys: seq<TableKey>,
                projection: Expr<'TRecord -> 'TProjection>,
                ?consistentRead: bool
            ) : 'TProjection[] =
            t.BatchGetItemsProjectedAsync(keys, projection, ?consistentRead = consistentRead)
            |> Async.RunSynchronously

        /// <summary>Deletes item of given key from table.</summary>
        /// <param name="key">Key of item to be deleted.</param>
        /// <param name="precondition">Precondition to satisfy where item exists. Use <c>Precondition.CheckFailed</c> to identify Precondition Check failures.</param>
        member t.DeleteItem(key: TableKey, ?precondition: ConditionExpression<'TRecord>) =
            t.DeleteItemAsync(key, ?precondition = precondition) |> Async.RunSynchronously

        /// <summary>
        ///     Deletes item of given key from table.
        /// </summary>
        /// <param name="key">Key of item to be deleted.</param>
        /// <param name="precondition">Precondition to satisfy where item exists. Use <c>Precondition.CheckFailed</c> to identify Precondition Check failures.</param>
        member t.DeleteItem(key: TableKey, precondition: Expr<'TRecord -> bool>) =
            t.DeleteItemAsync(key, precondition) |> Async.RunSynchronously


        /// <summary>
        ///     Performs batch delete operation on items of given keys.
        /// </summary>
        /// <returns>Any unprocessed keys due to throttling.</returns>
        /// <param name="keys">Keys of items to be deleted.</param>
        member t.BatchDeleteItems(keys: seq<TableKey>) = t.BatchDeleteItemsAsync(keys) |> Async.RunSynchronously


        /// <summary>
        ///     Queries table with given condition expressions.
        /// </summary>
        /// <param name="keyCondition">Key condition expression.</param>
        /// <param name="filterCondition">Filter condition expression.</param>
        /// <param name="limit">Maximum number of items to evaluate.</param>
        /// <param name="consistentRead">Specify whether to perform consistent read operation.</param>
        /// <param name="scanIndexForward">Specifies the order in which to evaluate results. Either ascending (true) or descending (false).</param>
        member t.Query
            (
                keyCondition: ConditionExpression<'TRecord>,
                ?filterCondition: ConditionExpression<'TRecord>,
                ?limit: int,
                ?consistentRead: bool,
                ?scanIndexForward: bool
            ) : 'TRecord[] =
            t.QueryAsync(
                keyCondition,
                ?filterCondition = filterCondition,
                ?limit = limit,
                ?consistentRead = consistentRead,
                ?scanIndexForward = scanIndexForward
            )
            |> Async.RunSynchronously

        /// <summary>
        ///     Queries table with given condition expressions.
        /// </summary>
        /// <param name="keyCondition">Key condition expression.</param>
        /// <param name="filterCondition">Filter condition expression.</param>
        /// <param name="limit">Maximum number of items to evaluate.</param>
        /// <param name="consistentRead">Specify whether to perform consistent read operation.</param>
        /// <param name="scanIndexForward">Specifies the order in which to evaluate results. Either ascending (true) or descending (false).</param>
        member t.Query
            (
                keyCondition: Expr<'TRecord -> bool>,
                ?filterCondition: Expr<'TRecord -> bool>,
                ?limit: int,
                ?consistentRead: bool,
                ?scanIndexForward: bool
            ) : 'TRecord[] =
            t.QueryAsync(
                keyCondition,
                ?filterCondition = filterCondition,
                ?limit = limit,
                ?consistentRead = consistentRead,
                ?scanIndexForward = scanIndexForward
            )
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
        member t.QueryProjected<'TProjection>
            (
                keyCondition: ConditionExpression<'TRecord>,
                projection: ProjectionExpression<'TRecord, 'TProjection>,
                ?filterCondition: ConditionExpression<'TRecord>,
                ?limit: int,
                ?consistentRead: bool,
                ?scanIndexForward: bool
            ) : 'TProjection[] =
            t.QueryProjectedAsync(
                keyCondition,
                projection,
                ?filterCondition = filterCondition,
                ?limit = limit,
                ?consistentRead = consistentRead,
                ?scanIndexForward = scanIndexForward
            )
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
        member t.QueryProjected<'TProjection>
            (
                keyCondition: Expr<'TRecord -> bool>,
                projection: Expr<'TRecord -> 'TProjection>,
                ?filterCondition: Expr<'TRecord -> bool>,
                ?limit: int,
                ?consistentRead: bool,
                ?scanIndexForward: bool
            ) : 'TProjection[] =
            t.QueryProjectedAsync(
                keyCondition,
                projection,
                ?filterCondition = filterCondition,
                ?limit = limit,
                ?consistentRead = consistentRead,
                ?scanIndexForward = scanIndexForward
            )
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
        member t.QueryPaginated
            (
                keyCondition: ConditionExpression<'TRecord>,
                ?filterCondition: ConditionExpression<'TRecord>,
                ?limit: int,
                ?exclusiveStartKey: IndexKey,
                ?consistentRead: bool,
                ?scanIndexForward: bool
            ) : PaginatedResult<'TRecord, IndexKey> =
            t.QueryPaginatedAsync(
                keyCondition,
                ?filterCondition = filterCondition,
                ?limit = limit,
                ?exclusiveStartKey = exclusiveStartKey,
                ?consistentRead = consistentRead,
                ?scanIndexForward = scanIndexForward
            )
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
        member t.QueryPaginated
            (
                keyCondition: Expr<'TRecord -> bool>,
                ?filterCondition: Expr<'TRecord -> bool>,
                ?limit: int,
                ?exclusiveStartKey: IndexKey,
                ?consistentRead: bool,
                ?scanIndexForward: bool
            ) : PaginatedResult<'TRecord, IndexKey> =
            t.QueryPaginatedAsync(
                keyCondition,
                ?filterCondition = filterCondition,
                ?limit = limit,
                ?exclusiveStartKey = exclusiveStartKey,
                ?consistentRead = consistentRead,
                ?scanIndexForward = scanIndexForward
            )
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
        member t.QueryProjectedPaginated<'TProjection>
            (
                keyCondition: ConditionExpression<'TRecord>,
                projection: ProjectionExpression<'TRecord, 'TProjection>,
                ?filterCondition: ConditionExpression<'TRecord>,
                ?limit: int,
                ?exclusiveStartKey: IndexKey,
                ?consistentRead: bool,
                ?scanIndexForward: bool
            ) : PaginatedResult<'TProjection, IndexKey> =
            t.QueryProjectedPaginatedAsync(
                keyCondition,
                projection,
                ?filterCondition = filterCondition,
                ?limit = limit,
                ?exclusiveStartKey = exclusiveStartKey,
                ?consistentRead = consistentRead,
                ?scanIndexForward = scanIndexForward
            )
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
        member t.QueryProjectedPaginated<'TProjection>
            (
                keyCondition: Expr<'TRecord -> bool>,
                projection: Expr<'TRecord -> 'TProjection>,
                ?filterCondition: Expr<'TRecord -> bool>,
                ?limit: int,
                ?exclusiveStartKey: IndexKey,
                ?consistentRead: bool,
                ?scanIndexForward: bool
            ) : PaginatedResult<'TProjection, IndexKey> =
            t.QueryProjectedPaginatedAsync(
                keyCondition,
                projection,
                ?filterCondition = filterCondition,
                ?limit = limit,
                ?exclusiveStartKey = exclusiveStartKey,
                ?consistentRead = consistentRead,
                ?scanIndexForward = scanIndexForward
            )
            |> Async.RunSynchronously


        /// <summary>
        ///     Scans table with given condition expressions.
        /// </summary>
        /// <param name="filterCondition">Filter condition expression.</param>
        /// <param name="limit">Maximum number of items to evaluate.</param>
        /// <param name="consistentRead">Specify whether to perform consistent read operation.</param>
        member t.Scan(?filterCondition: ConditionExpression<'TRecord>, ?limit: int, ?consistentRead: bool) : 'TRecord[] =
            t.ScanAsync(?filterCondition = filterCondition, ?limit = limit, ?consistentRead = consistentRead)
            |> Async.RunSynchronously

        /// <summary>
        ///     Scans table with given condition expressions.
        /// </summary>
        /// <param name="filterCondition">Filter condition expression.</param>
        /// <param name="limit">Maximum number of items to evaluate.</param>
        /// <param name="consistentRead">Specify whether to perform consistent read operation.</param>
        member t.Scan(filterCondition: Expr<'TRecord -> bool>, ?limit: int, ?consistentRead: bool) : 'TRecord[] =
            t.ScanAsync(filterCondition, ?limit = limit, ?consistentRead = consistentRead)
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
        member t.ScanProjected<'TProjection>
            (
                projection: ProjectionExpression<'TRecord, 'TProjection>,
                ?filterCondition: ConditionExpression<'TRecord>,
                ?limit: int,
                ?consistentRead: bool
            ) : 'TProjection[] =
            t.ScanProjectedAsync(projection, ?filterCondition = filterCondition, ?limit = limit, ?consistentRead = consistentRead)
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
        member t.ScanProjected<'TProjection>
            (
                projection: Expr<'TRecord -> 'TProjection>,
                ?filterCondition: Expr<'TRecord -> bool>,
                ?limit: int,
                ?consistentRead: bool
            ) : 'TProjection[] =
            t.ScanProjectedAsync(projection, ?filterCondition = filterCondition, ?limit = limit, ?consistentRead = consistentRead)
            |> Async.RunSynchronously

        /// <summary>
        ///     Scans table with given condition expressions.
        /// </summary>
        /// <param name="filterCondition">Filter condition expression.</param>
        /// <param name="limit">Maximum number of items per page - DynamoDB default is used if not specified.</param>
        /// <param name="exclusiveStartKey">LastEvaluatedKey from the previous page.</param>
        /// <param name="consistentRead">Specify whether to perform consistent read operation.</param>
        member t.ScanPaginated
            (
                ?filterCondition: ConditionExpression<'TRecord>,
                ?limit: int,
                ?exclusiveStartKey: TableKey,
                ?consistentRead: bool
            ) : PaginatedResult<'TRecord, TableKey> =
            t.ScanPaginatedAsync(
                ?filterCondition = filterCondition,
                ?limit = limit,
                ?exclusiveStartKey = exclusiveStartKey,
                ?consistentRead = consistentRead
            )
            |> Async.RunSynchronously


        /// <summary>
        ///     Scans table with given condition expressions.
        /// </summary>
        /// <param name="filterCondition">Filter condition expression.</param>
        /// <param name="limit">Maximum number of items per page - DynamoDB default is used if not specified.</param>
        /// <param name="exclusiveStartKey">LastEvaluatedKey from the previous page.</param>
        /// <param name="consistentRead">Specify whether to perform consistent read operation.</param>
        member t.ScanPaginated
            (
                filterCondition: Expr<'TRecord -> bool>,
                ?limit: int,
                ?exclusiveStartKey: TableKey,
                ?consistentRead: bool
            ) : PaginatedResult<'TRecord, TableKey> =
            t.ScanPaginatedAsync(filterCondition, ?limit = limit, ?exclusiveStartKey = exclusiveStartKey, ?consistentRead = consistentRead)
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
        member t.ScanProjectedPaginated<'TProjection>
            (
                projection: ProjectionExpression<'TRecord, 'TProjection>,
                ?filterCondition: ConditionExpression<'TRecord>,
                ?limit: int,
                ?exclusiveStartKey: TableKey,
                ?consistentRead: bool
            ) : PaginatedResult<'TProjection, TableKey> =
            t.ScanProjectedPaginatedAsync(
                projection,
                ?filterCondition = filterCondition,
                ?limit = limit,
                ?exclusiveStartKey = exclusiveStartKey,
                ?consistentRead = consistentRead
            )
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
        member t.ScanProjectedPaginated<'TProjection>
            (
                projection: Expr<'TRecord -> 'TProjection>,
                ?filterCondition: Expr<'TRecord -> bool>,
                ?limit: int,
                ?exclusiveStartKey: TableKey,
                ?consistentRead: bool
            ) : PaginatedResult<'TProjection, TableKey> =
            t.ScanProjectedPaginatedAsync(
                projection,
                ?filterCondition = filterCondition,
                ?limit = limit,
                ?exclusiveStartKey = exclusiveStartKey,
                ?consistentRead = consistentRead
            )
            |> Async.RunSynchronously


        /// <summary>Updates the underlying table with supplied provisioned throughput.</summary>
        /// <param name="provisionedThroughput">Provisioned throughput to use on table.</param>
        member t.UpdateProvisionedThroughput(provisionedThroughput: ProvisionedThroughput) : unit =
            let spec = Throughput.Provisioned provisionedThroughput
            t.UpdateTableIfRequiredAsync(spec) |> Async.Ignore |> Async.RunSynchronously

/// Helpers for working with <c>TransactWriteItemsRequest</c>
module TransactWriteItemsRequest =
    /// <summary>Exception filter to identify whether a <c>TransactWriteItems</c> call has failed due to
    /// one or more of the supplied <c>precondition</c> checks failing.</summary>
    let (|TransactionCanceledConditionalCheckFailed|_|): exn -> unit option =
        function
        | :? TransactionCanceledException as e when e.CancellationReasons.Exists(fun x -> x.Code = "ConditionalCheckFailed") -> Some()
        | _ -> None
