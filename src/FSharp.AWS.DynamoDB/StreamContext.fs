namespace FSharp.AWS.DynamoDB

open System
open System.Threading
open System.Threading.Tasks

open Amazon.DynamoDBv2
open Amazon.DynamoDBv2.Model

type StreamOperation =
    | Insert
    | Modify
    | Remove

type StreamRecord<'TRecord> =
    { TableKey: TableKey
      Operation: StreamOperation
      ApproximateCreationDateTime: DateTimeOffset
      New: 'TRecord option
      Old: 'TRecord option }

type StreamPosition = { ShardId: string; SequenceNumber: string option }

type ReadStreamFrom =
    | Oldest
    | Newest
    | Positions of StreamPosition list

module private Shard =

    /// <summary>
    /// Returns all of the open shards (with no ending sequence number) in the stream
    /// </summary>
    /// <param name="shards">sequence of `Shard` objects from the `StreamDescription`</param>
    let openShards (shards: Shard seq) = shards |> Seq.filter (fun s -> String.IsNullOrEmpty(s.SequenceNumberRange.EndingSequenceNumber))

    /// <summary>
    /// Returns the ancestor shard ids of a shard in the stream
    /// </summary>
    /// <param name="shardId">shard id of the shard to find ancestors for</param>
    /// <param name="shards">sequence of `Shard` objects from the `StreamDescription`</param>
    let ancestors (shards: Shard seq) (shardId: string) =
        let rec ancestorsInner (shardId: string) =
            match shards |> Seq.tryFind (fun s -> s.ShardId = shardId) with
            | None -> []
            | Some s ->
                match s.ParentShardId with
                | null
                | "" -> []
                | parent -> parent :: ancestorsInner parent

        ancestorsInner shardId |> Seq.distinct

    /// <summary>
    /// Returns true if the shard id is in the list of shards
    /// </summary>
    /// <param name="shards">sequence of `Shard` objects from the `StreamDescription`</param>
    /// <param name="shardId">shard id to check for</param>
    let containsShardId (shards: Shard seq) (shardId: string) = shards |> Seq.exists (fun s -> s.ShardId = shardId)

type private ShardMsg =
    | UpdatedShardPosition of StreamPosition
    | EndOfShard of string
    | ShardsUpdated of Shard seq

type private ShardProcessingState =
    | NotStarted
    | Processing of StreamPosition
    | Completed

type private StreamState =
    { Shards: Shard list
      StreamProgress: Map<string, ShardProcessingState>
      ShardWorkers: Map<string, MailboxProcessor<unit>> }

module private Map =
    let union (replaceWith: Map<'K, 'V>) (original: Map<'K, 'V>) =
        Map.fold (fun acc key value -> Map.add key value acc) original replaceWith

module private StreamState =
    let empty = { Shards = []; StreamProgress = Map.empty; ShardWorkers = Map.empty }

    /// <summary>
    /// Initializes the stream state with the initial list of shards. Supports the following start positions:
    /// - Oldest: Start processing from the start of the oldest shards available (note 24h retention)
    /// - Newest: Start processing from the end of the currently open shards
    /// - Positions: Start processing from the given positions
    /// </summary>
    /// <param name="shards">List of shards in the stream</param>
    /// <param name="start">`ReadStreamFrom` start position</param>
    let init (shards: Shard seq) (start: ReadStreamFrom) =
        let shards = shards |> Seq.toList
        let progress =
            match start with
            | Oldest -> shards |> Seq.map (fun s -> (s.ShardId, NotStarted)) |> Map.ofSeq

            | Newest ->
                let openShards =
                    Shard.openShards shards
                    |> Seq.map (fun s ->
                        (s.ShardId,
                         Processing
                             { ShardId = s.ShardId
                               SequenceNumber = None }))
                    |> Map.ofSeq
                let closedShards =
                    shards
                    |> Seq.filter (fun s -> not (openShards |> Map.containsKey s.ShardId))
                    |> Seq.map (fun s -> (s.ShardId, Completed))
                    |> Map.ofSeq

                closedShards |> Map.union openShards

            | Positions pos ->
                let inProgress =
                    pos
                    |> Seq.filter (fun p -> Shard.containsShardId shards p.ShardId)
                    |> Seq.map (fun p -> (p.ShardId, Processing p))
                    |> Map.ofSeq
                let completed =
                    inProgress
                    |> Map.keys
                    |> Seq.collect (Shard.ancestors shards)
                    |> Seq.map (fun s -> (s, Completed))
                    |> Map.ofSeq
                let notStarted =
                    shards
                    |> Seq.filter (fun s -> not (inProgress |> Map.containsKey s.ShardId) && not (completed |> Map.containsKey s.ShardId))
                    |> Seq.map (fun s -> (s.ShardId, NotStarted))
                    |> Map.ofSeq
                inProgress |> Map.union completed |> Map.union notStarted

        { Shards = shards; StreamProgress = progress; ShardWorkers = Map.empty }

    /// <summary>
    /// Updates (appends to) the list of shards in the stream state
    /// </summary>
    /// <param name="shards">New list of shards</param>
    /// <param name="state">Current state of the stream</param>
    /// <returns>Updated state</returns>
    let updateShards (shards: Shard seq) (state: StreamState) =
        // TODO: Prune old ShardIds from StreamProgress. What should happen if the shard has disappeard but not been processed?
        let newProgress =
            shards
            |> Seq.filter (fun s -> not (state.StreamProgress |> Map.containsKey s.ShardId))
            |> Seq.map (fun s -> (s.ShardId, NotStarted))

        { state with
            Shards = shards |> Seq.toList
            StreamProgress = state.StreamProgress |> Map.toSeq |> Seq.append newProgress |> Map.ofSeq }

    /// <summary>
    /// Registers the new shard workers and marks the shards as processing if necessary
    /// </summary>
    /// <param name="shardId">ShardId of the newly started shard</param>
    /// <param name="worker">Shard worker</param>
    /// <param name="state">Current state of the stream</param>
    /// <returns>Updated state</returns>
    let startShards (workers: Map<String, MailboxProcessor<unit>>) (state: StreamState) =
        let updatedProgress =
            workers
            |> Map.map (fun s _ ->
                match state.StreamProgress |> Map.tryFind s with
                | Some NotStarted
                | None -> Processing { ShardId = s; SequenceNumber = None }
                | Some p -> p) // TODO: Log some asserts here (eg if completed)

        { state with
            StreamProgress = state.StreamProgress |> Map.union updatedProgress
            ShardWorkers = state.ShardWorkers |> Map.union workers }

    /// <summary>
    /// Updates the position of a shard in the stream
    /// </summary>
    /// <param name="position">New position of the shard</param>
    /// <param name="state">Current state of the stream</param>
    /// <returns>Updated state</returns>
    let updatedShardPosition (position: StreamPosition) (state: StreamState) =
        { state with
            StreamProgress = state.StreamProgress |> Map.add position.ShardId (Processing position) }

    /// <summary>
    /// Marks a shard as completed and removes the worker
    /// </summary>
    /// <param name="shardId">ShardId of the completed shard</param>
    /// <param name="state">Current state of the stream</param>
    /// <returns>Updated state</returns>
    let completeShard (shardId: string) (state: StreamState) =
        { state with
            StreamProgress = state.StreamProgress |> Map.add shardId Completed
            ShardWorkers = state.ShardWorkers |> Map.remove (shardId) }

    /// <summary>
    /// Returns all shards ready to be processed:
    /// - `NotStarted` with no parent shard or parent shard completed
    /// - `Processing` with no worker
    /// </summary>
    /// <param name="state">Current state of the stream</param>
    /// <returns>List of shards ready to start processing, with the sequence number to start from</returns>
    let nextShards (state: StreamState) =
        state.Shards
        |> List.choose (fun s ->
            match state.StreamProgress |> Map.tryFind s.ShardId with
            | Some NotStarted ->
                match state.StreamProgress |> Map.tryFind s.ParentShardId with
                | Some Completed
                | None -> Some(s, None)
                | Some NotStarted
                | Some(Processing _) -> None
            | Some(Processing pos) ->
                match state.ShardWorkers |> Map.tryFind s.ShardId with
                | Some _ -> None
                | None -> Some(s, pos.SequenceNumber)
            | _ -> None)


/// DynamoDB client object for interpreting Dynamo streams sharing the same F# record representations as the `TableContext`
[<Sealed; AutoSerializable(false)>]
type StreamContext<'TRecord> internal (client: IAmazonDynamoDBStreams, tableName: string, template: RecordTemplate<'TRecord>) =

    [<Literal>]
    let idleTimeBetweenReadsMilliseconds = 1000
    [<Literal>]
    let maxRecords = 1000
    [<Literal>]
    let listShardsCacheAgeMilliseconds = 10000

    let listStreams (ct: CancellationToken) : Task<string list> = task {
        let req = ListStreamsRequest(TableName = tableName, Limit = 100)
        let! response = client.ListStreamsAsync(req, ct)
        return response.Streams |> Seq.map _.StreamArn |> Seq.toList
    }

    let describeStream (streamArn: string) (ct: CancellationToken) = task {
        let! response = client.DescribeStreamAsync(DescribeStreamRequest(StreamArn = streamArn), ct)
        return response.StreamDescription
    }

    // TODO: what does this return if no more records in shard?
    let getShardIterator (streamArn: string) (position: StreamPosition) (ct: CancellationToken) = task {
        let iteratorType = if position.SequenceNumber.IsSome then ShardIteratorType.AFTER_SEQUENCE_NUMBER else ShardIteratorType.TRIM_HORIZON
        let req = GetShardIteratorRequest(StreamArn = streamArn, ShardId = position.ShardId, ShardIteratorType = iteratorType)
        position.SequenceNumber |> Option.iter (fun n -> req.SequenceNumber <- n)
        let! response = client.GetShardIteratorAsync(req, ct)
        return response.ShardIterator
    }

    let getRecords (iterator: string) (limit: int) (ct: CancellationToken) = task {
        let! response = client.GetRecordsAsync(GetRecordsRequest(Limit = limit, ShardIterator = iterator), ct)
        return
            response.Records |> Array.ofSeq,
            if isNull response.NextShardIterator then
                None
            else
                Some response.NextShardIterator
    }

    let startShardProcessor
        (outbox: MailboxProcessor<ShardMsg>)
        (streamArn: string)
        (position: StreamPosition)
        (processRecord: Record -> Task)
        (ct: CancellationToken)
        =
        MailboxProcessor<unit>
            .Start(
                (fun _ ->
                    let rec loop iterator = async {
                        if not ct.IsCancellationRequested then
                            // TODO: Handle AWS read failures
                            let! (recs, nextIterator) = getRecords iterator maxRecords ct |> Async.AwaitTaskCorrect
                            // TODO: Process Record error handling
                            for r in recs do
                                do! processRecord r |> Async.AwaitTaskCorrect

                            match nextIterator with
                            | None -> outbox.Post(EndOfShard position.ShardId)
                            // End the loop - done processing this shard
                            | Some i ->
                                outbox.Post(UpdatedShardPosition { ShardId = position.ShardId; SequenceNumber = Some i })
                                do! Async.Sleep idleTimeBetweenReadsMilliseconds
                                do! loop i
                    }

                    async {
                        let! initialIterator =
                            getShardIterator streamArn position ct |> Async.AwaitTaskCorrect
                        do! loop initialIterator
                    }),
                ct
            )

    let startShardSyncWorker (outbox: MailboxProcessor<ShardMsg>) (streamArn: string) (ct: CancellationToken) =
        MailboxProcessor<unit>
            .Start(
                (fun _ ->
                    let rec loop () = async {
                        if not ct.IsCancellationRequested then
                            // TODO: Handle AWS read failures
                            let! stream = describeStream streamArn ct |> Async.AwaitTaskCorrect
                            outbox.Post(ShardsUpdated stream.Shards)
                            do! Async.Sleep listShardsCacheAgeMilliseconds
                            do! loop ()
                    }

                    loop ()),
                ct
            )

    let startStreamProcessor (streamArn: string) (startPosition: ReadStreamFrom) (processRecord: Record -> Task) (ct: CancellationToken) =
        MailboxProcessor<ShardMsg>.Start(fun inbox ->
            let startShardWorker (shard: Shard, sequenceNumber: string option) =
                let position = { ShardId = shard.ShardId; SequenceNumber = sequenceNumber }
                (shard.ShardId, startShardProcessor inbox streamArn position processRecord ct)

            let rec loop (state: StreamState) = async {
                if not ct.IsCancellationRequested then
                    let! msg = inbox.Receive()
                    match msg with
                    | UpdatedShardPosition position ->
                        printfn "Recording current shard position: %0A" position
                        do! loop (state |> StreamState.updatedShardPosition position)
                    | EndOfShard shardId ->
                        let state = state |> StreamState.completeShard shardId
                        let newWorkers = state |> StreamState.nextShards |> Seq.map startShardWorker |> Map.ofSeq
                        newWorkers |> Map.iter (fun sId _ -> printfn "Starting worker for shard %s" sId)
                        do! loop (state |> StreamState.startShards newWorkers)
                    | ShardsUpdated shards ->
                        let state = state |> StreamState.updateShards shards
                        let newWorkers = state |> StreamState.nextShards |> Seq.map startShardWorker |> Map.ofSeq
                        newWorkers |> Map.iter (fun sId _ -> printfn "Starting worker for shard %s" sId)
                        do! loop (state |> StreamState.startShards newWorkers)
            }
            async {
                let! stream = describeStream streamArn ct |> Async.AwaitTaskCorrect
                let state = StreamState.init stream.Shards startPosition
                startShardSyncWorker inbox streamArn ct |> ignore // TODO: Store shard sync worker
                let workers = StreamState.nextShards state |> Seq.map startShardWorker |> Map.ofSeq
                workers |> Map.iter (fun sId _ -> printfn "Starting worker for shard %s" sId)
                do! loop (state |> StreamState.startShards workers)
            })

    /// DynamoDB streaming client instance used for the table operations
    member _.Client = client
    /// DynamoDB table name targeted by the context
    member _.TableName = tableName
    /// Record-induced table template
    member _.Template = template


    /// <summary>
    ///   Creates a DynamoDB stream processing instance for given F# record and table name.<br/>
    /// </summary>
    /// <param name="client">DynamoDB streaming client instance.</param>
    /// <param name="tableName">Table name to target.</param>
    new(client: IAmazonDynamoDBStreams, tableName: string) =
        if not <| isValidTableName tableName then
            invalidArg "tableName" "unsupported DynamoDB table name."
        StreamContext<'TRecord>(client, tableName, RecordTemplate.Define<'TRecord>())

    /// <summary>
    ///   Lists all the streams associated with the table.
    /// </summary>
    /// <param name="ct">Cancellation token for the operation.</param>
    member _.ListStreamsAsync(?ct: CancellationToken) = listStreams (ct |> Option.defaultValue CancellationToken.None)

    /// <summary>
    ///   Deserializes a DynamoDB stream record into the corresponding F# type (matching the table definition).
    ///   Intended to be used directly from Lambda event handlers or KCL consumers.
    /// </summary>
    /// <param name="record">DynamoDB stream record to deserialize.</param>
    /// <returns>`StreamRecord` with old value, new value, and/or `TableKey`, depending on stream config & operation</returns>
    member t.DeserializeStreamRecord(record: Record) : StreamRecord<'TRecord> =
        let op =
            match record.EventName with
            | n when n = OperationType.INSERT -> Insert
            | n when n = OperationType.MODIFY -> Modify
            | n when n = OperationType.REMOVE -> Remove
            | n -> failwithf "Unexpected OperationType %s" n.Value
        let key = t.Template.ExtractKey record.Dynamodb.Keys
        let newRec =
            if record.Dynamodb.NewImage.Count = 0 then
                None
            else
                Some(t.Template.OfAttributeValues record.Dynamodb.NewImage)
        let oldRec =
            if record.Dynamodb.OldImage.Count = 0 then
                None
            else
                Some(t.Template.OfAttributeValues record.Dynamodb.OldImage)
        { Operation = op
          TableKey = key
          ApproximateCreationDateTime = DateTimeOffset(record.Dynamodb.ApproximateCreationDateTime)
          New = newRec
          Old = oldRec }

    // TODO: Return/save checkpoint updates somehow
    /// <summary>
    /// Start a single-process Dynamo stream reader
    /// </summary>
    /// <param name="streamArn">ARN of the stream to read from</param>
    /// <param name="processRecord">Function to process each record</param>
    /// <param name="startPosition">Start position of the stream - defaults to `Newest`</param>
    /// <param name="ct">Cancellation token for the operation - defaults to `CancellationToken.None`</param>
    member internal t.StartReadingAsync
        (
            streamArn: string,
            processRecord: StreamRecord<'TRecord> -> Task,
            ?startPosition: ReadStreamFrom,
            ?ct: CancellationToken
        ) =
        startStreamProcessor
            streamArn
            (defaultArg startPosition Newest)
            (fun r -> processRecord (t.DeserializeStreamRecord r))
            (defaultArg ct CancellationToken.None)
        :> IDisposable
