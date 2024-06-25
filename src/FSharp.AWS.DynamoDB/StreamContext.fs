namespace FSharp.AWS.DynamoDB

open System
open System.Collections.Concurrent
open System.Threading
open System.Threading.Tasks

open Amazon.DynamoDBv2
open Amazon.DynamoDBv2.Model
open System.Collections.Generic

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

type private Enumerator<'TRecord>(initialIterator : string, fetchNext: string -> Task<('TRecord [] * string option)>) =
    let mutable iterator = initialIterator
    let mutable records: 'TRecord [] = Array.empty
    let mutable index = 0

    interface IAsyncEnumerator<'TRecord> with
        member _.Current = records[index]

        member _.MoveNextAsync() =
            if index < records.Length - 1 then
                index <- index + 1
                ValueTask<bool>(true)
            else
                let nextTask = 
                    (fetchNext iterator)
                        .ContinueWith((fun (task: Task<('TRecord [] * string option)>) ->
                            match task.Result with
                            | (_, None) ->
                                records <- Array.empty
                                index <- 0
                                false
                            | (recs, Some nextIterator) ->
                                records <- recs
                                iterator <- nextIterator
                                index <- 0
                                true
                        ), TaskContinuationOptions.OnlyOnRanToCompletion)
                ValueTask<bool>(nextTask)

        member _.DisposeAsync() = ValueTask()

type private ShardTree =
    | Leaf of Shard
    | Node of Shard * ShardTree list

module private ShardTree =
    let shard (tree : ShardTree) =
        match tree with
        | Leaf shard | Node (shard, _) -> shard

    let find (shardId : string) (tree : ShardTree) = 
        let rec findInner t =
            match t with
            | Leaf s | Node (s, _) when s.ShardId = shardId -> Some(t)
            | Node (_, children) -> children |> Seq.choose (findInner) |> Seq.tryHead
            | Leaf _ -> None
            
        findInner tree

    let nextShards (shardId : string) (trees : ShardTree list) =
        match trees |> List.choose (find shardId) |> List.tryHead with
        | Some (Node (_, children)) -> children
        | _ -> []

module private Shard =
    /// <summary>
    /// Returns the 'oldest' shard in a stream, according to the logic:
    /// First shard which has no parent ID
    /// First shard which has a parent ID not returned in the description
    /// First shard in the list
    /// </summary>
    /// <param name="shards">sequence of `Shard` objects from the `StreamDescription`</param>
    let oldest (shards : Shard seq) =
        shards
        |> Seq.tryFind (fun s -> String.IsNullOrEmpty(s.ParentShardId))
        |> Option.orElseWith (fun () ->
            shards
            |> Seq.tryFind (fun s1 -> not (shards |> Seq.exists (fun s2 -> s1.ParentShardId = s2.ShardId))))
        |> Option.orElseWith (fun () -> shards |> Seq.tryHead)

    /// <summary>
    /// Returns the 'newest' shard in a stream, according to the logic:
    /// First shard which has no child IDs
    /// First shard which has an empty `EndingSequenceNumber` (is still being actively written to)
    /// First shard in the list
    /// </summary>
    /// <param name="shards">sequence of `Shard` objects from the `StreamDescription`</param>
    // TODO: Should this ONLY return active streams?
    let newest (shards : Shard seq) =
        shards
        |> Seq.tryFind (fun s1 -> not (shards |> Seq.exists (fun s2 -> s1.ShardId = s2.ParentShardId)))
        |> Option.orElseWith (fun () ->
            shards
            |> Seq.tryFind (fun s -> String.IsNullOrEmpty(s.SequenceNumberRange.EndingSequenceNumber)))
        |> Option.orElseWith (fun () -> shards |> Seq.tryHead)
    
    /// <summary>
    /// Returns all of the root shards (with no parent) in the stream
    /// </summary>
    /// <param name="stream">`StreamDescription` returned for the stream ARN</param>
    let roots (shards : Shard seq) =
        shards |> Seq.filter (fun s -> 
            String.IsNullOrEmpty(s.ParentShardId) 
                || not (shards |> Seq.exists (fun c -> c.ShardId = s.ParentShardId)))

    /// <summary>
    /// Returns all of the leaf shards (with no children) in the stream
    /// </summary>
    /// <param name="stream">`StreamDescription` returned for the stream ARN</param>
    let leaves (shards : Shard seq) =
        shards |> Seq.filter (fun s -> not (shards |> Seq.exists (fun c -> c.ParentShardId = s.ShardId)))

    let ancestors (shardId : string) (shards : Shard seq) =
        let rec ancestorsInner (shardId : string) =
            match shards |> Seq.tryFind (fun s -> s.ShardId = shardId) with
            | None -> []
            | Some s -> 
                match s.ParentShardId with
                | null | "" -> []
                | parent -> parent :: ancestorsInner parent

        ancestorsInner shardId

    /// <summary>
    /// Build trees of the shard hierarchy in the stream.
    /// Nodes will usually have a single child, but can have multiple children if the shard has been split.
    /// </summary>
    /// <param name="shards">sequence of `Shard` objects from the `StreamDescription`</param>
    let toTrees (shards : Shard seq): ShardTree list =
        let children = 
            shards 
                |> Seq.filter (fun s -> not (String.IsNullOrEmpty(s.ParentShardId)))
                |> Seq.groupBy _.ParentShardId 
                |> Map.ofSeq

        let rec buildTree (shard: Shard) =
            match children |> Map.tryFind shard.ShardId with
            | None -> Leaf shard
            | Some cs -> Node(shard, cs |> Seq.map buildTree |> Seq.toList)

        roots shards |> Seq.map buildTree |> Seq.toList
        

type private ShardMsg = 
    | UpdatedShardPosition of StreamPosition 
    | EndOfShard of string 
    | ShardsUpdated of Shard seq

type private ShardProcessingState = 
    | NotStarted
    | Processing of StreamPosition
    | Completed

type private StreamState = { Shards: Shard list; StreamProgress: Map<string, ShardProcessingState>; ShardWorkers: Map<string, MailboxProcessor<unit>> }

module private Map =
    let union (replaceWith: Map<'K, 'V>) (original: Map<'K, 'V>) =
         Map.fold (fun acc key value -> Map.add key value acc) original replaceWith

module private StreamState =
    let empty = { Shards = []; StreamProgress = Map.empty; ShardWorkers = Map.empty }

    /// <summary>
    /// Initializes the stream state with the initial list of shards. Supports the following start positions:
    /// - Oldest: Start processing from the start of the oldest shards available (note 24h retention)
    /// - Newest: Start processing from the start of the currently open shards
    /// - Positions: Start processing from the given positions (not currently supported)
    /// </summary>
    /// <param name="shards">List of shards in the stream</param>
    /// <param name="start">`ReadStreamFrom` start position</param>
    let init (shards: Shard seq) (start : ReadStreamFrom) =
        let shards = shards |> Seq.toList
        let progress = 
            match start with
            | Oldest -> shards |> Seq.map (fun s -> (s.ShardId, NotStarted)) |> Map.ofSeq
            | Newest -> 
                let leaves = Shard.leaves shards |> Seq.map (fun s -> (s.ShardId, NotStarted)) // TODO: Check to see if stream is closed
                let oldShards = shards |> Seq.filter (fun s -> not (leaves |> Seq.exists (fun (id, _) -> id = s.ShardId))) |> Seq.map (fun s -> (s.ShardId, Completed))
                leaves |> Seq.append oldShards |> Map.ofSeq
            | Positions pos ->
                failwithf "Not supported yet"

        { Shards = shards; StreamProgress = progress; ShardWorkers = Map.empty }

    /// <summary>
    /// Updates (appends to) the list of shards in the stream state
    /// </summary>
    /// <param name="shards">New list of shards</param>
    /// <param name="state">Current state of the stream</param>
    /// <returns>Updated state</returns>
    let updateShards (shards: Shard seq) (state: StreamState) =
        let newProgress = shards |> Seq.filter (fun s -> not (state.StreamProgress |> Map.containsKey s.ShardId)) 
                                 |> Seq.map (fun s -> (s.ShardId, NotStarted))

        { state with Shards = shards |> Seq.toList; StreamProgress = state.StreamProgress |> Map.toSeq |> Seq.append newProgress |> Map.ofSeq }

    /// <summary>
    /// Registers a new shard worker and marks the shard as processing
    /// </summary>
    /// <param name="shardId">ShardId of the newly started shard</param>
    /// <param name="worker">Shard worker</param>
    /// <param name="state">Current state of the stream</param>
    /// <returns>Updated state</returns>
    let startShard (shardId: string) (worker: MailboxProcessor<unit>) (state: StreamState) =
        { state with StreamProgress = state.StreamProgress |> Map.add shardId (Processing { ShardId = shardId; SequenceNumber = None });  ShardWorkers = state.ShardWorkers |> Map.add shardId worker }

    /// <summary>
    /// Registers the new shard workers and marks the shards as processing
    /// </summary>
    /// <param name="shardId">ShardId of the newly started shard</param>
    /// <param name="worker">Shard worker</param>
    /// <param name="state">Current state of the stream</param>
    /// <returns>Updated state</returns>
    let startShards (workers: Map<String, MailboxProcessor<unit>>) (state: StreamState) =
        let updatedProgress = workers |> Map.map (fun sId _ -> Processing { ShardId = sId; SequenceNumber = None })
        { state with StreamProgress = state.StreamProgress |> Map.union updatedProgress ;  ShardWorkers = state.ShardWorkers |> Map.union workers }

    /// <summary>
    /// Updates the position of a shard in the stream
    /// </summary>
    /// <param name="position">New position of the shard</param>
    /// <param name="state">Current state of the stream</param>
    /// <returns>Updated state</returns>
    let updatedShardPosition (position: StreamPosition) (state: StreamState) =
        { state with StreamProgress = state.StreamProgress |> Map.add position.ShardId (Processing position) }

    let completeShard (shardId: string) (state: StreamState) =
        { state with StreamProgress = state.StreamProgress |> Map.add shardId Completed; ShardWorkers = state.ShardWorkers |> Map.remove(shardId) }

    /// <summary>
    /// Returns all shards ready to be processed - with no parent shard or parent shard completed
    /// </summary>
    /// <param name="state">Current state of the stream</param>
    /// <returns>List of shards ready to start processing</returns>
    let nextShards (state : StreamState) =
        state.Shards 
        |> List.filter (fun s -> state.StreamProgress |> Map.tryFind s.ShardId |> Option.exists ((=) NotStarted))
        |> List.filter (fun s ->
            // Filter shards with a parent that hasn’t been completed
            match state.StreamProgress |> Map.tryFind s.ParentShardId with 
            | Some Completed | None -> true 
            | Some NotStarted | Some (Processing _) -> false)
    
/// DynamoDB client object for intrepreting Dynamo streams sharing the same F# record representations as the `TableContext`
[<Sealed; AutoSerializable(false)>]
type StreamContext<'TRecord> internal (client: IAmazonDynamoDBStreams, tableName: string, template: RecordTemplate<'TRecord>) =

    [<Literal>] 
    let idleTimeBetweenReadsMilliseconds = 1000
    [<Literal>] 
    let maxRecords = 1000
    [<Literal>]
    let listShardsCacheAgeMilliseconds = 10000

    let listStreams (ct: CancellationToken) : Task<string list> =
        task {
            let req = ListStreamsRequest(TableName = tableName, Limit = 100)
            let! response = client.ListStreamsAsync(req, ct)
            return response.Streams |> Seq.map _.StreamArn |> Seq.toList
        }

    let describeStream (streamArn: string) (ct: CancellationToken) =
        task {
            let! response = client.DescribeStreamAsync(DescribeStreamRequest(StreamArn = streamArn), ct)
            return response.StreamDescription
        }


    // TODO: what does this return if no more records in shard?
    let getShardIterator (streamArn: string) (iteratorType: ShardIteratorType) (position: StreamPosition) (ct: CancellationToken) =
        task {
            let req = GetShardIteratorRequest(
                    StreamArn = streamArn,
                    ShardId = position.ShardId,
                    ShardIteratorType = iteratorType)
            position.SequenceNumber |> Option.iter (fun n -> req.SequenceNumber <- n)
            let! response = client.GetShardIteratorAsync(req, ct)
            return response.ShardIterator
        }

    let getRecords (iterator: string) (limit: int) (ct: CancellationToken) =
        task {
            let! response = client.GetRecordsAsync(GetRecordsRequest(Limit = limit, ShardIterator = iterator), ct)
            return response.Records |> Array.ofSeq, if isNull response.NextShardIterator then None else Some response.NextShardIterator
        }

    let getRecordsAsync (iterator: string) (ct: CancellationToken) =
        let batchSize = 5
        { new IAsyncEnumerable<Record> with
            member _.GetAsyncEnumerator(ct) = Enumerator<Record>(iterator, (fun i -> getRecords i batchSize ct)) :> IAsyncEnumerator<Record> }

    let startShardProcessor (outbox: MailboxProcessor<ShardMsg>) (streamArn : string) (position : StreamPosition) (processRecord: Record -> unit) (ct: CancellationToken) =
        MailboxProcessor<unit>.Start((fun _ ->
            let rec loop iterator =
                async {
                    if not ct.IsCancellationRequested then
                        // TODO: Handle AWS read failures
                        let! (recs, nextIterator) = getRecords iterator maxRecords ct |> Async.AwaitTaskCorrect
                        // TODO: Process Record error handling
                        recs |> Array.iter processRecord
                        match nextIterator with
                        | None -> 
                            outbox.Post(EndOfShard position.ShardId)
                            // End processing the shard
                        | Some i -> 
                            outbox.Post(UpdatedShardPosition { ShardId = position.ShardId; SequenceNumber = Some i })
                            do! Async.Sleep idleTimeBetweenReadsMilliseconds
                            do! loop i
                }

            async {
                let! initialIterator = getShardIterator streamArn ShardIteratorType.TRIM_HORIZON position ct |> Async.AwaitTaskCorrect
                do! loop initialIterator
                while not ct.IsCancellationRequested do
                    do! Async.Sleep idleTimeBetweenReadsMilliseconds
            }
        ), ct)

    let startShardSyncWorker (outbox: MailboxProcessor<ShardMsg>) (streamArn : string) (ct : CancellationToken) =
        MailboxProcessor<unit>.Start((fun _ ->
            let rec loop () =
                async {
                    if not ct.IsCancellationRequested then
                        // TODO: Handle AWS read failures
                        let! stream = describeStream streamArn ct |> Async.AwaitTaskCorrect
                        outbox.Post(ShardsUpdated stream.Shards)
                        do! Async.Sleep listShardsCacheAgeMilliseconds
                        do! loop ()
                }

            loop ()
        ), ct)
    
    let startStreamProcessor (streamArn : string) (startPosition : ReadStreamFrom) (processRecord: Record -> unit) (ct: CancellationToken) =
        MailboxProcessor<ShardMsg>.Start(fun inbox ->
            let startShardWorker (shard : Shard) =
                let position = { ShardId = shard.ShardId; SequenceNumber = None } // TODO: Supply sequence number
                (shard.ShardId, startShardProcessor inbox streamArn position processRecord ct)

            let rec loop (state: StreamState) =
                async {
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
            }
        )
        
    /// DynamoDB streaming client instance used for the table operations
    member _.Client = client
    /// DynamoDB table name targeted by the context
    member _.TableName = tableName
    /// Record-induced table template
    member _.Template = template


    /// <summary>
    ///     Creates a DynamoDB client instance for given F# record and table name.<br/>
    ///     For creating, provisioning or verification, see <c>VerifyOrCreateTableAsync</c> and <c>VerifyTableAsync</c>.
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
    member _.ListStreamsAsync(?ct : CancellationToken) =
        listStreams(ct |> Option.defaultValue CancellationToken.None)

    /// <summary>
    ///   Parses a DynamoDB stream record into the corresponding F# type (matching the table definition).
    ///   Intended to be used directly from Lambda event handlers or Kinesis consumers.
    /// </summary>
    /// <param name="record">DynamoDB stream record to parse.</param>
    /// <returns>`StreamRecord` with old value, new value, and/or `TableKey`, depending on stream config & operation</returns>
    member t.ParseStreamRecord(record: Record) : StreamRecord<'TRecord> =
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

    member t.TempReadAllRecords() =
        task {
            let! streams = listStreams CancellationToken.None
            let! stream = describeStream (streams |> List.head) CancellationToken.None
            match Shard.oldest stream.Shards with
            | Some shard ->
                let! iterator = getShardIterator stream.StreamArn ShardIteratorType.TRIM_HORIZON { ShardId = shard.ShardId; SequenceNumber = None } CancellationToken.None
                let! records = getRecords iterator maxRecords CancellationToken.None
                return records |> fst |> Array.map t.ParseStreamRecord
            | None ->
                return [||]
        }


    member t.StartReadingAsync(streamArn: string, processRecord: StreamRecord<'TRecord> -> unit, ?startPosition : ReadStreamFrom, ?ct: CancellationToken) =
        task {
            let processor = startStreamProcessor streamArn (defaultArg startPosition Newest) (fun r -> processRecord (t.ParseStreamRecord r)) (ct |> Option.defaultValue CancellationToken.None)
            return ()
        }
