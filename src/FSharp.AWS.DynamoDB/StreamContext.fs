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

type StreamReadFrom =
    | Oldest
    | Newest
    | Position of StreamPosition

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

type private ShardMsg = 
    | UpdatedShardPosition of StreamPosition 
    | EndOfShard of string 
    | ShardsUpdated of ShardTree list

type private StreamState = { Trees: ShardTree list; Workers: Map<string, MailboxProcessor<unit> * StreamPosition> }


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

    /// <summary>
    /// Returns the 'oldest' shard in a stream, according to the logic:
    /// First shard which has no parent ID
    /// First shard which has a parent ID not returned in the description
    /// First shard in the list
    /// </summary>
    /// <param name="stream">`StreamDescription` returned for the stream ARN</param>
    let oldestShard (stream: StreamDescription) =
        stream.Shards
        |> Seq.tryFind (fun s -> String.IsNullOrEmpty(s.ParentShardId))
        |> Option.orElseWith (fun () ->
            stream.Shards
            |> Seq.tryFind (fun s1 -> not (stream.Shards |> Seq.exists (fun s2 -> s1.ParentShardId = s2.ShardId))))
        |> Option.orElseWith (fun () -> stream.Shards |> Seq.tryHead)

    /// <summary>
    /// Returns the 'newest' shard in a stream, according to the logic:
    /// First shard which has no child IDs
    /// First shard which has an empty `EndingSequenceNumber` (is still being actively written to)
    /// First shard in the list
    /// </summary>
    /// <param name="stream">`StreamDescription` returned for the stream ARN</param>
    // TODO: Should this ONLY return active streams?
    let newestShard (stream: StreamDescription) =
        stream.Shards
        |> Seq.tryFind (fun s1 -> not (stream.Shards |> Seq.exists (fun s2 -> s1.ShardId = s2.ParentShardId)))
        |> Option.orElseWith (fun () ->
            stream.Shards
            |> Seq.tryFind (fun s -> String.IsNullOrEmpty(s.SequenceNumberRange.EndingSequenceNumber)))
        |> Option.orElseWith (fun () -> stream.Shards |> Seq.tryHead)

    /// <summary>
    /// Returns all of the currently open shards in the stream
    /// </summary>
    /// <param name="stream">`StreamDescription` returned for the stream ARN</param>
    let openShards (stream: StreamDescription) =
        stream.Shards |> Seq.filter (fun s -> String.IsNullOrEmpty(s.ParentShardId))
    
    /// <summary>
    /// Returns all of the root shards (with no parent) in the stream
    /// </summary>
    /// <param name="stream">`StreamDescription` returned for the stream ARN</param>
    let rootShards (stream : StreamDescription) =
        stream.Shards |> Seq.filter (fun s -> 
            String.IsNullOrEmpty(s.ParentShardId) 
                || not (stream.Shards |> Seq.exists (fun c -> c.ShardId = s.ParentShardId)))

    /// <summary>
    /// Build trees of the shard hierarchy in the stream.
    /// Nodes will usually have a single child, but can have multiple children if the shard has been split.
    /// </summary>
    /// <param name="stream">`StreamDescription` returned for the stream ARN</param>
    let shardTrees (stream: StreamDescription): ShardTree seq =
        let children = 
            stream.Shards 
                |> Seq.filter (fun s -> not (String.IsNullOrEmpty(s.ParentShardId)))
                |> Seq.groupBy _.ParentShardId 
                |> Map.ofSeq

        let rec buildTree (shard: Shard) =
            match children |> Map.tryFind shard.ShardId with
            | None -> Leaf shard
            | Some cs -> Node(shard, cs |> Seq.map buildTree |> Seq.toList)

        rootShards stream |> Seq.map buildTree
        


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
    
    let startStreamProcessor (streamArn : string) (processRecord: Record -> unit) (ct: CancellationToken) =
        MailboxProcessor<ShardMsg>.Start(fun inbox ->
            let startWorker (shard : Shard) =
                let position = { ShardId = shard.ShardId; SequenceNumber = None }
                (shard.ShardId, (startShardProcessor inbox streamArn position processRecord ct, position))

            let rec loop (state: StreamState) =
                async {
                    if not ct.IsCancellationRequested then
                        let! msg = inbox.Receive()
                        match msg with
                        | UpdatedShardPosition position ->
                            printfn "Recording current shard position: %0A" position
                            do! loop { state with Workers = state.Workers |> Map.change position.ShardId (Option.map (fun (p, _) -> (p, position))) }
                        | EndOfShard shardId ->
                            let newWorkers = ShardTree.nextShards shardId state.Trees |> List.map (ShardTree.shard >> startWorker)
                            do! loop { state with Workers = state.Workers |> Map.remove shardId |> Map.toList |> List.append newWorkers |> Map.ofList}
                        | ShardsUpdated trees ->
                            do! loop { state with Trees = trees } // TODO: Start new workers if necessary?
                }
            async {
                let! stream = describeStream streamArn ct |> Async.AwaitTaskCorrect
                let trees = shardTrees stream |> Seq.toList
                // Start workers at the root shards. 
                // TODO: Handle LATEST semantics or starting from a checkpoint
                let workers = trees |> Seq.map (ShardTree.shard >> startWorker) |> Map.ofSeq
                do! loop { Trees = trees; Workers = workers }
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
            match oldestShard stream with
            | Some shard ->
                let! iterator = getShardIterator stream.StreamArn ShardIteratorType.TRIM_HORIZON { ShardId = shard.ShardId; SequenceNumber = None } CancellationToken.None
                let! records = getRecords iterator maxRecords CancellationToken.None
                return records |> fst |> Array.map t.ParseStreamRecord
            | None ->
                return [||]
        }


    // member t.StartReadingAsync(streamArn: string, processRecord: StreamRecord -> unit, ?ct: CancellationToken) =
    //     task {
    //         let! stream = describeStream streamArn (ct |> Option.defaultValue CancellationToken.None)
    //         let shard = oldestShard stream |> Option.get
    //         let! iterator = getShardIterator streamArn ShardIteratorType.TRIM_HORIZON { ShardId = shard.ShardId; SequenceNumber = None } ct
    //         let rec readLoop (iterator: string) =
    //             let! (recs, nextIterator) = getRecords iterator 1000 ct
    //             recs |> Array.iter (fun r -> processRecord (t.ParseStreamRecord r))
    //             match nextIterator with
    //             | None -> ()
    //             | Some i -> readLoop i
    //         readLoop iterator
    //     }