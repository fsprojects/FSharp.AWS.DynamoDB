namespace FSharp.AWS.DynamoDB.Tests

open System

open Swensen.Unquote
open Xunit

open FSharp.AWS.DynamoDB
open FSharp.AWS.DynamoDB.Scripting
open Amazon.DynamoDBv2
open System.Collections.Concurrent
open System.Threading.Tasks

[<AutoOpen>]
module StreamingTests =

    type StreamingRecord =
        { [<HashKey>]
          HashKey: string
          [<RangeKey>]
          RangeKey: string
          StringValue: string
          IntValue: int64 }

type ``Streaming Tests``(fixture: TableFixture) =

    let rand = let r = Random.Shared in fun () -> int64 <| r.Next()
    let mkItem () =
        { HashKey = guid ()
          RangeKey = guid ()
          StringValue = guid ()
          IntValue = rand () }

    let (table, stream) = fixture.CreateEmptyWithStreaming<StreamingRecord>(StreamViewType.NEW_AND_OLD_IMAGES)

    [<Fact>]
    let ``Parse New Item from stream`` () =
        task {
            let recordQueue = ConcurrentQueue<StreamRecord<StreamingRecord>>()
            let! streams = stream.ListStreamsAsync()
            let streamArn = streams |> Seq.head
            let value = mkItem ()
            let key = table.PutItem value
            let value' = table.GetItem key
            let processRecord r =
                recordQueue.Enqueue r
                Task.CompletedTask
            use disp = stream.StartReadingAsync(streamArn, processRecord)
            do! Async.Sleep 1000
            let records = recordQueue.ToArray()
            test <@ 1 = Array.length records @>
            test <@ records[0].Operation = Insert @>
            test <@ records[0].Old = None @>
            test <@ records[0].New = Some value' @>
        }
        
    interface IClassFixture<TableFixture>
