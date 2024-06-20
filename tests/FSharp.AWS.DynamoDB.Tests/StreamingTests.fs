namespace FSharp.AWS.DynamoDB.Tests

open System

open Swensen.Unquote
open Xunit

open FSharp.AWS.DynamoDB
open FSharp.AWS.DynamoDB.Scripting
open Amazon.DynamoDBv2

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
          IntValue = rand ()  }

    let (table, streams) = fixture.CreateEmptyWithStreaming<StreamingRecord>(StreamViewType.NEW_AND_OLD_IMAGES)

    [<Fact>]
    let ``Parse New Item from stream`` () =
        let value = mkItem ()
        let key = table.PutItem value
        let value' = table.GetItem key
        let task = streams.TempReadAllRecords()
        task.Wait()
        let records = task.Result
        test <@ 1 = Array.length records @>
        test <@ records[0].Operation = Insert @>
        test <@ records[0].Old = None @>
        test <@ records[0].New = Some value' @>
        
    interface IClassFixture<TableFixture>