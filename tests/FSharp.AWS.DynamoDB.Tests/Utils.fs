namespace FSharp.AWS.DynamoDB.Tests

open System
open System.IO
open System.Collections.Generic

open FsCheck
open Swensen.Unquote
open Xunit

open FSharp.AWS.DynamoDB

open Amazon.DynamoDBv2
open Amazon.DynamoDBv2.Model
open Amazon.Runtime

[<AutoOpen>]
module Utils =
    open System.Diagnostics

    let guid () = Guid.NewGuid().ToString("N")

    let getRandomTableName () = sprintf "fsdynamodb-%s" <| guid ()

    let shouldFailwith<'T, 'Exn when 'Exn :> exn> (f: unit -> 'T) = <@ f () |> ignore @> |> raises<'Exn>

    let getDynamoDBAccount () =
        let credentials = BasicAWSCredentials("Fake", "Fake")
        let config = AmazonDynamoDBConfig(ServiceURL = "http://localhost:8000")

        new AmazonDynamoDBClient(credentials, config) :> IAmazonDynamoDB


    let clearAttribute (table: TableContext<'T>) (key: TableKey) (attribute: string) =
        let keyAttr = table.Template.ToAttributeValues key
        table.Client.UpdateItemAsync(
            new UpdateItemRequest(
                TableName = table.TableName,
                Key = keyAttr,
                AttributeUpdates = Dictionary(Map.ofSeq [ attribute, new AttributeValueUpdate(Action = AttributeAction.DELETE) ])
            )
        )
        |> Async.AwaitTask
        |> Async.RunSynchronously
        |> ignore

    type FsCheckGenerators =
        static member MemoryStream =
            Arb.generate<byte[] option>
            |> Gen.map (function
                | None -> null
                | Some bs -> new MemoryStream(bs))
            |> Arb.fromGen


    type TableFixture() =

        let client = getDynamoDBAccount ()
        let tableName = getRandomTableName ()
        let activities = new Collections.Concurrent.ConcurrentBag<Activity>()

        member _.Client = client
        member _.TableName = tableName

        member _.CreateEmpty<'TRecord>() =
            let throughput = ProvisionedThroughput(readCapacityUnits = 10L, writeCapacityUnits = 10L)
            let listener = new ActivityListener()
            listener.ShouldListenTo <- fun s -> s.Name = "FSharp.AWS.DynamoDB"
            listener.Sample <- fun _ -> ActivitySamplingResult.AllData
            listener.ActivityStopped <- fun a -> activities.Add(a)
            ActivitySource.AddActivityListener(listener)
            Scripting.TableContext.Initialize<'TRecord>(client, tableName, Throughput.Provisioned throughput)

        interface IAsyncLifetime with
            member _.InitializeAsync() = System.Threading.Tasks.Task.CompletedTask
            member _.DisposeAsync() = client.DeleteTableAsync(tableName)
