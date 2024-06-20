namespace FSharp.AWS.DynamoDB.Tests

open System
open System.IO

open FsCheck
open Swensen.Unquote
open Xunit

open FSharp.AWS.DynamoDB

open Amazon.DynamoDBv2
open Amazon.Runtime

[<AutoOpen>]
module Utils =

    let guid () = Guid.NewGuid().ToString("N")

    let getRandomTableName () = sprintf "fsdynamodb-%s" <| guid ()

    let shouldFailwith<'T, 'Exn when 'Exn :> exn> (f: unit -> 'T) = <@ f () |> ignore @> |> raises<'Exn>

    let getDynamoDBAccount () =
        let credentials = BasicAWSCredentials("Fake", "Fake")
        let config = AmazonDynamoDBConfig(ServiceURL = "http://localhost:8000")

        new AmazonDynamoDBClient(credentials, config) :> IAmazonDynamoDB

    let getStreamsAccount () =
        let credentials = BasicAWSCredentials("Fake", "Fake")
        let config = AmazonDynamoDBStreamsConfig(ServiceURL = "http://localhost:8000")

        new AmazonDynamoDBStreamsClient(credentials, config) :> IAmazonDynamoDBStreams

    type FsCheckGenerators =
        static member MemoryStream =
            Arb.generate<byte[] option>
            |> Gen.map (function
                | None -> null
                | Some bs -> new MemoryStream(bs))
            |> Arb.fromGen


    type TableFixture() =

        let client = getDynamoDBAccount ()
        let streamsClient = getStreamsAccount ()
        let tableName = getRandomTableName ()

        member _.Client = client
        member _.TableName = tableName

        member _.CreateEmpty<'TRecord>() =
            let throughput = ProvisionedThroughput(readCapacityUnits = 10L, writeCapacityUnits = 10L)
            Scripting.TableContext.Initialize<'TRecord>(client, tableName, Throughput.Provisioned throughput)

        member _.CreateEmptyWithStreaming<'TRecord>(streamType: StreamViewType) =
            let throughput = ProvisionedThroughput(readCapacityUnits = 10L, writeCapacityUnits = 10L)
            (Scripting.TableContext.Initialize<'TRecord>(client, tableName, Throughput.Provisioned throughput, Streaming.Enabled streamType),
             StreamContext<'TRecord>(streamsClient, tableName))

        interface IAsyncLifetime with
            member _.InitializeAsync() = System.Threading.Tasks.Task.CompletedTask
            member _.DisposeAsync() = client.DeleteTableAsync(tableName)
