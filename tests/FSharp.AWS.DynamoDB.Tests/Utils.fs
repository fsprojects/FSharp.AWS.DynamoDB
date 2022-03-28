namespace FSharp.AWS.DynamoDB.Tests

open System
open System.IO

open FsCheck
open Swensen.Unquote

open FSharp.AWS.DynamoDB

open Amazon.DynamoDBv2
open Amazon.Runtime
open Xunit

[<AutoOpen>]
module Utils =

    let getRandomTableName() =
        sprintf "fsdynamodb-%s" <| Guid.NewGuid().ToString("N")

    let guid() = Guid.NewGuid().ToString("N")

    let shouldFailwith<'T, 'Exn when 'Exn :> exn>(f : unit -> 'T) =
        <@ f () |>  ignore @>
        |> raises<'Exn>

    let getDynamoDBAccount () =
        let credentials = BasicAWSCredentials("Fake", "Fake")
        let config = AmazonDynamoDBConfig()
        config.ServiceURL <- "http://localhost:8000"
        new AmazonDynamoDBClient(credentials, config) :> IAmazonDynamoDB


    type FsCheckGenerators =
        static member MemoryStream =
            Arb.generate<byte[] option>
            |> Gen.map (function None -> null | Some bs -> new MemoryStream(bs))
            |> Arb.fromGen


    type TableFixture() =
        let client = getDynamoDBAccount()
        let tableName = getRandomTableName()
        member _.Client = client
        member _.TableName = tableName

        member _.CreateContextAndTableIfNotExists<'TRecord>() =
            let createThroughput = ProvisionedThroughput(10L, 10L)
            Scripting.TableContext.Initialize<'TRecord>(client, tableName, createThroughput)

        interface IAsyncLifetime with
            member _.InitializeAsync() =
                System.Threading.Tasks.Task.CompletedTask
            member _.DisposeAsync() =
                client.DeleteTableAsync(tableName)
