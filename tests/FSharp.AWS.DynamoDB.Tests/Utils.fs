namespace FSharp.AWS.DynamoDB.Tests

open System
open System.IO

open Xunit
open FsCheck

open Amazon
open Amazon.Util
open Amazon.DynamoDBv2
open Amazon.Runtime

open FSharp.AWS.DynamoDB

[<AutoOpen>]
module Utils =

    let getRandomTableName() =
        sprintf "fsdynamodb-%s" <| System.Guid.NewGuid().ToString("N")

    let guid() = Guid.NewGuid().ToString("N")

    let shouldFailwith<'T, 'Exn when 'Exn :> exn>(f : unit -> 'T) =
        ignore <| Assert.Throws<'Exn>(f >> ignore)

    let getTestRegion () = 
        match Environment.ResolveEnvironmentVariable "AWS_REGION" with
        | null | "" -> RegionEndpoint.EUCentral1
        | region -> RegionEndpoint.GetBySystemName region

    let getAWSProfileName () = 
        match Environment.ResolveEnvironmentVariable "AWS_CREDENTIAL_STORE_PROFILE" with
        | null | "" -> "default"
        | pf -> pf

    let getAWSCredentials () = 
        try AWSCredentials.FromEnvironmentVariables()
        with _ -> AWSCredentials.FromCredentialsStore(getAWSProfileName())

    let getDynamoDBAccount () =
        let creds = getAWSCredentials()
        let region = getTestRegion()
        new AmazonDynamoDBClient(creds, region) :> IAmazonDynamoDB


    type FsCheckGenerators =
        static member MemoryStream = 
            Arb.generate<byte[] option>
            |> Gen.map (function None -> null | Some bs -> new MemoryStream(bs))
            |> Arb.fromGen


    type TableFixture() =
        let client = getDynamoDBAccount()
        let tableName = getRandomTableName()
        member __.Client = client
        member __.TableName = tableName

        interface IDisposable with
            member __.Dispose() =
                client.DeleteTableAsync(tableName).RunSynchronously()