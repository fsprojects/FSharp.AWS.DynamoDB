namespace FSharp.AWS.DynamoDB.Tests

open System
open System.IO

open Xunit
open FsCheck

open Amazon
open Amazon.Util
open Amazon.DynamoDBv2
open Amazon.Runtime

[<AutoOpen>]
module Utils =

    let getRandomTableName() =
        sprintf "fsdynamodb-%s" <| System.Guid.NewGuid().ToString("N")

    let guid() = Guid.NewGuid().ToString("N")

    let shouldFailwith<'T, 'Exn when 'Exn :> exn>(f : unit -> 'T) =
        ignore <| Assert.Throws<'Exn>(f >> ignore)

    let getEnvironmentVariable (envName:string) =
        let aux found target =
            if String.IsNullOrWhiteSpace found then Environment.GetEnvironmentVariable(envName, target)
            else found

        Array.fold aux null [|EnvironmentVariableTarget.Process; EnvironmentVariableTarget.User; EnvironmentVariableTarget.Machine|]
        
    let getEnvironmentVariableOrDefault envName defaultValue = 
        match getEnvironmentVariable envName with
        | null | "" -> defaultValue
        | ev -> ev

    let getTestRegion () = 
        match getEnvironmentVariable "fsddbtestsregion" with
        | null | "" -> RegionEndpoint.EUCentral1
        | region -> RegionEndpoint.GetBySystemName region

    let getAWSProfileName () = getEnvironmentVariableOrDefault "fsddbtestsprofile" "default"
    let tryGetAWSCredentials () = 
        match getEnvironmentVariable "fsddbtestscreds" with
        | null | "" -> None
        | creds -> 
            let toks = creds.Split(',')
            let creds = new BasicAWSCredentials(toks.[0], toks.[1]) :> Amazon.Runtime.AWSCredentials
            Some creds

    let getAWSCredentials () =
        let region = getTestRegion()
        match tryGetAWSCredentials() with
        | Some creds -> creds
        | None ->
            let profile = getAWSProfileName()
            AWSCredentialsProfile.LoadFrom(profile).Credentials :> AWSCredentials

    let getDynamoDBAccount () =
        let creds = getAWSCredentials()
        let region = getTestRegion()
        new AmazonDynamoDBClient(creds, region) :> IAmazonDynamoDB


    type FsCheckGenerators =
        static member MemoryStream = 
            Arb.generate<byte[] option>
            |> Gen.map (function None -> null | Some bs -> new MemoryStream(bs))
            |> Arb.fromGen