#I "../../bin"
#r "AWSSDK.Core.dll"
#r "AWSSDK.DynamoDBv2.dll"
#r "FSharp.DynamoDB.dll"

open System

open Amazon
open Amazon.Util
open Amazon.DynamoDBv2
open Amazon.DynamoDBv2.Model

open FSharp.DynamoDB

let account = AWSCredentialsProfile.LoadFrom("default").Credentials
let ddb = new AmazonDynamoDBClient(account, RegionEndpoint.EUCentral1) :> IAmazonDynamoDB

type Test =
    {
        [<HashKey>]
        HashKey : string
        Value : bool
        Values : Map<int, string>
    }

let table = TableContext.GetTableContext<Test>(ddb, "test", createIfNotExists = true) |> Async.RunSynchronously

let value = { HashKey = "1" ; Value = false ; Values = Map.empty }

let key = table.PutItemAsync(value) |> Async.RunSynchronously

table.PutItemAsync({ value with Value = true}, <@ fun r -> r.Values.ContainsKey 42 |> not @>) |> Async.RunSynchronously