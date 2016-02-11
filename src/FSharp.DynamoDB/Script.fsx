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
        [<HashKey; CustomName("ID")>]
        HashKey : string
        RangeKey : string
        Values : Map<int, string[]>
    }

type Test2 =
    {
        [<HashKey; CustomName("ID")>]
        Id : string

        Values : int list

        Date : DateTimeOffset

        TimeSpan : TimeSpan

        Boolean : bool
    }

let table = TableContext.GetTableContext<Test>(ddb, "test", createIfNotExists = true) |> Async.RunSynchronously

let table2 = table.WithRecordType<Test2> ()


let value = { Id = "42" ; Values = [1 .. 100]; Date = DateTimeOffset.Now; TimeSpan = TimeSpan.FromMinutes 2. ; Boolean = true }

let value' = table2.GetItemAsync("42") |> Async.RunSynchronously

value'.Date.ToLocalTime()