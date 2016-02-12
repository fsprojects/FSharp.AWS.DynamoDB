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

type Test3 =
    {
        [<HashKey; CustomName("ID")>]
        Id : string
    
        TimeSpan : Nullable<TimeSpan>
    }

[<HashKeyConstant("ID", "42")>]
type Test4 =
    {
        [<RangeKey>]
        Value : Nullable<int>
    }

let table = TableContext.GetTableContext<Test3>(ddb, "test", createIfNotExists = true) |> Async.RunSynchronously

table.KeySchema

table.WithRecordType<Test4> ()

open FSharp.DynamoDB.TypeShape

shapeof<Nullable<int>>

let table' = TableContext.GetTableContext<Test4>(ddb, "test2", createIfNotExists = true) |> Async.RunSynchronously

table'.PutItemAsync({ Value = 41}) |> Async.RunSynchronously

let values = [1 .. 25] |> List.map (fun i -> { Id = string i ; TimeSpan = TimeSpan.FromMinutes(float i) })

values |> Seq.length

table.PutItemsAsync(values) |> Async.RunSynchronously

let keys = values |> List.map table.ExtractKey
table.GetItemsAsync(keys) |> Async.RunSynchronously |> Seq.length

table.DeleteItemsAsync(keys) |> Async.RunSynchronously