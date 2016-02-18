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

type Nested = { A : string ; B : System.Reflection.BindingFlags }

type Test =
    {
        [<HashKey>]
        HashKey : string
        Value : int
        Value2 : int option
        Values : Nested
    }


let table = TableContext.GetTableContext<Test>(ddb, "test", createIfNotExists = true) |> Async.RunSynchronously

let value = { HashKey = "1" ; Value = 40 ; Value2 = None ; Values = { A = "τεστ" ; B = System.Reflection.BindingFlags.Instance } }

let key = table.PutItemAsync(value) |> Async.RunSynchronously

table.GetItemAsync key |> Async.RunSynchronously
table.PutItemAsync({ value with Value2 = None}, <@ fun r -> r.Values.A = "τεστ" @>) |> Async.RunSynchronously

table.UpdateItemAsync(key, <@ fun r -> { r with Values = r.Values + set [42] } @>) |> Async.RunSynchronously


<@ fun (r:Test) -> { r with Value = not r.Value ; HashKey = "xa" } @>