namespace FSharp.DynamoDB.Tests

open System
open System.Threading

open Xunit
open FsUnit.Xunit

open FSharp.DynamoDB

type SimpleRecord =
    {
        [<HashKey>]
        Name : string

        Value : int64
    }

type CompatibleRecord =
    {
        [<HashKey; CustomName("Name")>]
        Id : string

        Values : int list
    }

type ``Simple Table Operation Tests`` () =

    let client = getDynamoDBAccount()
    let tableName = getRandomTableName()

    let run = Async.RunSynchronously

    let table = TableContext.GetTableContext<SimpleRecord>(client, tableName, createIfNotExists = true) |> run

    [<Fact>]
    let ``Simple Put Operation`` () =
        let value = { Name = guid() ; Value = 42L }
        let key = table.PutItemAsync value |> run
        let value' = table.GetItemAsync key |> run
        value' |> should equal value

    [<Fact>]
    let ``ContainsKey Operation`` () =
        let value = { Name = guid() ; Value = 42L }
        let key = table.PutItemAsync value |> run
        table.ContainsKeyAsync key |> run |> should equal true

    [<Fact>]
    let ``Batch Put Operation`` () =
        let values = set [ for i in 1L .. 20L -> { Name = guid() ; Value = i } ]
        let keys = table.PutItemsAsync values |> run
        let values' = table.GetItemsAsync keys |> run |> Set.ofArray
        values' |> should equal values

    [<Fact>]
    let ``Simple Delete Operation`` () =
        let item = { Name = guid() ; Value = 0L }
        let key = table.PutItemAsync item |> run
        table.ContainsKeyAsync key |> run |> should equal true
        table.DeleteItemAsync key |> run
        Thread.Sleep 5000
        table.ContainsKeyAsync key |> run |> should equal false

    interface IDisposable with
        member __.Dispose() =
            ignore <| client.DeleteTable(tableName)