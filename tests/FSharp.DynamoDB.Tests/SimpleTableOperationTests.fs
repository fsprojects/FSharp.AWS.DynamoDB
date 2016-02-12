namespace FSharp.DynamoDB.Tests

open System

open Xunit
open FsUnit.Xunit

open FSharp.DynamoDB

type SimpleRecord =
    {
        [<HashKey>]
        Name : string

        Value : int64
    }

type ``Simple Table Operation Tests`` () =

    let client = getDynamoDBAccount()
    let tableName = getRandomTableName()

    let run = Async.RunSynchronously

    let table = TableContext.GetTableContext<SimpleRecord>(client, tableName, createIfNotExists = true) |> run

//    [<Fact>]
    let ``Simple Put Operation`` () =
        let value = { Name = guid() ; Value = 42L }
        let key = table.PutItemAsync value |> run
        let value' = table.GetItemAsync key |> run
        value' |> should equal value

    interface IDisposable with
        member __.Dispose() =
            ignore <| client.DeleteTable(tableName)