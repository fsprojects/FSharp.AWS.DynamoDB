namespace FSharp.AWS.DynamoDB.Tests

open System
open System.Threading

open Xunit
open FsUnit.Xunit

open FSharp.AWS.DynamoDB

[<AutoOpen>]
module SimpleTableTypes =

    type SimpleRecord =
        {
            [<HashKey>]
            HashKey : string
            [<RangeKey>]
            RangeKey : string

            Value : int64

            Tuple : int64 * int64

            Map : Map<string, int64>

            Unions : Choice<string, int64, byte[]> list
        }

    [<ConstantHashKeyAttribute("HashKey", "compatible")>]
    type CompatibleRecord =
        {
            [<RangeKey; CustomName("RangeKey")>]
            Id : string

            Values : Set<int>
        }

    [<ConstantRangeKeyAttribute("RangeKey", "compatible")>]
    type CompatibleRecord2 =
        {
            [<HashKey; CustomName("HashKey")>]
            Id : string

            Values : Set<int>
        }

type ``Simple Table Operation Tests`` () =

    let client = getDynamoDBAccount()
    let tableName = getRandomTableName()

    let rand = let r = Random() in fun () -> int64 <| r.Next()
    let mkItem() = 
        { 
            HashKey = guid() ; RangeKey = guid() ; 
            Value = rand() ; Tuple = rand(), rand() ;
            Map = seq { for i in 0L .. rand() % 5L -> "K" + guid(), rand() } |> Map.ofSeq 
            Unions = [Choice1Of3 (guid()) ; Choice2Of3(rand()) ; Choice3Of3(Guid.NewGuid().ToByteArray())]
        }

    let table = TableContext.Create<SimpleRecord>(client, tableName, createIfNotExists = true)

    [<Fact>]
    let ``Convert to compatible table`` () =
        let table' = table.WithRecordType<CompatibleRecord> ()
        table'.KeySchema |> should equal table.KeySchema

    [<Fact>]
    let ``Convert to compatible table 2`` () =
        let table' = table.WithRecordType<CompatibleRecord2> ()
        table'.KeySchema |> should equal table.KeySchema

    [<Fact>]
    let ``Simple Put Operation`` () =
        let value = mkItem()
        let key = table.PutItem value
        let value' = table.GetItem key
        value' |> should equal value

    [<Fact>]
    let ``ContainsKey Operation`` () =
        let value = mkItem()
        let key = table.PutItem value
        table.ContainsKey key |> should equal true
        let _ = table.DeleteItem key
        table.ContainsKey key |> should equal false

    [<Fact>]
    let ``Batch Put Operation`` () =
        let values = set [ for i in 1L .. 20L -> mkItem() ]
        let keys = table.BatchPutItems values
        let values' = table.BatchGetItems keys |> Set.ofArray
        values' |> should equal values

    [<Fact>]
    let ``Simple Delete Operation`` () =
        let item = mkItem()
        let key = table.PutItem item
        table.ContainsKey key |> should equal true
        let item' = table.DeleteItem key
        item' |> should equal item
        table.ContainsKey key |> should equal false


    interface IDisposable with
        member __.Dispose() =
            ignore <| client.DeleteTable(tableName)