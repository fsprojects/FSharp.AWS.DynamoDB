namespace FSharp.AWS.DynamoDB.Tests

open System

open Swensen.Unquote
open Xunit

open FSharp.AWS.DynamoDB
open FSharp.AWS.DynamoDB.Scripting // These tests lean on the Synchronous wrappers

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

    [<ConstantHashKey("HashKey", "compatible")>]
    type CompatibleRecord =
        {
            [<RangeKey; CustomName("RangeKey")>]
            Id : string

            Values : Set<int>
        }

    [<ConstantRangeKey("RangeKey", "compatible")>]
    type CompatibleRecord2 =
        {
            [<HashKey; CustomName("HashKey")>]
            Id : string

            Values : Set<int>
        }

type ``Simple Table Operation Tests`` (fixture : TableFixture) =

    let rand = let r = Random() in fun () -> int64 <| r.Next()
    let mkItem() =
        {
            HashKey = guid() ; RangeKey = guid() ;
            Value = rand() ; Tuple = rand(), rand() ;
            Map = seq { for _ in 0L .. rand() % 5L -> "K" + guid(), rand() } |> Map.ofSeq
            Unions = [Choice1Of3 (guid()) ; Choice2Of3(rand()) ; Choice3Of3(Guid.NewGuid().ToByteArray())]
        }

    let table = TableContext.Create<SimpleRecord>(fixture.Client, fixture.TableName, createIfNotExists = true)

    let [<Fact>] ``Convert to compatible table`` () =
        let table' = table.WithRecordType<CompatibleRecord> ()
        test <@ table.PrimaryKey = table'.PrimaryKey @>

    let [<Fact>] ``Convert to compatible table 2`` () =
        let table' = table.WithRecordType<CompatibleRecord2> ()
        test <@ table.PrimaryKey = table'.PrimaryKey @>

    let [<Fact>] ``Simple Put Operation`` () =
        let value = mkItem()
        let key = table.PutItem value
        let value' = table.GetItem key
        test <@ value = value' @>

    let [<Fact>] ``ContainsKey Operation`` () =
        let value = mkItem()
        let key = table.PutItem value
        test <@ table.ContainsKey key @>
        let _ = table.DeleteItem key
        test <@ not (table.ContainsKey key) @>

    let [<Fact>] ``TryGet Operation`` () =
        let value = mkItem()
        let computedKey = table.Template.ExtractKey value
        let get k = table.TryGetItemAsync k |> Async.RunSynchronously
        let initialLoad = get computedKey
        test <@ None = initialLoad @>
        let key = table.PutItem value
        test <@ computedKey = key @> // "Local key computation should be same as Put result"
        let loaded = get computedKey
        test <@ Some value = loaded @>
        let _ = table.DeleteItem key
        test <@ None = get key @>

    let [<Fact>] ``Batch Put Operation`` () =
        let values = set [ for _ in 1L .. 20L -> mkItem() ]
        let unprocessed = table.BatchPutItems values
        let values' = table.BatchGetItems (values |> Seq.map (fun r -> TableKey.Combined (r.HashKey, r.RangeKey))) |> Set.ofArray
        test <@ Array.isEmpty unprocessed @>
        test <@ values = values' @>

    let [<Fact>] ``Batch Delete Operation`` () =
        let values = set [ for _ in 1L .. 20L -> mkItem() ]
        table.BatchPutItems values |> ignore
        let unprocessed = table.BatchDeleteItems (values |> Seq.map (fun r -> TableKey.Combined (r.HashKey, r.RangeKey)))
        test <@ Array.isEmpty unprocessed @>
        let values' = table.BatchGetItems (values |> Seq.map (fun r -> TableKey.Combined (r.HashKey, r.RangeKey)))
        test <@ Array.isEmpty values' @>

    let [<Fact>] ``Simple Delete Operation`` () =
        let item = mkItem()
        let key = table.PutItem item
        test <@ table.ContainsKey key @>
        let item' = table.DeleteItem key
        test <@ Some item = item' @>
        test <@ not (table.ContainsKey key) @>

    let [<Fact>] ``Idempotent Delete Operation`` () =
        let item = mkItem()
        let key = table.PutItem item
        test <@ table.ContainsKey key @>
        let item' = table.DeleteItem key
        test <@ Option.isSome item' @>
        let deletedItem = table.DeleteItem key
        test <@ None = deletedItem @>
        test <@ not (table.ContainsKey key) @>

    interface IClassFixture<TableFixture>
