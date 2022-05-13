namespace FSharp.AWS.DynamoDB.Tests

open System

open FSharp.AWS.DynamoDB.Tests
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

    let table = fixture.CreateEmpty<SimpleRecord>()

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

type ``TransactWriteItems tests``(fixture : TableFixture) =

    let rand = let r = Random() in fun () -> int64 <| r.Next()
    let mkItem() =
        {
            HashKey = guid() ; RangeKey = guid() ;
            Value = rand() ; Tuple = rand(), rand() ;
            Map = seq { for _ in 0L .. rand() % 5L -> "K" + guid(), rand() } |> Map.ofSeq
            Unions = [Choice1Of3 (guid()) ; Choice2Of3(rand()) ; Choice3Of3(Guid.NewGuid().ToByteArray())]
        }

    let table = fixture.CreateEmpty<SimpleRecord>()
    let compile = table.Template.PrecomputeConditionalExpr
    let compileUpdate (e : Quotations.Expr<SimpleRecord -> SimpleRecord>) = table.Template.PrecomputeUpdateExpr e
    let doesntExistCondition = compile <@ fun t -> NOT_EXISTS t.Value @>
    let existsCondition = compile <@ fun t -> EXISTS t.Value @>

    let [<Fact>] ``Minimal happy path`` () = async {
        let item = mkItem ()

        let requests = [TransactWrite.Put (item, Some doesntExistCondition) ]

        do! table.TransactWriteItems requests

        let! itemFound = table.ContainsKeyAsync (table.Template.ExtractKey item)
        true =! itemFound }

    let [<Fact>] ``Minimal Canceled path`` () = async {
        let item = mkItem ()

        let requests = [TransactWrite.Put (item, Some existsCondition) ]

        let mutable failed = false
        try do! table.TransactWriteItems requests
        with TransactWriteItemsRequest.TransactionCanceledConditionalCheckFailed -> failed <- true

        true =! failed

        let! itemFound = table.ContainsKeyAsync (table.Template.ExtractKey item)
        false =! itemFound }

    let [<Theory; InlineData true; InlineData false>]
        ``ConditionCheck outcome should affect sibling TransactWrite`` shouldFail = async {
        let item, item2 = mkItem (), mkItem ()
        let! key = table.PutItemAsync item

        let requests = [
            if shouldFail then  TransactWrite.Check (key, doesntExistCondition)
            else                TransactWrite.Check (key, existsCondition)
                                TransactWrite.Put   (item2, None) ]
        let mutable failed = false
        try do! table.TransactWriteItems requests
        with TransactWriteItemsRequest.TransactionCanceledConditionalCheckFailed -> failed <- true

        failed =! shouldFail

        let! item2Found = table.ContainsKeyAsync (table.Template.ExtractKey item2)
        failed =! not item2Found }

    let [<Theory; InlineData true; InlineData false>]
        ``All paths`` shouldFail = async {
        let item, item2, item3, item4, item5, item6, item7 = mkItem (), mkItem (), mkItem (), mkItem (), mkItem (), mkItem (), mkItem ()
        let! key = table.PutItemAsync item

        let requests = [ TransactWrite.Update (key, Some existsCondition, compileUpdate <@ fun t -> { t with Value = 42 } @>)
                         TransactWrite.Put    (item2, None)
                         TransactWrite.Put    (item3, Some doesntExistCondition)
                         TransactWrite.Delete (table.Template.ExtractKey item4, Some doesntExistCondition)
                         TransactWrite.Delete (table.Template.ExtractKey item5, None)
                         TransactWrite.Check  (table.Template.ExtractKey item6, if shouldFail then existsCondition else doesntExistCondition)
                         TransactWrite.Update (TableKey.Combined(item7.HashKey, item7.RangeKey), None, compileUpdate <@ fun t -> { t with Tuple = (42, 42)} @>) ]
        let mutable failed = false
        try do! table.TransactWriteItems requests
        with TransactWriteItemsRequest.TransactionCanceledConditionalCheckFailed -> failed <- true
        failed =! shouldFail

        let! maybeItem = table.TryGetItemAsync key
        test <@ shouldFail <> (maybeItem |> Option.contains { item with Value = 42 }) @>

        let! maybeItem2 = table.TryGetItemAsync(table.Template.ExtractKey item2)
        test <@ shouldFail <> (maybeItem2 |> Option.contains item2) @>

        let! maybeItem3 = table.TryGetItemAsync(table.Template.ExtractKey item3)
        test <@ shouldFail <> (maybeItem3 |> Option.contains item3) @>

        let! maybeItem7 = table.TryGetItemAsync(table.Template.ExtractKey item7)
        test <@ shouldFail <> (maybeItem7 |> Option.map (fun x -> x.Tuple) |> Option.contains (42, 42)) @> }

    let shouldBeRejectedWithArgumentOutOfRangeException requests = async {
        let! e = Async.Catch (table.TransactWriteItems requests)
        test <@ match e with
                | Choice1Of2 () -> false
                | Choice2Of2 e -> e :? ArgumentOutOfRangeException @> }

    let [<Fact>] ``Empty request list is rejected with AORE`` () =
        shouldBeRejectedWithArgumentOutOfRangeException []

    let [<Fact>] ``Over 25 writes are rejected with AORE`` () =
        shouldBeRejectedWithArgumentOutOfRangeException [for _x in 1..26 -> TransactWrite.Put (mkItem (), None)]

    interface IClassFixture<TableFixture>
