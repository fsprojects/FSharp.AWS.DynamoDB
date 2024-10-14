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
        { [<HashKey>]
          HashKey: string
          [<RangeKey>]
          RangeKey: string

          EmptyString: string

          Value: int64

          Tuple: int64 * int64

          Map: Map<string, int64>

          Unions: Choice<string, int64, byte[]> list }

    [<ConstantHashKey("HashKey", "compatible")>]
    type CompatibleRecord =
        { [<RangeKey; CustomName("RangeKey")>]
          Id: string

          Values: Set<int> }

    [<ConstantRangeKey("RangeKey", "compatible")>]
    type CompatibleRecord2 =
        { [<HashKey; CustomName("HashKey")>]
          Id: string

          Values: Set<int> }

type ``Simple Table Operation Tests``(fixture: TableFixture) =

    let rand = let r = Random.Shared in fun () -> int64 <| r.Next()
    let mkItem () =
        { HashKey = guid ()
          RangeKey = guid ()
          EmptyString = ""
          Value = rand ()
          Tuple = rand (), rand ()
          Map = seq { for _ in 0L .. rand () % 5L -> "K" + guid (), rand () } |> Map.ofSeq
          Unions = [ Choice1Of3(guid ()); Choice2Of3(rand ()); Choice3Of3(Guid.NewGuid().ToByteArray()) ] }

    let table = fixture.CreateEmpty<SimpleRecord>()

    [<Fact>]
    let ``Convert to compatible table`` () =
        let table' = table.WithRecordType<CompatibleRecord>()
        test <@ table.PrimaryKey = table'.PrimaryKey @>

    [<Fact>]
    let ``Convert to compatible table 2`` () =
        let table' = table.WithRecordType<CompatibleRecord2>()
        test <@ table.PrimaryKey = table'.PrimaryKey @>

    [<Fact>]
    let ``Simple Put Operation`` () =
        let value = mkItem ()
        let key = table.PutItem value
        let value' = table.GetItem key
        test <@ value = value' @>

    [<Fact>]
    let ``ContainsKey Operation`` () =
        let value = mkItem ()
        let key = table.PutItem value
        test <@ table.ContainsKey key @>
        let _ = table.DeleteItem key
        test <@ not (table.ContainsKey key) @>

    [<Fact>]
    let ``TryGet Operation`` () =
        let value = mkItem ()
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

    [<Fact>]
    let ``Batch Put Operation`` () =
        let values = set [ for _ in 1L .. 20L -> mkItem () ]
        let unprocessed = table.BatchPutItems values
        let values' =
            table.BatchGetItems(values |> Seq.map (fun r -> TableKey.Combined(r.HashKey, r.RangeKey)))
            |> Set.ofArray
        test <@ Array.isEmpty unprocessed @>
        test <@ values = values' @>

    [<Fact>]
    let ``Batch Delete Operation`` () =
        let values = set [ for _ in 1L .. 20L -> mkItem () ]
        table.BatchPutItems values |> ignore
        let unprocessed = table.BatchDeleteItems(values |> Seq.map (fun r -> TableKey.Combined(r.HashKey, r.RangeKey)))
        test <@ Array.isEmpty unprocessed @>
        let values' = table.BatchGetItems(values |> Seq.map (fun r -> TableKey.Combined(r.HashKey, r.RangeKey)))
        test <@ Array.isEmpty values' @>

    [<Fact>]
    let ``Simple Delete Operation`` () =
        let item = mkItem ()
        let key = table.PutItem item
        test <@ table.ContainsKey key @>
        let item' = table.DeleteItem key
        test <@ Some item = item' @>
        test <@ not (table.ContainsKey key) @>

    [<Fact>]
    let ``Idempotent Delete Operation`` () =
        let item = mkItem ()
        let key = table.PutItem item
        test <@ table.ContainsKey key @>
        let item' = table.DeleteItem key
        test <@ item' = Some item @>
        let deletedItem = table.DeleteItem key
        test <@ None = deletedItem @>
        test <@ not (table.ContainsKey key) @>

    [<Theory>]
    [<InlineData("",
                 "rangeKey",
                 "One or more parameter values are not valid. The AttributeValue for a key attribute cannot contain an empty string value. Key: HashKey")>]
    [<InlineData("hashKey",
                 "",
                 "One or more parameter values are not valid. The AttributeValue for a key attribute cannot contain an empty string value. Key: RangeKey")>]
    [<InlineData("",
                 "",
                 "One or more parameter values are not valid. The AttributeValue for a key attribute cannot contain an empty string value. Key: HashKey")>]
    let ``Operations with empty key values should fail with a DynamoDB client error`` (hashKey, rangeKey, expectedErrorMsg) =
        let value = { mkItem () with HashKey = hashKey; RangeKey = rangeKey }
        try
            table.PutItem value |> ignore
        with :? Amazon.DynamoDBv2.AmazonDynamoDBException as ex ->
            test <@ ex.Message = expectedErrorMsg @>

    interface IClassFixture<TableFixture>

type ``TransactWriteItems tests``(table1: TableFixture, table2: TableFixture) =

    let randInt64 = let r = Random.Shared in fun () -> int64 <| r.Next()
    let randInt = let r = Random.Shared in fun () -> int32 <| r.Next()
    let mkItem () =
        { HashKey = guid ()
          RangeKey = guid ()
          EmptyString = ""
          Value = randInt64 ()
          Tuple = randInt64 (), randInt64 ()
          Map = seq { for _ in 0L .. randInt64 () % 5L -> "K" + guid (), randInt64 () } |> Map.ofSeq
          Unions = [ Choice1Of3(guid ()); Choice2Of3(randInt64 ()); Choice3Of3(Guid.NewGuid().ToByteArray()) ] }

    let mkCompatibleItem () : CompatibleRecord = { Id = guid (); Values = set [ for _ in 0 .. (randInt () % 5) -> randInt () ] }

    let table1 = table1.CreateEmpty<SimpleRecord>()
    let table2 = table2.CreateEmpty<CompatibleRecord>()
    let compileTable1 = table1.Template.PrecomputeConditionalExpr
    let compileTable2 = table2.Template.PrecomputeConditionalExpr
    let compileUpdateTable1 (e: Quotations.Expr<SimpleRecord -> SimpleRecord>) = table1.Template.PrecomputeUpdateExpr e
    let compileUpdateTable2 (e: Quotations.Expr<SimpleRecord -> SimpleRecord>) = table1.Template.PrecomputeUpdateExpr e
    let doesntExistConditionTable1 = compileTable1 <@ fun t -> NOT_EXISTS t.Value @>
    let doesntExistConditionTable2 = compileTable2 <@ fun t -> NOT_EXISTS t.Values @>
    let existsConditionTable1 = compileTable1 <@ fun t -> EXISTS t.Value @>
    let existsConditionTable2 = compileTable2 <@ fun t -> EXISTS t.Values @>

    [<Fact>]
    let ``Minimal happy path`` () = async {
        let item = mkItem ()
        do!
            Transaction()
                .Put(table1, item, doesntExistConditionTable1)
                .TransactWriteItems()

        let! itemFound = table1.ContainsKeyAsync(table1.Template.ExtractKey item)
        true =! itemFound
    }

    [<Fact>]
    let ``Minimal happy path with multiple tables`` () = async {
        let item = mkItem ()
        let compatibleItem = mkCompatibleItem ()

        do!
            Transaction()
                .Put(table1, item, doesntExistConditionTable1)
                .Put(table2, compatibleItem, doesntExistConditionTable2)
                .TransactWriteItems()

        let! itemFound = table1.ContainsKeyAsync(table1.Template.ExtractKey item)
        true =! itemFound

        let! compatibleItemFound = table2.ContainsKeyAsync(table2.Template.ExtractKey compatibleItem)
        true =! compatibleItemFound
    }

    [<Fact>]
    let ``Minimal Canceled path`` () = async {
        let item = mkItem ()

        let mutable failed = false
        try
            do!
                Transaction()
                    .Put(table1, item, existsConditionTable1)
                    .TransactWriteItems()
        with TransactWriteItemsRequest.TransactionCanceledConditionalCheckFailed ->
            failed <- true

        true =! failed

        let! itemFound = table1.ContainsKeyAsync(table1.Template.ExtractKey item)
        false =! itemFound
    }

    [<Theory; InlineData true; InlineData false>]
    let ``ConditionCheck outcome should affect sibling TransactWrite`` shouldFail = async {
        let item, item2 = mkItem (), mkItem ()
        let! key = table1.PutItemAsync item

        let transaction =
            if shouldFail then
                Transaction().Check(table1, key, doesntExistConditionTable1)
            else
                Transaction()
                    .Check(table1, key, existsConditionTable1)
                    .Put(table1, item2)

        let mutable failed = false
        try
            do! transaction.TransactWriteItems()
        with TransactWriteItemsRequest.TransactionCanceledConditionalCheckFailed ->
            failed <- true

        failed =! shouldFail

        let! item2Found = table1.ContainsKeyAsync(table1.Template.ExtractKey item2)
        failed =! not item2Found
    }

    [<Theory; InlineData true; InlineData false>]
    let ``All paths`` shouldFail = async {
        let item, item2, item3, item4, item5, item6, item7 = mkItem (), mkItem (), mkItem (), mkItem (), mkItem (), mkItem (), mkItem ()
        let! key = table1.PutItemAsync item
        let Transaction = Transaction()

        let requests =
            [ Transaction.Update(table1, key, compileUpdateTable1 <@ fun t -> { t with Value = 42 } @>, existsConditionTable1)
              Transaction.Put(table1, item2)
              Transaction.Put(table1, item3, doesntExistConditionTable1)
              Transaction.Delete(table1, table1.Template.ExtractKey item4, Some doesntExistConditionTable1)
              Transaction.Delete(table1, table1.Template.ExtractKey item5, None)
              Transaction.Check(
                  table1,
                  table1.Template.ExtractKey item6,
                  (if shouldFail then
                       existsConditionTable1
                   else
                       doesntExistConditionTable1)
              )
              Transaction.Update(
                  table1,
                  TableKey.Combined(item7.HashKey, item7.RangeKey),
                  compileUpdateTable1 <@ fun t -> { t with Tuple = (42, 42) } @>
              ) ]
        let mutable failed = false
        try
            do! Transaction.TransactWriteItems()
        with TransactWriteItemsRequest.TransactionCanceledConditionalCheckFailed ->
            failed <- true
        failed =! shouldFail

        let! maybeItem = table1.TryGetItemAsync key
        test <@ shouldFail <> (maybeItem |> Option.contains { item with Value = 42 }) @>

        let! maybeItem2 = table1.TryGetItemAsync(table1.Template.ExtractKey item2)
        test <@ shouldFail <> (maybeItem2 |> Option.contains item2) @>

        let! maybeItem3 = table1.TryGetItemAsync(table1.Template.ExtractKey item3)
        test <@ shouldFail <> (maybeItem3 |> Option.contains item3) @>

        let! maybeItem7 = table1.TryGetItemAsync(table1.Template.ExtractKey item7)
        test <@ shouldFail <> (maybeItem7 |> Option.map (fun x -> x.Tuple) |> Option.contains (42, 42)) @>
    }

    let shouldBeRejectedWithArgumentOutOfRangeException (builder: Transaction) = async {
        let! e = Async.Catch(builder.TransactWriteItems())
        test
            <@
                match e with
                | Choice1Of2() -> false
                | Choice2Of2 e -> e :? ArgumentOutOfRangeException
            @>
    }

    [<Fact>]
    let ``Empty request list is rejected with AORE`` () =
        shouldBeRejectedWithArgumentOutOfRangeException (Transaction())
        |> Async.RunSynchronously
        |> ignore

    [<Fact>]
    let ``Over 100 writes are rejected with AORE`` () =
        let Transaction = Transaction()
        for _x in 1..101 do
            Transaction.Put(table1, mkItem ()) |> ignore

        shouldBeRejectedWithArgumentOutOfRangeException Transaction

    interface IClassFixture<TableFixture>
