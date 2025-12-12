module FSharp.AWS.DynamoDB.Tests.Diagnostics

open System

open Swensen.Unquote
open Xunit

open FSharp.AWS.DynamoDB
open FSharp.AWS.DynamoDB.Scripting

open Amazon.DynamoDBv2.Model
open System.Diagnostics.Metrics
open System.Diagnostics
open System.Collections.Generic

type DiagnosticsRecord =
    { [<HashKey>]
      HashKey: string
      [<RangeKey>]
      RangeKey: int

      [<LocalSecondaryIndex>]
      LocalSecondaryRangeKey: string

      [<GlobalSecondaryHashKey("GSI")>]
      SecondaryHashKey: string
      [<GlobalSecondaryRangeKey("GSI")>]
      SecondaryRangeKey: int

      LocalAttribute: int }

let rand = let r = Random.Shared in fun () -> r.Next() |> int64
let mkItem (hk: string) (gshk: string) (i: int) : DiagnosticsRecord =
    { HashKey = hk
      RangeKey = i
      LocalSecondaryRangeKey = guid ()
      SecondaryHashKey = gshk
      SecondaryRangeKey = i
      LocalAttribute = int (rand () % 2L) }

/// Tests without common setup
type Tests(fixture: TableFixture) =

    let table = fixture.CreateEmpty<DiagnosticsRecord>()

    let (|ExpectedTableName|_|) name = if name = fixture.TableName then Some() else None

    let capacityListener = new MeterListener()
    let activityListener = new ActivityListener()

    let capacityMeasurements = ResizeArray<Measurement<float>>()
    let activities = ResizeArray<Activity>()

    do capacityListener.EnableMeasurementEvents Meter.consumedCapacity
    do
        capacityListener.SetMeasurementEventCallback(
            MeasurementCallback(fun _ value tags _ -> capacityMeasurements.Add(Measurement(value, tags)))
        )
    do activityListener.ShouldListenTo <- fun asrc -> asrc.Name = "FSharp.AWS.DynamoDB"
    do activityListener.ActivityStopped <- fun a -> activities.Add a
    do ActivitySource.AddActivityListener activityListener

    let clearMetrics () =
        capacityMeasurements.Clear()
        activities.Clear()

    let isTagSet tag (tags: KeyValuePair<string, string> seq) = tags |> Seq.exists (fun t -> t.Key = tag)

    [<Fact>]
    let ``Collect Metrics on TryGetItem`` () = async {
        let! result =
            let nonExistentHk = guid ()
            table.TryGetItemAsync(key = TableKey.Combined(nonExistentHk, 0))

        None =! result
        test <@ activities.Count > 0 @>
        test <@ capacityMeasurements.Count > 0 @>
    }

    [<Fact>]
    let ``Collect Metrics on PutItem`` () =
        let item = mkItem (guid ()) (guid ()) 0
        let _ = table.PutItem item

        test <@ activities.Count > 0 @>
        // test <@ activities |> Seq.forall (fun a -> a.Tags |> isTagSet "aws.dynamodb.table_names") @>
        test <@ capacityMeasurements.Count > 0 @>

    let compile = table.Template.PrecomputeConditionalExpr

    // [<Fact>]
    // let ``Collect Metrics on Transactional PutItem`` () = async {
    //     let item = mkItem (guid ()) (guid ()) 0
    //     let _ = table.PutItem item
    //     let simpleCu =
    //         trap
    //             <@
    //                 match collector.Metrics with
    //                 | [ { ConsumedCapacity = TotalCu cu } ] -> cu
    //                 | x -> failwithf "Unexpected %A" x
    //             @>
    //     clearMetrics ()

    //     let item = mkItem (guid ()) (guid ()) 0
    //     let transaction = Transaction(sut.Client, collector.Collect)
    //     transaction.Put(sut, item, compile <@ fun t -> NOT_EXISTS t.RangeKey @>)
    //     do! transaction.TransactWriteItems()

    //     test
    //         <@
    //             match collector.Metrics with
    //             | [ { ItemCount = 1
    //                   Operation = TransactWriteItems
    //                   TableName = ExpectedTableName
    //                   ConsumedCapacity = TotalCu cu } ] -> cu >= simpleCu * 2. // doing it transactionally costs at least double
    //             | _ -> false
    //         @>

    //     let! itemFound = sut.ContainsKeyAsync(sut.Template.ExtractKey item)
    //     test <@ itemFound @>
    // }

    // [<Fact>]
    // let ``No Metrics on Canceled PutItem`` () = async {
    //     let collector = TestCollector()
    //     let sut = rawTable.WithMetricsCollector(collector.Collect)

    //     let item = mkItem (guid ()) (guid ()) 0
    //     let transaction = rawTable.CreateTransaction()
    //     transaction.Put(sut, item, compile <@ fun t -> EXISTS t.RangeKey @>)
    //     let mutable failed = false
    //     try
    //         do!
    //             // The check will fail, which triggers a throw from the underlying AWS SDK; there's no way to extract the consumption info in that case
    //             transaction.TransactWriteItems()
    //     with Transaction.TransactionCanceledConditionalCheckFailed ->
    //         failed <- true
    //     true =! failed
    //     [] =! collector.Metrics
    // }

    interface IClassFixture<TableFixture>

// /// Tests that look up a specific item. Each test run gets a fresh individual item
// type ItemTests(fixture: TableFixture) =

//     let rawTable = fixture.CreateEmpty<DiagnosticsRecord>()
//     let (|ExpectedTableName|_|) name = if name = fixture.TableName then Some() else None

//     let item = mkItem (guid ()) (guid ()) 0
//     do rawTable.PutItem item |> ignore

//     let collector = TestCollector()
//     let sut = rawTable.WithMetricsCollector(collector.Collect)

//     [<Fact>]
//     let ``Collect Metrics on GetItem`` () =
//         let _ = sut.GetItem(key = TableKey.Combined(item.HashKey, 0))

//         test
//             <@
//                 match collector.Metrics with
//                 | [ { ItemCount = 1
//                       Operation = GetItem
//                       TableName = ExpectedTableName
//                       ConsumedCapacity = TotalCu cu } ] -> cu > 0
//                 | _ -> false
//             @>

//     [<Fact>]
//     let ``Collect Metrics on ContainsKey`` () =
//         let _ = sut.ContainsKey(key = TableKey.Combined(item.HashKey, 0))

//         test
//             <@
//                 match collector.Metrics with
//                 | [ { ItemCount = 1
//                       Operation = GetItem
//                       TableName = ExpectedTableName
//                       ConsumedCapacity = TotalCu cu } ] -> cu > 0
//                 | _ -> false
//             @>

//     [<Fact>]
//     let ``Collect Metrics on UpdateItem`` () =
//         let _ =
//             sut.UpdateItem(
//                 TableKey.Combined(item.HashKey, item.RangeKey),
//                 <@ fun (i: DiagnosticsRecord) -> { i with LocalAttribute = 1000 } @>
//             )

//         test
//             <@
//                 match collector.Metrics with
//                 | [ { ItemCount = 1
//                       Operation = UpdateItem
//                       TableName = ExpectedTableName
//                       ConsumedCapacity = TotalCu cu } ] -> cu > 0
//                 | _ -> false
//             @>

//     [<Fact>]
//     let ``Collect Metrics on DeleteItem`` () =
//         let _ = sut.DeleteItem(TableKey.Combined(item.HashKey, item.RangeKey))

//         test
//             <@
//                 match collector.Metrics with
//                 | [ { ItemCount = 1
//                       Operation = DeleteItem
//                       TableName = ExpectedTableName
//                       ConsumedCapacity = TotalCu cu } ] -> cu > 0
//                 | _ -> false
//             @>

//     interface IClassFixture<TableFixture>

// /// Heavy tests reliant on establishing (and mutating) multiple items. Separate Test Class so Xunit will run them in parallel with others
// type BulkMutationTests(fixture: TableFixture) =

//     let rawTable = fixture.CreateEmpty<DiagnosticsRecord>()
//     let (|ExpectedTableName|_|) name = if name = fixture.TableName then Some() else None

//     // NOTE we mutate the items so they need to be established each time
//     let items =
//         let hk, gsk = guid (), guid ()
//         [| for i in 0..24 -> mkItem hk gsk i |]
//     do
//         for item in items do
//             rawTable.PutItem item |> ignore

//     let collector = TestCollector()
//     let sut = rawTable.WithMetricsCollector(collector.Collect)

//     [<Fact>]
//     let ``Collect Metrics on BatchPutItem`` () =
//         let _results = sut.BatchPutItems(items |> Seq.map (fun i -> { i with LocalAttribute = 1000 }))

//         test
//             <@
//                 match collector.Metrics with
//                 | [ { ItemCount = c
//                       Operation = BatchWriteItems
//                       TableName = ExpectedTableName
//                       ConsumedCapacity = TotalCu cu } ] -> c = items.Length && cu > 0
//                 | _ -> false
//             @>

//     [<Fact>]
//     let ``Collect Metrics on BatchDeleteItem`` () =
//         let _keys = sut.BatchDeleteItems(items |> Seq.map (fun i -> TableKey.Combined(i.HashKey, i.RangeKey)))

//         test
//             <@
//                 match collector.Metrics with
//                 | [ { ItemCount = c
//                       Operation = BatchWriteItems
//                       TableName = ExpectedTableName
//                       ConsumedCapacity = TotalCu cu } ] -> c = items.Length && cu > 0
//                 | _ -> false
//             @>

//     interface IClassFixture<TableFixture>

// /// TableFixture with 1000 items with a known HashKey pre-inserted
// type ManyReadOnlyItemsFixture() =
//     inherit TableFixture()

//     // TOCONSIDER shift this into IAsyncLifetime.InitializeAsync
//     let table = base.CreateEmpty<DiagnosticsRecord>()

//     let hk = guid ()
//     do
//         let gsk = guid ()
//         let items = [| for i in 0..99 -> mkItem hk gsk i |]
//         for item in items do
//             table.PutItem item |> ignore

//     member _.Table = table
//     member _.HashKey = hk

// /// NOTE These tests share the prep work of making a Table Containing lots of items to read
// // DO NOT add tests that will delete or mutate those items
// type ``Bulk Read Operations``(fixture: ManyReadOnlyItemsFixture) =

//     let (|ExpectedTableName|_|) name = if name = fixture.TableName then Some() else None

//     let collector = TestCollector()
//     let sut = fixture.Table.WithMetricsCollector(collector.Collect)

//     [<Fact>]
//     let ``Collect Metrics on Scan`` () =
//         let items = sut.Scan()

//         test
//             <@
//                 match collector.Metrics with
//                 | [ { ItemCount = c
//                       Operation = Scan
//                       TableName = ExpectedTableName
//                       ConsumedCapacity = TotalCu cu } ] -> c = items.Length && cu > 0
//                 | _ -> false
//             @>

//     [<Fact>]
//     let ``Collect Metrics on Query`` () =
//         let items = sut.Query(<@ fun (r: DiagnosticsRecord) -> r.HashKey = fixture.HashKey @>)

//         test
//             <@
//                 match collector.Metrics with
//                 | [ { ItemCount = c
//                       Operation = Query
//                       TableName = ExpectedTableName
//                       ConsumedCapacity = TotalCu cu } ] -> c = items.Length && cu > 0
//                 | _ -> false
//             @>

//     [<Fact>]
//     let ``Collect Metrics on BatchGetItem`` () =
//         let items = sut.BatchGetItems(seq { for i in 0..99 -> TableKey.Combined(fixture.HashKey, i) })

//         test
//             <@
//                 match collector.Metrics with
//                 | [ { ItemCount = c
//                       Operation = BatchGetItems
//                       TableName = ExpectedTableName
//                       ConsumedCapacity = TotalCu cu } ] -> c = items.Length && cu > 0
//                 | _ -> false
//             @>

//     interface IClassFixture<ManyReadOnlyItemsFixture>
