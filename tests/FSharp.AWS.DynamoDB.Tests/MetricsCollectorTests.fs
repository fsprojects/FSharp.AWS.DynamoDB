namespace FSharp.AWS.DynamoDB.Tests

open System

open Swensen.Unquote
open Xunit

open FSharp.AWS.DynamoDB
open FSharp.AWS.DynamoDB.Scripting

open Amazon.DynamoDBv2.Model

[<AutoOpen>]
module MetricsCollectorTests =

    type MetricsRecord =
        {
            [<HashKey>]
            HashKey : string
            [<RangeKey>]
            RangeKey : int

            [<LocalSecondaryIndex>]
            LocalSecondaryRangeKey : string

            [<GlobalSecondaryHashKey("GSI")>]
            SecondaryHashKey : string
            [<GlobalSecondaryRangeKey("GSI")>]
            SecondaryRangeKey : int

            LocalAttribute : int
        }

type TestCollector() =

    let metrics = ResizeArray<RequestMetrics>()

    member _.Collect(m : RequestMetrics) = metrics.Add m

    member _.Metrics = metrics |> Seq.toList

type ``Metrics Collector Tests`` (fixture : TableFixture) =

    let rand = let r = Random() in fun () -> r.Next() |> int64
    let mkItem (hk : string) (gshk : string) (i : int): MetricsRecord =
        {
            HashKey = hk
            RangeKey = i
            LocalSecondaryRangeKey = guid()
            SecondaryHashKey = gshk
            SecondaryRangeKey = i
            LocalAttribute = int (rand () % 2L)
        }

    let table = TableContext.Create<MetricsRecord>(fixture.Client, fixture.TableName, createIfNotExists = true)

    let (|ExpectedTableName|_|) name = if name = fixture.TableName then Some () else None
    let (|TotalCu|) : ConsumedCapacity list -> float = Seq.sumBy (fun c -> c.CapacityUnits)

    let [<Fact>] ``Collect Metrics on TryGetItem`` () = async {
        let collector = TestCollector()
        let sut = table.WithMetricsCollector(collector.Collect)

        let! result =
            let nonExistentHk = guid()
            sut.TryGetItemAsync(key = TableKey.Combined (nonExistentHk, 0))
        None =! result

        test <@ match collector.Metrics with
                | [{ ItemCount = 0; Operation = GetItem; TableName = ExpectedTableName; ConsumedCapacity = TotalCu cu }] ->
                    cu > 0
                | _ -> false @> }

    let [<Fact>] ``Collect Metrics on GetItem`` () =
        let item = mkItem (guid()) (guid()) 0
        table.PutItem item |> ignore

        let collector = TestCollector()
        let sut = table.WithMetricsCollector(collector.Collect)

        let _ = sut.GetItem(key = TableKey.Combined (item.HashKey, 0))

        test <@ match collector.Metrics with
                | [{ ItemCount = 1; Operation = GetItem; TableName = ExpectedTableName; ConsumedCapacity = TotalCu cu }] ->
                    cu > 0
                | _ -> false @>

    let [<Fact>] ``Collect Metrics on ContainsKey`` () =
        let item = mkItem (guid()) (guid()) 0
        table.PutItem item |> ignore

        let collector = TestCollector()
        let sut = table.WithMetricsCollector(collector.Collect)

        let _ = sut.ContainsKey(key = TableKey.Combined (item.HashKey, 0))

        test <@ match collector.Metrics with
                | [{ ItemCount = 1; Operation = GetItem; TableName = ExpectedTableName; ConsumedCapacity = TotalCu cu }] ->
                    cu > 0
                | _ -> false @>

    let [<Fact>] ``Collect Metrics on PutItem`` () =
        let collector = TestCollector()
        let sut = table.WithMetricsCollector(collector.Collect)

        let item = mkItem (guid()) (guid()) 0
        let _ = sut.PutItem item

        test <@ match collector.Metrics with
                | [{ ItemCount = 1; Operation = PutItem; TableName = ExpectedTableName; ConsumedCapacity = TotalCu cu }] ->
                    cu > 0
                | _ -> false @>

    let [<Fact>] ``Collect Metrics on UpdateItem`` () =
        let item = mkItem (guid()) (guid()) 0
        table.PutItem item |> ignore

        let collector = TestCollector()
        let sut = table.WithMetricsCollector(collector.Collect)

        let _ = sut.UpdateItem(TableKey.Combined (item.HashKey, item.RangeKey), <@ fun (i : MetricsRecord) -> { i with LocalAttribute = 1000 } @>)

        test <@ match collector.Metrics with
                | [{ ItemCount = 1; Operation = UpdateItem; TableName = ExpectedTableName; ConsumedCapacity = TotalCu cu }] ->
                    cu > 0
                | _ -> false @>

    let [<Fact>] ``Collect Metrics on DeleteItem`` () =
        let item = mkItem (guid()) (guid()) 0
        table.PutItem item |> ignore

        let collector = TestCollector()
        let sut = table.WithMetricsCollector(collector.Collect)

        let _ = sut.DeleteItem(TableKey.Combined (item.HashKey, item.RangeKey))

        test <@ match collector.Metrics with
                | [{ ItemCount = 1; Operation = DeleteItem; TableName = ExpectedTableName; ConsumedCapacity = TotalCu cu }] ->
                    cu > 0
                | _ -> false @>

    let [<Fact>] ``Collect Metrics on Scan`` () =
        let hk = guid()
        let gsk = guid()
        let items = [| for i in 0 .. 1000 -> mkItem hk gsk i |]
        for item in items do
          table.PutItem item |> ignore

        let collector = TestCollector()
        let sut = table.WithMetricsCollector(collector.Collect)

        let items = sut.Scan()

        test <@ match collector.Metrics with
                | [{ ItemCount = c; Operation = Scan; TableName = ExpectedTableName; ConsumedCapacity = TotalCu cu }] ->
                    c = items.Length
                    && cu > 0
                | _ -> false @>

    let [<Fact>] ``Collect Metrics on Query`` () =
        let hk = guid()
        let gsk = guid()
        let items = [| for i in 0 .. 1000 -> mkItem hk gsk i |]
        for item in items do
          table.PutItem item |> ignore

        let collector = TestCollector()
        let sut = table.WithMetricsCollector(collector.Collect)

        let items = sut.Query(<@ fun (r : MetricsRecord) -> r.HashKey = hk @>)

        test <@ match collector.Metrics with
                | [{ ItemCount = c; Operation = Query; TableName = ExpectedTableName; ConsumedCapacity = TotalCu cu }] ->
                    c = items.Length
                    && cu > 0
                | _ -> false @>

    let [<Fact>] ``Collect Metrics on BatchGetItem`` () =
        let hk = guid()
        let gsk = guid()
        let items = [| for i in 0 .. 99 -> mkItem hk gsk i |]
        for item in items do
          table.PutItem item |> ignore

        let collector = TestCollector()
        let sut = table.WithMetricsCollector(collector.Collect)

        let items = sut.BatchGetItems(seq { for i in 0 .. 99 -> TableKey.Combined (hk, i) })

        test <@ match collector.Metrics with
                | [{ ItemCount = c; Operation = BatchGetItems; TableName = ExpectedTableName; ConsumedCapacity = TotalCu cu }] ->
                    c = items.Length
                    && cu > 0
                | _ -> false @>

    let [<Fact>] ``Collect Metrics on BatchPutItem`` () =
        let hk = guid()
        let gsk = guid()
        let items = [| for i in 0 .. 24 -> mkItem hk gsk i |]
        for item in items do
          table.PutItem item |> ignore

        let collector = TestCollector()
        let sut = table.WithMetricsCollector(collector.Collect)

        let _ = sut.BatchPutItems(items |> Seq.map (fun i -> { i with LocalAttribute = 1000 }))

        test <@ match collector.Metrics with
                | [{ ItemCount = c; Operation = BatchWriteItems; TableName = ExpectedTableName; ConsumedCapacity = TotalCu cu }] ->
                    c = items.Length
                    && cu > 0
                | _ -> false @>

    let [<Fact>] ``Collect Metrics on BatchDeleteItem`` () =
        let hk = guid()
        let gsk = guid()
        let items = [| for i in 0 .. 24 -> mkItem hk gsk i |]
        for item in items do
          table.PutItem item |> ignore

        let collector = TestCollector()
        let sut = table.WithMetricsCollector(collector.Collect)

        let _ = sut.BatchDeleteItems(items |> Seq.map (fun i -> TableKey.Combined (i.HashKey, i.RangeKey)))

        test <@ match collector.Metrics with
                | [{ ItemCount = c; Operation = BatchWriteItems; TableName = ExpectedTableName; ConsumedCapacity = TotalCu cu }] ->
                    c = items.Length
                    && cu > 0
                | _ -> false @>

    interface IClassFixture<TableFixture>
