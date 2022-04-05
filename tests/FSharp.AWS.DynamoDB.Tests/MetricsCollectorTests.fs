namespace FSharp.AWS.DynamoDB.Tests

open System

open Expecto

open FSharp.AWS.DynamoDB
open FSharp.AWS.DynamoDB.Scripting

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
    member _.Collect (m : RequestMetrics) =
        metrics.Add m

    member _.Metrics with get() = metrics |> Seq.toList

type ``Metrics Collector Tests`` (fixture : TableFixture) =

    let rand = let r = Random() in fun () -> int64 <| r.Next()
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

    member __.``Collect Metrics on GetItem`` () =
        let item = mkItem (guid()) (guid()) 0
        table.PutItem item |> ignore

        let collector = TestCollector()
        let _ = table.WithMetricsCollector(collector.Collect).GetItem (key = TableKey.Combined (item.HashKey, 0))
        Expect.equal collector.Metrics.Length 1 ""
        let metrics = collector.Metrics.Head
        Expect.equal metrics.Operation GetItem ""
        Expect.equal metrics.TableName fixture.TableName ""
        Expect.equal metrics.ItemCount 1 ""
        Expect.isGreaterThan (metrics.ConsumedCapacity |> Seq.sumBy (fun c -> c.CapacityUnits)) 0. ""

    member __.``Collect Metrics on ContainsKey`` () =
        let item = mkItem (guid()) (guid()) 0
        table.PutItem item |> ignore

        let collector = TestCollector()
        let _ = table.WithMetricsCollector(collector.Collect).ContainsKey (key = TableKey.Combined (item.HashKey, 0))
        Expect.equal collector.Metrics.Length 1 ""
        let metrics = collector.Metrics.Head
        Expect.equal metrics.Operation GetItem ""
        Expect.equal metrics.TableName fixture.TableName ""
        Expect.equal metrics.ItemCount 1 ""
        Expect.isGreaterThan (metrics.ConsumedCapacity |> Seq.sumBy (fun c -> c.CapacityUnits)) 0. ""

    member __.``Collect Metrics on PutItem`` () =
        let item = mkItem (guid()) (guid()) 0
        let collector = TestCollector()
        table.WithMetricsCollector(collector.Collect).PutItem item |> ignore

        Expect.equal collector.Metrics.Length 1 ""
        let metrics = collector.Metrics.Head
        Expect.equal metrics.Operation PutItem ""
        Expect.equal metrics.TableName fixture.TableName ""
        Expect.equal metrics.ItemCount 1 ""
        Expect.isGreaterThan (metrics.ConsumedCapacity |> Seq.sumBy (fun c -> c.CapacityUnits)) 0. ""

    member __.``Collect Metrics on UpdateItem`` () =
        let item = mkItem (guid()) (guid()) 0
        let collector = TestCollector()
        table.PutItem item |> ignore
        table.WithMetricsCollector(collector.Collect).UpdateItem(TableKey.Combined (item.HashKey, item.RangeKey), <@ fun (i : MetricsRecord) -> { i with LocalAttribute = 1000 } @>) |> ignore

        Expect.equal collector.Metrics.Length 1 ""
        let metrics = collector.Metrics.Head
        Expect.equal metrics.Operation UpdateItem ""
        Expect.equal metrics.TableName fixture.TableName ""
        Expect.equal metrics.ItemCount 1 ""
        Expect.isGreaterThan (metrics.ConsumedCapacity |> Seq.sumBy (fun c -> c.CapacityUnits)) 0. ""

    member __.``Collect Metrics on DeleteItem`` () =
        let item = mkItem (guid()) (guid()) 0
        let collector = TestCollector()
        table.PutItem item |> ignore
        table.WithMetricsCollector(collector.Collect).DeleteItem(TableKey.Combined (item.HashKey, item.RangeKey)) |> ignore

        Expect.equal collector.Metrics.Length 1 ""
        let metrics = collector.Metrics.Head
        Expect.equal metrics.Operation DeleteItem ""
        Expect.equal metrics.TableName fixture.TableName ""
        Expect.equal metrics.ItemCount 1 ""
        Expect.isGreaterThan (metrics.ConsumedCapacity |> Seq.sumBy (fun c -> c.CapacityUnits)) 0. ""

    member __.``Collect Metrics on Scan`` () =
        let hk = guid()
        let gsk = guid()
        let items = seq { for i in 0 .. 1000 -> mkItem hk gsk i } |> Seq.toArray |> Array.sortBy (fun r -> r.RangeKey)
        for item in items do
          table.PutItem item |> ignore

        let collector = TestCollector()
        let items = table.WithMetricsCollector(collector.Collect).Scan()

        Expect.equal collector.Metrics.Length 1 ""
        let metrics = collector.Metrics.Head
        Expect.equal metrics.Operation Scan ""
        Expect.equal metrics.TableName fixture.TableName ""
        Expect.equal metrics.ItemCount items.Length ""
        Expect.isGreaterThan (metrics.ConsumedCapacity |> Seq.sumBy (fun c -> c.CapacityUnits)) 0. ""

    member __.``Collect Metrics on Query`` () =
        let hk = guid()
        let gsk = guid()
        let items = seq { for i in 0 .. 1000 -> mkItem hk gsk i } |> Seq.toArray |> Array.sortBy (fun r -> r.RangeKey)
        for item in items do
          table.PutItem item |> ignore

        let collector = TestCollector()
        let items = table.WithMetricsCollector(collector.Collect).Query(<@ fun (r : MetricsRecord) -> r.HashKey = hk @>)

        Expect.equal collector.Metrics.Length 1 ""
        let metrics = collector.Metrics.Head
        Expect.equal metrics.Operation Query ""
        Expect.equal metrics.TableName fixture.TableName ""
        Expect.equal metrics.ItemCount items.Length ""
        Expect.isGreaterThan (metrics.ConsumedCapacity |> Seq.sumBy (fun c -> c.CapacityUnits)) 0. ""

    member __.``Collect Metrics on BatchGetItem`` () =
        let hk = guid()
        let gsk = guid()
        let items = seq { for i in 0 .. 99 -> mkItem hk gsk i } |> Seq.toArray |> Array.sortBy (fun r -> r.RangeKey)
        for item in items do
          table.PutItem item |> ignore

        let collector = TestCollector()
        let items = table.WithMetricsCollector(collector.Collect).BatchGetItems (seq { for i in 0 .. 99 -> TableKey.Combined (hk, i) })

        Expect.equal collector.Metrics.Length 1 ""
        let metrics = collector.Metrics.Head
        Expect.equal metrics.Operation BatchGetItems ""
        Expect.equal metrics.TableName fixture.TableName ""
        Expect.equal metrics.ItemCount items.Length ""
        Expect.isGreaterThan (metrics.ConsumedCapacity |> Seq.sumBy (fun c -> c.CapacityUnits)) 0. ""

    member __.``Collect Metrics on BatchPutItem`` () =
        let hk = guid()
        let gsk = guid()
        let items = seq { for i in 0 .. 24 -> mkItem hk gsk i } |> Seq.toArray |> Array.sortBy (fun r -> r.RangeKey)
        for item in items do
          table.PutItem item |> ignore

        let collector = TestCollector()
        table.WithMetricsCollector(collector.Collect).BatchPutItems (items |> Seq.map (fun i -> { i with LocalAttribute = 1000 })) |> ignore

        Expect.equal collector.Metrics.Length 1 ""
        let metrics = collector.Metrics.Head
        Expect.equal metrics.Operation BatchWriteItems ""
        Expect.equal metrics.TableName fixture.TableName ""
        Expect.equal metrics.ItemCount items.Length ""
        Expect.isGreaterThan (metrics.ConsumedCapacity |> Seq.sumBy (fun c -> c.CapacityUnits)) 0. ""

    member __.``Collect Metrics on BatchDeleteItem`` () =
        let hk = guid()
        let gsk = guid()
        let items = seq { for i in 0 .. 24 -> mkItem hk gsk i } |> Seq.toArray |> Array.sortBy (fun r -> r.RangeKey)
        for item in items do
          table.PutItem item |> ignore

        let collector = TestCollector()
        table.WithMetricsCollector(collector.Collect).BatchDeleteItems (items |> Seq.map (fun i -> TableKey.Combined (i.HashKey, i.RangeKey))) |> ignore

        Expect.equal collector.Metrics.Length 1 ""
        let metrics = collector.Metrics.Head
        Expect.equal metrics.Operation BatchWriteItems ""
        Expect.equal metrics.TableName fixture.TableName ""
        Expect.equal metrics.ItemCount items.Length ""
        Expect.isGreaterThan (metrics.ConsumedCapacity |> Seq.sumBy (fun c -> c.CapacityUnits)) 0. ""
