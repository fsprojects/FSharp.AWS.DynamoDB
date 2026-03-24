module FSharp.AWS.DynamoDB.Tests.MultipleKeyAttributeTests

open System

open Swensen.Unquote
open Xunit

open FSharp.AWS.DynamoDB
open FSharp.AWS.DynamoDB.Scripting

[<AutoOpen>]
module MultiKeyTypes =

    type InverseKeyRecord =
        { [<HashKey>]
          [<GlobalSecondaryRangeKey(indexName = "Inverted")>]
          PrimaryKey: string
          [<RangeKey>]
          [<GlobalSecondaryHashKey(indexName = "Inverted")>]
          SortKey: string }

    type InverseKeyRecordUsingCustomName =
        { [<HashKey>]
          PrimaryKey: string
          [<RangeKey>]
          SortKey: string
          [<GlobalSecondaryHashKey(indexName = "GSIReversed"); CustomName("SortKey")>]
          GSIReverseHashKey: string
          [<GlobalSecondaryRangeKey(indexName = "GSIReversed"); CustomName("PrimaryKey")>]
          GSIReverseRangeKey: string }

    type SharedRangeKeyRecord =
        { [<HashKey>]
          HashKey: string
          [<GlobalSecondaryHashKey(indexName = "GSI1")>]
          GSI1: string
          [<GlobalSecondaryHashKey(indexName = "GSI2")>]
          GSI2: string
          [<GlobalSecondaryRangeKey(indexName = "GSI1")>]
          [<GlobalSecondaryRangeKey(indexName = "GSI2")>]
          SortKey: string }

type ``Inverse GSI Table Operation Tests``(fixture: TableFixture) =

    let rand = let r = Random.Shared in fun () -> r.Next() |> int64
    let mkItem () =
        { PrimaryKey = ((int (rand ())) % 50).ToString()
          SortKey = ((int (rand ())) % 50).ToString() }

    let table = fixture.CreateEmpty<InverseKeyRecord>()

    [<Fact>]
    let ``Query by Table Key and GSI`` () =
        let values = set [ for _ in 1L .. 1000L -> mkItem () ]
        for batch in values |> Set.toSeq |> Seq.chunkBySize 25 do
            table.BatchPutItems batch =! [||]
        let queriedTable = table.Query <@ fun (i: InverseKeyRecord) -> i.PrimaryKey = "1" && i.SortKey.StartsWith "2" @>
        test <@ set queriedTable = set (values |> Set.filter (fun i -> i.PrimaryKey = "1" && i.SortKey.StartsWith "2")) @>
        let queriedGSI = table.Query <@ fun (i: InverseKeyRecord) -> i.SortKey = "1" && i.PrimaryKey.StartsWith "2" @>
        test <@ set queriedGSI = set (values |> Set.filter (fun i -> i.SortKey = "1" && i.PrimaryKey.StartsWith "2")) @>

    interface IClassFixture<TableFixture>

type ``Inverse GSI Table with Custom Names Operation Tests``(fixture: TableFixture) =

    let rand = let r = Random.Shared in fun () -> r.Next() |> int64
    let mkItem () =
        let hashKey = ((int (rand ())) % 50).ToString()
        let sortKey = ((int (rand ())) % 50).ToString()
        { PrimaryKey = hashKey
          SortKey = sortKey
          GSIReverseHashKey = sortKey
          GSIReverseRangeKey = hashKey }

    let table = fixture.CreateEmpty<InverseKeyRecordUsingCustomName>()

    [<Fact>]
    let ``Query by Table Key and GSI`` () =
        let values = set [ for _ in 1L .. 1000L -> mkItem () ]
        for batch in values |> Set.toSeq |> Seq.chunkBySize 25 do
            table.BatchPutItems batch =! [||]
        let queriedTable = table.Query <@ fun (i: InverseKeyRecordUsingCustomName) -> i.PrimaryKey = "1" && i.SortKey.StartsWith "2" @>
        test <@ set queriedTable = set (values |> Set.filter (fun i -> i.PrimaryKey = "1" && i.SortKey.StartsWith "2")) @>
        let queriedGSI = table.Query <@ fun (i: InverseKeyRecordUsingCustomName) -> i.SortKey = "1" && i.PrimaryKey.StartsWith "2" @>
        test <@ set queriedGSI = set (values |> Set.filter (fun i -> i.SortKey = "1" && i.PrimaryKey.StartsWith "2")) @>
        let queriedGSIWithCustomName =
            table.Query <@ fun (i: InverseKeyRecordUsingCustomName) -> i.GSIReverseHashKey = "1" && i.GSIReverseRangeKey.StartsWith "2" @>
        test
            <@
                set queriedGSIWithCustomName = set (
                    values |> Set.filter (fun i -> i.GSIReverseHashKey = "1" && i.GSIReverseRangeKey.StartsWith "2")
                )
            @>

    interface IClassFixture<TableFixture>

type ``Shared Range Key Table Operation Tests``(fixture: TableFixture) =

    let rand = let r = Random.Shared in fun () -> r.Next() |> int64

    let mkItem () =
        { HashKey = guid ()
          GSI1 = ((int (rand ())) % 5).ToString()
          GSI2 = ((int (rand ())) % 5 + 20).ToString()
          SortKey = ((int (rand ())) % 50).ToString() }

    let table = fixture.CreateEmpty<SharedRangeKeyRecord>()

    [<Fact>]
    let ``Query by GSIs with shared range key`` () =
        let values = set [ for _ in 1L .. 1000L -> mkItem () ]
        for batch in values |> Set.toSeq |> Seq.chunkBySize 25 do
            table.BatchPutItems batch =! [||]
        let queried1 = table.Query <@ fun (i: SharedRangeKeyRecord) -> i.GSI1 = "1" && i.SortKey = "23" @>
        test <@ set queried1 = set (values |> Set.filter (fun i -> i.GSI1 = "1" && i.SortKey = "23")) @>
        let queried2 = table.Query <@ fun (i: SharedRangeKeyRecord) -> i.GSI2 = "2" && i.SortKey = "25" @>
        test <@ set queried2 = set (values |> Set.filter (fun i -> i.GSI2 = "2" && i.SortKey = "25")) @>

    interface IClassFixture<TableFixture>
