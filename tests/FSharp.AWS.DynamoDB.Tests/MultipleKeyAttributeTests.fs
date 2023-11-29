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

type ``Multi Key Attribute Table Operation Tests`` (fixture : TableFixture) =

    let rand = let r = Random() in fun () -> int64 <| r.Next()
    let mkInverseItem() =
        {
            PrimaryKey = ((int (rand ())) % 50).ToString ()
            SortKey = ((int (rand ())) % 50).ToString ()
        }

    let table = fixture.CreateEmpty<InverseKeyRecord>()

    let [<Fact>] ``Query by Table Key`` () =
        let values = set [ for _ in 1L .. 1000L -> mkInverseItem() ]
        for batch in values |> Set.toSeq |> Seq.chunkBySize 25 do
            table.BatchPutItems batch |> ignore
        let queried = table.Query <@ fun (i : InverseKeyRecord) -> i.PrimaryKey = "1" && i.SortKey.StartsWith "2" @>
        test <@ set queried = set (values |> Set.filter (fun i -> i.PrimaryKey = "1" && i.SortKey.StartsWith "2") )@>

    let [<Fact>] ``Query by GSI`` () =
        let values = set [ for _ in 1L .. 1000L -> mkInverseItem() ]
        for batch in values |> Set.toSeq |> Seq.chunkBySize 25 do
            table.BatchPutItems batch |> ignore
        let queried = table.Query <@ fun (i : InverseKeyRecord) -> i.SortKey = "1" && i.PrimaryKey.StartsWith "2" @>
        test <@ set queried = set (values |> Set.filter (fun i -> i.SortKey = "1" && i.PrimaryKey.StartsWith "2") )@>

    interface IClassFixture<TableFixture>
