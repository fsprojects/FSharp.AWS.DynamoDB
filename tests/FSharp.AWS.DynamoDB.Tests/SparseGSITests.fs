namespace FSharp.AWS.DynamoDB.Tests

open System

open Swensen.Unquote
open Xunit

open FSharp.AWS.DynamoDB
open FSharp.AWS.DynamoDB.Scripting

[<AutoOpen>]
module SparseGSITests =

    type GsiRecord =
        {
            [<HashKey>]
            HashKey : string
            [<RangeKey>]
            RangeKey : string

            [<GlobalSecondaryHashKey("GSI")>]
            SecondaryHashKey : string option
        }

type ``Sparse GSI Tests`` (fixture : TableFixture) =

    let rand = let r = Random() in fun () -> int64 <| r.Next()
    let mkItem() =
        {
            HashKey = guid() ; RangeKey = guid() ;
            SecondaryHashKey = if rand() % 2L = 0L then Some (guid()) else None ;
        }

    let table = fixture.CreateEmpty<GsiRecord>()

    let [<Fact>] ``GSI Put Operation`` () =
        let value = mkItem()
        let key = table.PutItem value
        let value' = table.GetItem key
        test <@ value = value' @>

    let [<Fact>] ``GSI Query Operation (match)`` () =
        let value = { mkItem() with SecondaryHashKey = Some(guid()) }
        let _key = table.PutItem value
        let res = table.Query(keyCondition = <@ fun (r: GsiRecord) -> r.SecondaryHashKey = value.SecondaryHashKey @>)
        test <@ 1 = Array.length res @>

    let [<Fact>] ``GSI Query Operation (missing)`` () =
        let value = { mkItem() with SecondaryHashKey = Some(guid()) }
        let key = table.PutItem value
        table.UpdateItem(key, <@ fun r -> { r with SecondaryHashKey = None } @>) |> ignore
        let res = table.Query(keyCondition = <@ fun (r: GsiRecord) -> r.SecondaryHashKey = value.SecondaryHashKey @>)
        test <@ Array.isEmpty res @>

    interface IClassFixture<TableFixture>
