namespace FSharp.AWS.DynamoDB.Tests

open System
open System.Threading

open Expecto

open FSharp.AWS.DynamoDB

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

    let table = TableContext.Create<GsiRecord>(fixture.Client, fixture.TableName, createIfNotExists = true)

    member this.``GSI Put Operation`` () =
        let value = mkItem()
        let key = table.PutItem value
        let value' = table.GetItem key
        Expect.equal value' value ""

    member this.``GSI Query Operation (match)`` () =
        let value = { mkItem() with SecondaryHashKey = Some(guid()) }
        let key = table.PutItem value
        Expect.equal
            (table.Query(keyCondition = <@ fun (r: GsiRecord) -> r.SecondaryHashKey = value.SecondaryHashKey @>) 
             |> Array.length)
            1
            ""

    member this.``GSI Query Operation (missing)`` () =
        let value = { mkItem() with SecondaryHashKey = Some(guid()) }
        let key = table.PutItem value
        table.UpdateItem(key, <@ fun r -> { r with SecondaryHashKey = None } @>) |> ignore
        Expect.equal
            (table.Query(keyCondition = <@ fun (r: GsiRecord) -> r.SecondaryHashKey = value.SecondaryHashKey @>) 
             |> Array.length)
            0
            ""