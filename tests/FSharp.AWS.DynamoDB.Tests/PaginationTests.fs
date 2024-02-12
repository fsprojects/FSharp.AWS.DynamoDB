namespace FSharp.AWS.DynamoDB.Tests

open System

open Swensen.Unquote
open Xunit

open FSharp.AWS.DynamoDB
open FSharp.AWS.DynamoDB.Scripting

[<AutoOpen>]
module PaginationTests =

    type PaginationRecord =
        { [<HashKey>]
          HashKey: string
          [<RangeKey>]
          RangeKey: string

          [<LocalSecondaryIndex>]
          LocalSecondaryRangeKey: string

          [<GlobalSecondaryHashKey("GSI")>]
          SecondaryHashKey: string
          [<GlobalSecondaryRangeKey("GSI")>]
          SecondaryRangeKey: string

          LocalAttribute: int }


type ``Pagination Tests``(fixture: TableFixture) =

    let rand = let r = Random.Shared in fun () -> int64 <| r.Next()
    let mkItem (hk: string) (gshk: string) : PaginationRecord =
        { HashKey = hk
          RangeKey = guid ()
          LocalSecondaryRangeKey = guid ()
          SecondaryHashKey = gshk
          SecondaryRangeKey = guid ()
          LocalAttribute = int (rand () % 2L) }

    let table = fixture.CreateEmpty<PaginationRecord>()

    [<Fact>]
    let ``Paginated Query on Primary Key`` () =
        let hk = guid ()
        let gsk = guid ()
        let items = seq { for _ in 0..9 -> mkItem hk gsk } |> Seq.toArray |> Array.sortBy (fun r -> r.RangeKey)
        for item in items do
            table.PutItem item =! TableKey.Combined(item.HashKey, item.RangeKey)
        let res1 = table.QueryPaginated(<@ fun r -> r.HashKey = hk @>, limit = 5)
        let res2 = table.QueryPaginated(<@ fun r -> r.HashKey = hk @>, limit = 5, ?exclusiveStartKey = res1.LastEvaluatedKey)
        let res3 = table.QueryPaginated(<@ fun r -> r.HashKey = hk @>, limit = 5, ?exclusiveStartKey = res2.LastEvaluatedKey)
        test <@ None <> res1.LastEvaluatedKey && None <> res2.LastEvaluatedKey && None = res3.LastEvaluatedKey @>
        test <@ items = Array.append res1.Records res2.Records && Array.isEmpty res3.Records @>

    [<Fact>]
    let ``Paginated Query on LSI`` () =
        let hk = guid ()
        let gsk = guid ()
        let items =
            seq { for _ in 0..9 -> mkItem hk gsk }
            |> Seq.toArray
            |> Array.sortBy (fun r -> r.LocalSecondaryRangeKey)
        for item in items do
            table.PutItem item =! TableKey.Combined(item.HashKey, item.RangeKey)
        let res1 = table.QueryPaginated(<@ fun r -> r.HashKey = hk && r.LocalSecondaryRangeKey > "0" @>, limit = 5)
        let res2 =
            table.QueryPaginated(
                <@ fun r -> r.HashKey = hk && r.LocalSecondaryRangeKey > "0" @>,
                limit = 5,
                ?exclusiveStartKey = res1.LastEvaluatedKey
            )
        let res3 =
            table.QueryPaginated(
                <@ fun r -> r.HashKey = hk && r.LocalSecondaryRangeKey > "0" @>,
                limit = 5,
                ?exclusiveStartKey = res2.LastEvaluatedKey
            )
        test <@ None <> res1.LastEvaluatedKey && None <> res2.LastEvaluatedKey && None = res3.LastEvaluatedKey @>
        test <@ items = Array.append res1.Records res2.Records && Array.isEmpty res3.Records @>

    [<Fact>]
    let ``Paginated Query on GSI`` () =
        let hk = guid ()
        let gsk = guid ()
        let items = seq { for _ in 0..9 -> mkItem hk gsk } |> Seq.toArray |> Array.sortBy (fun r -> r.SecondaryRangeKey)
        for item in items do
            table.PutItem item =! TableKey.Combined(item.HashKey, item.RangeKey)
        let res1 = table.QueryPaginated(<@ fun r -> r.SecondaryHashKey = gsk @>, limit = 5)
        let res2 = table.QueryPaginated(<@ fun r -> r.SecondaryHashKey = gsk @>, limit = 5, ?exclusiveStartKey = res1.LastEvaluatedKey)
        let res3 = table.QueryPaginated(<@ fun r -> r.SecondaryHashKey = gsk @>, limit = 5, ?exclusiveStartKey = res2.LastEvaluatedKey)
        test <@ None <> res1.LastEvaluatedKey && None <> res2.LastEvaluatedKey && None = res3.LastEvaluatedKey @>
        test <@ items = Array.append res1.Records res2.Records && Array.isEmpty res3.Records @>

    [<Fact>]
    let ``Paginated Query with filter`` () =
        let hk = guid ()
        let gsk = guid ()
        let items = seq { for _ in 0..49 -> mkItem hk gsk } |> Seq.toArray |> Array.sortBy (fun r -> r.RangeKey)
        for item in items do
            table.PutItem item =! TableKey.Combined(item.HashKey, item.RangeKey)
        let res = table.QueryPaginated(<@ fun r -> r.HashKey = hk @>, filterCondition = <@ fun r -> r.LocalAttribute = 0 @>, limit = 5)
        test <@ items |> Array.filter (fun r -> r.LocalAttribute = 0) |> Array.take 5 = res.Records @>

    interface IClassFixture<TableFixture>
