namespace FSharp.AWS.DynamoDB.Tests

open System

open Microsoft.FSharp.Quotations

open Swensen.Unquote
open Xunit

open FSharp.AWS.DynamoDB
open FSharp.AWS.DynamoDB.Scripting

[<AutoOpen>]
module ProjectionExprTypes =

    [<Flags>]
    type Enum = A = 1 | B = 2 | C = 4

    type Nested = { NV : string ; NE : Enum }

    type Union = UA of int64 | UB of string

    type ProjectionExprRecord =
        {
            [<HashKey>]
            HashKey : string
            [<RangeKey>]
            RangeKey : string

            Value : int64

            String : string

            Tuple : int64 * int64

            Nested : Nested

            NestedList : Nested list

            TimeSpan : TimeSpan

            DateTimeOffset : DateTimeOffset

            Guid : Guid

            Bool : bool

            Bytes : byte[]

            Ref : string ref

            Union : Union

            Unions : Union list

            Optional : string option

            List : int64 list

            Map : Map<string, int64>

            IntSet : Set<int64>

            StringSet : Set<string>

            ByteSet : Set<byte[]>
        }

    type R = ProjectionExprRecord

type ``Projection Expression Tests`` (fixture : TableFixture) =

    static let rand = let r = Random() in fun () -> int64 <| r.Next()

    let bytes() = Guid.NewGuid().ToByteArray()
    let mkItem() =
        {
            HashKey = guid() ; RangeKey = guid() ; String = guid()
            Value = rand() ; Tuple = rand(), rand() ;
            TimeSpan = TimeSpan.FromTicks(rand()) ; DateTimeOffset = DateTimeOffset.Now ; Guid = Guid.NewGuid()
            Bool = false ; Optional = Some (guid()) ; Ref = ref (guid()) ; Bytes = Guid.NewGuid().ToByteArray()
            Nested = { NV = guid() ; NE = enum<Enum> (int (rand()) % 3) } ;
            NestedList = [{ NV = guid() ; NE = enum<Enum> (int (rand()) % 3) } ]
            Map = seq { for _ in 0L .. rand() % 5L -> "K" + guid(), rand() } |> Map.ofSeq
            IntSet = seq { for _ in 0L .. rand() % 5L -> rand() } |> Set.ofSeq
            StringSet = seq { for _ in 0L .. rand() % 5L -> guid() } |> Set.ofSeq
            ByteSet = seq { for _ in 0L .. rand() % 5L -> bytes() } |> Set.ofSeq
            List = [for _ in 0L .. rand() % 5L -> rand() ]
            Union = if rand() % 2L = 0L then UA (rand()) else UB(guid())
            Unions = [for _ in 0L .. rand() % 5L -> if rand() % 2L = 0L then UA (rand()) else UB(guid()) ]
        }

    let table = fixture.CreateEmpty<ProjectionExprRecord>()

    let [<Fact>] ``Should fail on invalid projections`` () =
        let testProj (p : Expr<R -> 'T>) =
            fun () -> proj p
            |> shouldFailwith<_, ArgumentException>

        testProj <@ fun _ -> 1 @>
        testProj <@ fun _ -> Guid.Empty @>
        testProj <@ fun r -> not r.Bool @>
        testProj <@ fun r -> r.List[0] + 1L @>

    let [<Fact>] ``Should fail on conflicting projections`` () =
        let testProj (p : Expr<R -> 'T>) =
            fun () -> proj p
            |> shouldFailwith<_, ArgumentException>

        testProj <@ fun r -> r.Bool, r.Bool @>
        testProj <@ fun r -> r.NestedList[0].NE, r.NestedList[0] @>

    let [<Fact>] ``Null value projection`` () =
        let item = mkItem()
        let key = table.PutItem(item)
        table.GetItemProjected(key, <@ fun _ -> () @>)
        table.GetItemProjected(key, <@ ignore @>)

    let [<Fact>] ``Single value projection`` () =
        let item = mkItem()
        let key = table.PutItem(item)
        let guid = table.GetItemProjected(key, <@ fun r -> r.Guid @>)
        test <@ item.Guid = guid @>

    let [<Fact>] ``Map projection`` () =
        let item = mkItem()
        let key = table.PutItem(item)
        let map = table.GetItemProjected(key, <@ fun r -> r.Map @>)
        test <@ item.Map = map @>

    let [<Fact>] ``Option-None projection`` () =
        let item = { mkItem() with Optional = None }
        let key = table.PutItem(item)
        let opt = table.GetItemProjected(key, <@ fun r -> r.Optional @>)
        test <@ None = opt @>

    let [<Fact>] ``Option-Some projection`` () =
        let item = { mkItem() with Optional = Some "test" }
        let key = table.PutItem(item)
        let opt = table.GetItemProjected(key, <@ fun r -> r.Optional @>)
        test <@ item.Optional = opt @>

    let [<Fact>] ``Multi-value projection`` () =
        let item = mkItem()
        let key = table.PutItem(item)
        let result = table.GetItemProjected(key, <@ fun r -> r.Bool, r.ByteSet, r.Bytes @>)
        test <@ (item.Bool, item.ByteSet, item.Bytes) = result @>

    let [<Fact>] ``Nested value projection 1`` () =
        let item = { mkItem() with Map = Map.ofList ["Nested", 42L ] }
        let key = table.PutItem(item)
        let result = table.GetItemProjected(key, <@ fun r -> r.Nested.NV, r.NestedList[0].NV, r.Map["Nested"] @>)
        test <@ (item.Nested.NV, item.NestedList[0].NV, item.Map["Nested"]) = result @>

    let [<Fact>] ``Nested value projection 2`` () =
        let item = { mkItem() with List = [1L;2L;3L] }
        let key = table.PutItem(item)
        let result = table.GetItemProjected(key, <@ fun r -> r.List[0], r.List[1] @>)
        test <@ (item.List[0], item.List[1]) = result @>

    let [<Fact>] ``Projected query`` () =
        let hKey = guid()

        seq { for i in 1 .. 200 -> { mkItem() with HashKey = hKey ; RangeKey = string i }}
        |> Seq.splitInto 25
        |> Seq.map table.BatchPutItemsAsync
        |> Async.Parallel
        |> Async.Ignore
        |> Async.RunSynchronously

        let results = table.QueryProjected(<@ fun r -> r.HashKey = hKey @>, <@ fun r -> r.RangeKey @>)
        test <@ set [1 .. 200] = (results |> Seq.map int |> set) @>

    let [<Fact>] ``Projected scan`` () =
        let hKey = guid()

        seq { for i in 1 .. 200 -> { mkItem() with HashKey = hKey ; RangeKey = string i }}
        |> Seq.splitInto 25
        |> Seq.map table.BatchPutItemsAsync
        |> Async.Parallel
        |> Async.Ignore
        |> Async.RunSynchronously

        let results = table.ScanProjected(<@ fun r -> r.RangeKey @>, filterCondition = <@ fun r -> r.HashKey = hKey @>)
        test <@ set [1 .. 200] = (results |> Seq.map int |> set) @>

    interface IClassFixture<TableFixture>
