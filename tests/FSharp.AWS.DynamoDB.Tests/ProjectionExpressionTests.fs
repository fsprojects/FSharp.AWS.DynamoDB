namespace FSharp.AWS.DynamoDB.Tests

open System
open System.Threading

open Microsoft.FSharp.Quotations

open Xunit
open FsUnit.Xunit

open FSharp.AWS.DynamoDB

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

            [<BinaryFormatter>]
            Serialized : int64 * string

            [<BinaryFormatter>]
            Serialized2 : Nested
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
            Map = seq { for i in 0L .. rand() % 5L -> "K" + guid(), rand() } |> Map.ofSeq 
            IntSet = seq { for i in 0L .. rand() % 5L -> rand() } |> Set.ofSeq
            StringSet = seq { for i in 0L .. rand() % 5L -> guid() } |> Set.ofSeq
            ByteSet = seq { for i in 0L .. rand() % 5L -> bytes() } |> Set.ofSeq
            List = [for i in 0L .. rand() % 5L -> rand() ]
            Union = if rand() % 2L = 0L then UA (rand()) else UB(guid())
            Unions = [for i in 0L .. rand() % 5L -> if rand() % 2L = 0L then UA (rand()) else UB(guid()) ]
            Serialized = rand(), guid() ; Serialized2 = { NV = guid() ; NE = enum<Enum> (int (rand()) % 3) } ;
        }

    let table = TableContext.Create<ProjectionExprRecord>(fixture.Client, fixture.TableName, createIfNotExists = true)

    [<Fact>]
    let ``Should fail on invalid projections`` () =
        let testProj (p : Expr<R -> 'T>) = 
            fun () -> proj p
            |> shouldFailwith<_, ArgumentException>

        testProj <@ fun r -> 1 @>
        testProj <@ fun r -> Guid.Empty @>
        testProj <@ fun r -> not r.Bool @>
        testProj <@ fun r -> r.List.[0] + 1L @>

    [<Fact>]
    let ``Should fail on conflicting projections`` () =
        let testProj (p : Expr<R -> 'T>) = 
            fun () -> proj p
            |> shouldFailwith<_, ArgumentException>

        testProj <@ fun r -> r.Bool, r.Bool @>
        testProj <@ fun r -> r.NestedList.[0].NE, r.NestedList.[0] @>

    [<Fact>]
    let ``Null value projection`` () =
        let item = mkItem()
        let key = table.PutItem(item)
        table.GetItemProjected(key, <@ fun r -> () @>)
        table.GetItemProjected(key, <@ ignore @>)

    [<Fact>]
    let ``Single value projection`` () =
        let item = mkItem()
        let key = table.PutItem(item)
        let guid = table.GetItemProjected(key, <@ fun r -> r.Guid @>)
        guid |> should equal item.Guid

    [<Fact>]
    let ``Map projection`` () =
        let item = mkItem()
        let key = table.PutItem(item)
        let map = table.GetItemProjected(key, <@ fun r -> r.Map @>)
        map |> should equal item.Map

    [<Fact>]
    let ``Option-None projection`` () =
        let item = { mkItem() with Optional = None }
        let key = table.PutItem(item)
        let opt = table.GetItemProjected(key, <@ fun r -> r.Optional @>)
        opt |> should equal None

    [<Fact>]
    let ``Option-Some projection`` () =
        let item = { mkItem() with Optional = Some "test" }
        let key = table.PutItem(item)
        let opt = table.GetItemProjected(key, <@ fun r -> r.Optional @>)
        opt |> should equal item.Optional

    [<Fact>]
    let ``Multi-value projection`` () =
        let item = mkItem()
        let key = table.PutItem(item)
        let result = table.GetItemProjected(key, <@ fun r -> r.Bool, r.ByteSet, r.Bytes, r.Serialized2 @>)
        result |> should equal (item.Bool, item.ByteSet, item.Bytes, item.Serialized2)


    [<Fact>]
    let ``Nested value projection 1`` () =
        let item = { mkItem() with Map = Map.ofList ["Nested", 42L ] }
        let key = table.PutItem(item)
        let result = table.GetItemProjected(key, <@ fun r -> r.Nested.NV, r.NestedList.[0].NV, r.Map.["Nested"] @>)
        result |> should equal (item.Nested.NV, item.NestedList.[0].NV, item.Map.["Nested"])

    [<Fact>]
    let ``Nested value projection 2`` () =
        let item = { mkItem() with List = [1L;2L;3L] }
        let key = table.PutItem(item)
        let result = table.GetItemProjected(key, <@ fun r -> r.List.[0], r.List.[1] @>)
        result |> should equal (item.List.[0], item.List.[1])

    [<Fact>]
    let ``Projected query`` () =
        let hKey = guid()

        seq { for i in 1 .. 200 -> { mkItem() with HashKey = hKey ; RangeKey = string i }}
        |> Seq.splitInto 25
        |> Seq.map table.BatchPutItemsAsync
        |> Async.Parallel
        |> Async.Ignore
        |> Async.RunSynchronously

        let results = table.QueryProjected(<@ fun r -> r.HashKey = hKey @>, <@ fun r -> r.RangeKey @>)
        results |> Seq.map int |> set |> should equal (set [1 .. 200])

    [<Fact>]
    let ``Projected scan`` () =
        let hKey = guid()

        seq { for i in 1 .. 200 -> { mkItem() with HashKey = hKey ; RangeKey = string i }}
        |> Seq.splitInto 25
        |> Seq.map table.BatchPutItemsAsync
        |> Async.Parallel
        |> Async.Ignore
        |> Async.RunSynchronously

        let results = table.ScanProjected(<@ fun r -> r.RangeKey @>, filterCondition = <@ fun r -> r.HashKey = hKey @>)
        results |> Seq.map int |> set |> should equal (set [1 .. 200])
        

    interface IClassFixture<TableFixture>