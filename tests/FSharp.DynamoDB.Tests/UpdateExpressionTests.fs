namespace FSharp.DynamoDB.Tests

open System
open System.Threading

open Xunit
open FsUnit.Xunit

open FSharp.DynamoDB

[<AutoOpen>]
module UpdateExprTypes =

    type Enum = A = 0 | B = 1 | C = 2

    type Nested = { NV : string ; NE : Enum }

    type Union = UA of int64 | UB of string

    type UpdateExprRecord =
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

            Set : Set<int64>

            [<BinaryFormatter>]
            Serialized : int64 * string

            [<BinaryFormatter>]
            Serialized2 : Nested
        }

    type R = UpdateExprRecord

type ``Update Expression Tests`` () =

    let client = getDynamoDBAccount()
    let tableName = getRandomTableName()

    let rand = let r = Random() in fun () -> int64 <| r.Next()
    let mkItem() = 
        { 
            HashKey = guid() ; RangeKey = guid() ; String = guid()
            Value = rand() ; Tuple = rand(), rand() ;
            TimeSpan = TimeSpan.FromTicks(rand()) ; DateTimeOffset = DateTimeOffset.Now ; Guid = Guid.NewGuid()
            Bool = false ; Optional = Some (guid()) ; Ref = ref (guid()) ; Bytes = Guid.NewGuid().ToByteArray()
            Nested = { NV = guid() ; NE = enum<Enum> (int (rand()) % 3) } ;
            NestedList = [{ NV = guid() ; NE = enum<Enum> (int (rand()) % 3) } ]
            Map = seq { for i in 0L .. rand() % 5L -> "K" + guid(), rand() } |> Map.ofSeq 
            Set = seq { for i in 0L .. rand() % 5L -> rand() } |> Set.ofSeq
            List = [for i in 0L .. rand() % 5L -> rand() ]
            Union = if rand() % 2L = 0L then UA (rand()) else UB(guid())
            Unions = [for i in 0L .. rand() % 5L -> if rand() % 2L = 0L then UA (rand()) else UB(guid()) ]
            Serialized = rand(), guid() ; Serialized2 = { NV = guid() ; NE = enum<Enum> (int (rand()) % 3) } ;
        }

    let table = TableContext.Create<UpdateExprRecord>(client, tableName, createIfNotExists = true)

    [<Fact>]
    let ``Attempt to update HashKey`` () =
        let item = mkItem()
        let key = table.PutItem item
        fun () -> table.UpdateItem(key, <@ fun (r : R) -> { r with HashKey = guid() } @>)
        |> shouldFailwith<_, ArgumentException>

    [<Fact>]
    let ``Attempt to update RangeKey`` () =
        let item = mkItem()
        let key = table.PutItem item
        fun () -> table.UpdateItem(key, <@ fun (r : R) -> { r with RangeKey = guid() } @>)
        |> shouldFailwith<_, ArgumentException>

    [<Fact>]
    let ``Returning old value`` () =
        let item = mkItem()
        let key = table.PutItem item
        let item' = table.UpdateItem(key, <@ fun (r : R) -> { r with Value = r.Value + 1L } @>, returnLatest = false)
        item' |> should equal item

    [<Fact>]
    let ``Simple update DateTimeOffset`` () =
        let item = mkItem()
        let key = table.PutItem item
        let nv = DateTimeOffset.Now + TimeSpan.FromDays 366.
        let item' = table.UpdateItem(key, <@ fun (r : R) -> { r with DateTimeOffset = nv } @>)
        item'.DateTimeOffset |> should equal nv

    [<Fact>]
    let ``Simple update TimeSpan`` () =
        let item = mkItem()
        let key = table.PutItem item
        let ts = TimeSpan.FromTicks(rand())
        let item' = table.UpdateItem(key, <@ fun (r : R) -> { r with TimeSpan = ts } @>)
        item'.TimeSpan |> should equal ts

    [<Fact>]
    let ``Simple update Guid`` () =
        let item = mkItem()
        let key = table.PutItem item
        let g = Guid.NewGuid()
        let item' = table.UpdateItem(key, <@ fun (r : R) -> { r with Guid = g } @>)
        item'.Guid |> should equal g

    [<Fact>]
    let ``Simple increment update`` () =
        let item = mkItem()
        let key = table.PutItem item
        let item' = table.UpdateItem(key, <@ fun (r : R) -> { r with Value = r.Value + 1L } @>)
        item'.Value |> should equal (item.Value + 1L)

    [<Fact>]
    let ``Simple decrement update`` () =
        let item = mkItem()
        let key = table.PutItem item
        let item' = table.UpdateItem(key, <@ fun (r : R) -> { r with Value = r.Value - 10L } @>)
        item'.Value |> should equal (item.Value - 10L)

    [<Fact>]
    let ``Simple update serialized value`` () =
        let item = mkItem()
        let key = table.PutItem item
        let value' = rand(), guid()
        let item' = table.UpdateItem(key, <@ fun (r : R) -> { r with Serialized = value' } @>)
        item'.Serialized |> should equal value'

    [<Fact>]
    let ``Update using nested record values`` () =
        let item = mkItem()
        let key = table.PutItem item
        let item' = table.UpdateItem(key, <@ fun (r : R) -> { r with String = r.Nested.NV } @>)
        item'.String |> should equal item.Nested.NV

    [<Fact>]
    let ``Update using nested union values`` () =
        let item = mkItem()
        let key = table.PutItem item
        let u = UB(guid())
        let item' = table.UpdateItem(key, <@ fun (r : R) -> { r with Union = u } @>)
        item'.Union |> should equal u

    [<Fact>]
    let ``Update using nested list`` () =
        let item = mkItem()
        let key = table.PutItem item
        let item' = table.UpdateItem(key, <@ fun (r : R) -> { r with Nested = r.NestedList.[0] } @>)
        item'.Nested |> should equal item.NestedList.[0]

    [<Fact>]
    let ``Update using tuple values`` () =
        let item = mkItem()
        let key = table.PutItem item
        let item' = table.UpdateItem(key, <@ fun (r : R) -> { r with Value = fst r.Tuple + 1L } @>)
        item'.Value |> should equal (fst item.Tuple + 1L)

    [<Fact>]
    let ``Update optional field to None`` () =
        let item = { mkItem() with Optional = Some (guid()) }
        let key = table.PutItem item
        let item' = table.UpdateItem(key, <@ fun (r : R) -> { r with Optional = None } @>)
        item'.Optional |> should equal None

    [<Fact>]
    let ``Update optional field to Some`` () =
        let item = { mkItem() with Optional = None }
        let key = table.PutItem item
        let item' = table.UpdateItem(key, <@ fun (r : R) -> { r with Optional = Some(guid()) } @>)
        item'.Optional.IsSome |> should equal true

    [<Fact>]
    let ``Update list field to non-empty`` () =
        let item = { mkItem() with List = [1L] }
        let key = table.PutItem item
        let nv = [for i in 1 .. 10 -> rand() ]
        let item' = table.UpdateItem(key, <@ fun (r : R) -> { r with List = nv } @>)
        item'.List |> should equal nv

    [<Fact>]
    let ``Update list field to empty`` () =
        let item = { mkItem() with List = [1L] }
        let key = table.PutItem item
        let item' = table.UpdateItem(key, <@ fun (r : R) -> { r with List = [] } @>)
        item'.List.Length |> should equal 0

    [<Fact>]
    let ``Update list with concatenation`` () =
        let item = { mkItem() with List = [1L] }
        let key = table.PutItem item
        let item' = table.UpdateItem(key, <@ fun (r : R) -> { r with List = r.List @ r.List } @>)
        item'.List |> should equal (item.List @ item.List)

    [<Fact>]
    let ``Update list with consing`` () =
        let item = { mkItem() with List = [2L] }
        let key = table.PutItem item
        let item' = table.UpdateItem(key, <@ fun (r : R) -> { r with List = 1L :: r.List } @>)
        item'.List |> should equal [1L ; 2L]

    [<Fact>]
    let ``Update using defaultArg combinator (Some)`` () =
        let item = { mkItem() with Optional = Some (guid()) }
        let key = table.PutItem item
        let item' = table.UpdateItem(key, <@ fun (r : R) -> { r with String = defaultArg r.Optional "<undefined>" } @>)
        item'.String |> should equal item.Optional.Value

    [<Fact>]
    let ``Update using defaultArg combinator (None)`` () =
        let item = { mkItem() with Optional = None }
        let key = table.PutItem item
        let item' = table.UpdateItem(key, <@ fun (r : R) -> { r with String = defaultArg r.Optional "<undefined>" } @>)
        item'.String |> should equal "<undefined>"

    [<Fact>]
    let ``Update set with add element`` () =
        let item = { mkItem() with Set = set [1L;2L] }
        let key = table.PutItem item
        let item' = table.UpdateItem(key, <@ fun (r : R) -> { r with Set = r.Set |> Set.add 3L } @>)
        item'.Set.Contains 3L |> should equal true

    [<Fact>]
    let ``Update set with remove element`` () =
        let item = { mkItem() with Set = set [1L;2L] }
        let key = table.PutItem item
        let item' = table.UpdateItem(key, <@ fun (r : R) -> { r with Set = r.Set |> Set.remove 2L } @>)
        item'.Set.Contains 2L |> should equal false

    [<Fact>]
    let ``Update set with append set`` () =
        let item = { mkItem() with Set = set [1L;2L] }
        let key = table.PutItem item
        let item' = table.UpdateItem(key, <@ fun (r : R) -> { r with Set = r.Set + set [3L] } @>)
        item'.Set.Contains 3L |> should equal true

    [<Fact>]
    let ``Update set with remove set`` () =
        let item = { mkItem() with Set = set [1L;2L] }
        let key = table.PutItem item
        let item' = table.UpdateItem(key, <@ fun (r : R) -> { r with Set = r.Set - set [1L;2L;3L] } @>)
        item'.Set.Count |> should equal 0

    [<Fact>]
    let ``Update map with add element`` () =
        let item = { mkItem() with Map = Map.ofList [("A", 1L) ; ("B", 2L)] }
        let key = table.PutItem item
        let item' = table.UpdateItem(key, <@ fun (r : R) -> { r with Map = r.Map |> Map.add "C" 3L } @>)
        item'.Map.TryFind "C" |> should equal (Some 3L)

    [<Fact>]
    let ``Update map with remove element`` () =
        let item = { mkItem() with Map = Map.ofList [("A", 1L) ; ("B", 2L)] }
        let key = table.PutItem item
        let item' = table.UpdateItem(key, <@ fun (r : R) -> { r with Map = r.Map |> Map.remove "B" } @>)
        item'.Map.ContainsKey "B" |> should equal false

    [<Fact>]
    let ``Update map with remove element on existing`` () =
        let item = { mkItem() with Map = Map.ofList [("A", 1L) ; ("B", 2L)] }
        let key = table.PutItem item
        let item' = table.UpdateItem(key, <@ fun (r : R) -> { r with Map = r.Map |> Map.remove "C" } @>)
        item'.Map.Count |> should equal 2

    [<Fact>]
    let ``Combined update with succesful precondition`` () =
        let item = mkItem()
        let key = table.PutItem item
        let item' = table.UpdateItem(key, <@ fun (r : R) -> { r with Value = r.Value + 1L } @>,
                                               precondition = <@ fun r -> r.Value = item.Value @>)

        item'.Value |> should equal (item.Value + 1L)

    [<Fact>]
    let ``Combined update with failed precondition`` () =
        let item = mkItem()
        let key = table.PutItem item
        fun () -> table.UpdateItem(key, <@ fun (r : R) -> { r with Value = r.Value + 1L } @>,
                                               precondition = <@ fun r -> r.Value = item.Value + 1L @>)

        |> shouldFailwith<_, ConditionalCheckFailedException>

        let item' = table.GetItem key
        item'.Value |> should equal item.Value


    [<Fact>]
    let ``SET an attribute`` () =
        let item = mkItem()
        let key = table.PutItem item
        let item' = table.UpdateItem(key, <@ fun r -> SET r.NestedList.[0].NV item.HashKey &&&
                                                                 SET r.NestedList.[1] { NV = item.HashKey ; NE = Enum.C } @>)

        item'.NestedList.[0].NV |> should equal item.HashKey
        item'.NestedList.[1].NV |> should equal item.HashKey

    [<Fact>]
    let ``SET a union attribute`` () =
        let item = { mkItem() with Unions = [UB(guid())] }
        let key = table.PutItem item
        let u = UA(rand())
        let item' = table.UpdateItem(key, <@ fun r -> SET r.Unions.[0] u @>)

        item'.Unions.Length |> should equal 1
        item'.Unions.[0] |> should equal u

    [<Fact>]
    let ``REMOVE an attribute`` () =
        let item = { mkItem() with NestedList = [{NV = "foo" ; NE = Enum.A}] }
        let key = table.PutItem item
        let item' = table.UpdateItem(key, <@ fun r -> REMOVE r.NestedList.[0] @>)

        item'.NestedList.Length |> should equal 0

    [<Fact>]
    let ``ADD to set`` () =
        let item = mkItem()
        let key = table.PutItem item
        let item' = table.UpdateItem(key, <@ fun r -> ADD r.Set [42L] @>)

        item'.Set.Contains 42L |> should equal true

    [<Fact>]
    let ``DELETE from set`` () =
        let item = { mkItem() with Set = set [1L ; 42L] }
        let key = table.PutItem item
        let item' = table.UpdateItem(key, <@ fun r -> DELETE r.Set [42L] @>)

        item'.Set.Contains 42L |> should equal false
        item'.Set.Count |> should equal 1

    [<Fact>]
    let ``Detect overlapping paths`` () =
        let item = mkItem()
        let key = table.PutItem item
        fun () -> table.UpdateItem(key, <@ fun r -> SET r.NestedList.[0].NV "foo" &&& 
                                                               REMOVE r.NestedList @>)

        |> shouldFailwith<_, ArgumentException>

        let item' = table.GetItem key
        item'.Value |> should equal item.Value

    [<Fact>]
    let ``Simple Parametric Updater 1`` () =
        let item = mkItem()
        let key = table.PutItem item
        let cond = table.Template.PrecomputeUpdateExpr <@ fun v1 v2 r -> { r with Value = v1 ; String = v2 } @>
        let v1 = rand()
        let v2 = guid()
        let result = table.UpdateItem(key, cond v1 v2)
        result.Value |> should equal v1
        result.String |> should equal v2

    [<Fact>]
    let ``Simple Parametric Updater 2`` () =
        let item = mkItem()
        let key = table.PutItem item
        let cond = table.Template.PrecomputeUpdateExpr <@ fun v1 v2 r -> SET r.Value v1 &&& ADD r.Set v2 @>
        let v1 = rand()
        let v2 = [ for i in 1 .. 10 -> rand()]
        let result = table.UpdateItem(key, cond v1 v2)
        result.Value |> should equal v1
        for v in v2 do result.Set.Contains v |> should equal true

    [<Fact>]
    let ``Parametric Updater with optional argument`` () =
        let item = { mkItem() with Optional = Some (guid()) }
        let key = table.PutItem item
        let cond = table.Template.PrecomputeUpdateExpr <@ fun opt (r : R) -> { r with Optional = opt } @>
        let result = table.UpdateItem(key, cond None)
        result.Optional |> should equal None

    [<Fact>]
    let ``Parametric Updater with heterogeneous argument consumption`` () =
        let item = mkItem()
        let key = table.PutItem item
        let values = [ for i in 1 .. 10 -> rand()]
        let cond = table.Template.PrecomputeUpdateExpr <@ fun vs r -> SET r.List vs &&& ADD r.Set vs @>
        let result = table.UpdateItem(key, cond values)
        result.List |> should equal values
        for v in values do result.Set.Contains v |> should equal true

    [<Fact>]
    let ``Parametric Updater with invalid param usage`` () =
        let template = table.Template
        fun () -> template.PrecomputeUpdateExpr <@ fun v (r : R) -> { r with Value = List.head v } @>
        |> shouldFailwith<_, ArgumentException>

        fun () -> template.PrecomputeUpdateExpr <@ fun v (r : R) -> ADD r.Set (1L :: v) @>
        |> shouldFailwith<_, ArgumentException>

    interface IDisposable with
        member __.Dispose() =
            ignore <| client.DeleteTable(tableName)