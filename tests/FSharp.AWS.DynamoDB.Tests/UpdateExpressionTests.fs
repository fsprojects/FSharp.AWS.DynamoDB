namespace FSharp.AWS.DynamoDB.Tests

open System

open Swensen.Unquote
open Xunit

open FSharp.AWS.DynamoDB
open FSharp.AWS.DynamoDB.Scripting

[<AutoOpen>]
module UpdateExprTypes =

    [<Flags>]
    type Enum = A = 1 | B = 2 | C = 4

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

            IntSet : Set<int64>

            StringSet : Set<string>

            ByteSet : Set<byte[]>

            [<BinaryFormatter>]
            Serialized : int64 * string

            [<BinaryFormatter>]
            Serialized2 : Nested
        }

    type R = UpdateExprRecord

type ``Update Expression Tests``(fixture : TableFixture) =

    let rand = let r = Random() in fun () -> int64 <| r.Next()
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
            Serialized = rand(), guid() ; Serialized2 = { NV = guid() ; NE = enum<Enum> (int (rand()) % 3) } ;
        }

    let table = fixture.CreateContextAndTableIfNotExists<UpdateExprRecord>()

    let [<Fact>] ``Attempt to update HashKey`` () =
        let item = mkItem()
        let key = table.PutItem item
        fun () -> table.UpdateItem(key, <@ fun (r : R) -> { r with HashKey = guid() } @>)
        |> shouldFailwith<_, ArgumentException>

    let [<Fact>] ``Attempt to update RangeKey`` () =
        let item = mkItem()
        let key = table.PutItem item
        fun () -> table.UpdateItem(key, <@ fun (r : R) -> { r with RangeKey = guid() } @>)
        |> shouldFailwith<_, ArgumentException>

    let [<Fact>] ``Returning old value`` () =
        let item = mkItem()
        let key = table.PutItem item
        let item' = table.UpdateItem(key, <@ fun (r : R) -> { r with Value = r.Value + 1L } @>, returnLatest = false)
        test <@ item = item' @>

    let [<Fact>] ``Simple update DateTimeOffset`` () =
        let item = mkItem()
        let key = table.PutItem item
        let nv = DateTimeOffset.Now + TimeSpan.FromDays 366.
        let item' = table.UpdateItem(key, <@ fun (r : R) -> { r with DateTimeOffset = nv } @>)
        test <@ nv = item'.DateTimeOffset @>

    let [<Fact>] ``Simple update TimeSpan`` () =
        let item = mkItem()
        let key = table.PutItem item
        let ts = TimeSpan.FromTicks(rand())
        let item' = table.UpdateItem(key, <@ fun (r : R) -> { r with TimeSpan = ts } @>)
        test <@ ts = item'.TimeSpan @>

    let [<Fact>] ``Simple update Guid`` () =
        let item = mkItem()
        let key = table.PutItem item
        let g = Guid.NewGuid()
        let item' = table.UpdateItem(key, <@ fun (r : R) -> { r with Guid = g } @>)
        test <@ g = item'.Guid @>

    let [<Fact>] ``Simple increment update`` () =
        let item = mkItem()
        let key = table.PutItem item
        let item' = table.UpdateItem(key, <@ fun (r : R) -> { r with Value = r.Value + 1L } @>)
        test <@ item.Value + 1L = item'.Value @>

    let [<Fact>] ``Simple decrement update`` () =
        let item = mkItem()
        let key = table.PutItem item
        let item' = table.UpdateItem(key, <@ fun (r : R) -> { r with Value = r.Value - 10L } @>)
        test <@ item.Value - 10L = item'.Value @>

    let [<Fact>] ``Simple update serialized value`` () =
        let item = mkItem()
        let key = table.PutItem item
        let value' = rand(), guid()
        let item' = table.UpdateItem(key, <@ fun (r : R) -> { r with Serialized = value' } @>)
        test <@ value' = item'.Serialized @>

    let [<Fact>] ``Update using nested record values`` () =
        let item = mkItem()
        let key = table.PutItem item
        let item' = table.UpdateItem(key, <@ fun (r : R) -> { r with String = r.Nested.NV } @>)
        test <@ item.Nested.NV = item'.String @>

    let [<Fact>] ``Update using nested union values`` () =
        let item = mkItem()
        let key = table.PutItem item
        let u = UB(guid())
        let item' = table.UpdateItem(key, <@ fun (r : R) -> { r with Union = u } @>)
        test <@ u = item'.Union @>

    let [<Fact>] ``Update using nested list`` () =
        let item = mkItem()
        let key = table.PutItem item
        let item' = table.UpdateItem(key, <@ fun (r : R) -> { r with Nested = r.NestedList[0] } @>)
        test <@ item.NestedList[0] = item'.Nested @>

    let [<Fact>] ``Update using tuple values`` () =
        let item = mkItem()
        let key = table.PutItem item
        let item' = table.UpdateItem(key, <@ fun (r : R) -> { r with Value = fst r.Tuple + 1L } @>)
        test <@ fst item.Tuple + 1L = item'.Value @>

    let [<Fact>] ``Update optional field to None`` () =
        let item = { mkItem() with Optional = Some (guid()) }
        let key = table.PutItem item
        let item' = table.UpdateItem(key, <@ fun (r : R) -> { r with Optional = None } @>)
        test <@ None = item'.Optional @>

    let [<Fact>] ``Update optional field to Some`` () =
        let item = { mkItem() with Optional = None }
        let key = table.PutItem item
        let item' = table.UpdateItem(key, <@ fun (r : R) -> { r with Optional = Some(guid()) } @>)
        test <@ None <> item'.Optional @>

    let [<Fact>] ``Update list field to non-empty`` () =
        let item = { mkItem() with List = [1L] }
        let key = table.PutItem item
        let nv = [for _ in 1 .. 10 -> rand() ]
        let item' = table.UpdateItem(key, <@ fun (r : R) -> { r with List = nv } @>)
        test <@ nv = item'.List @>

    let [<Fact>] ``Update list field to empty`` () =
        let item = { mkItem() with List = [1L] }
        let key = table.PutItem item
        let item' = table.UpdateItem(key, <@ fun (r : R) -> { r with List = [] } @>)
        test <@ 0 = item'.List.Length @>

    let [<Fact>] ``Update list with concatenation`` () =
        let item = { mkItem() with List = [1L] }
        let key = table.PutItem item
        let item' = table.UpdateItem(key, <@ fun (r : R) -> { r with List = r.List @ r.List } @>)
        test <@ item.List @ item.List = item'.List @>

    let [<Fact>] ``Update list with consing`` () =
        let item = { mkItem() with List = [2L] }
        let key = table.PutItem item
        let item' = table.UpdateItem(key, <@ fun (r : R) -> { r with List = 1L :: r.List } @>)
        test <@ [1L ; 2L] = item'.List @>

    let [<Fact>] ``Update using defaultArg combinator (Some)`` () =
        let item = { mkItem() with Optional = Some (guid()) }
        let key = table.PutItem item
        let item' = table.UpdateItem(key, <@ fun (r : R) -> { r with String = defaultArg r.Optional "<undefined>" } @>)
        test <@ item.Optional = Some item'.String @>

    let [<Fact>] ``Update using defaultArg combinator (None)`` () =
        let item = { mkItem() with Optional = None }
        let key = table.PutItem item
        let item' = table.UpdateItem(key, <@ fun (r : R) -> { r with String = defaultArg r.Optional "<undefined>" } @>)
        test <@ "<undefined>" = item'.String @>

    let [<Fact>] ``Update int set with add element`` () =
        let item = { mkItem() with IntSet = set [1L;2L] }
        let key = table.PutItem item
        let item' = table.UpdateItem(key, <@ fun (r : R) -> { r with IntSet = r.IntSet |> Set.add 3L } @>)
        test <@ item'.IntSet.Contains 3L @>

    let [<Fact>] ``Update int set with remove element`` () =
        let item = { mkItem() with IntSet = set [1L;2L] }
        let key = table.PutItem item
        let item' = table.UpdateItem(key, <@ fun (r : R) -> { r with IntSet = r.IntSet |> Set.remove 2L } @>)
        test <@ not (item'.IntSet.Contains 2L) @>

    let [<Fact>] ``Update int set with append set`` () =
        let item = { mkItem() with IntSet = Set.empty }
        let key = table.PutItem item
        let item' = table.UpdateItem(key, <@ fun (r : R) -> { r with IntSet = r.IntSet + set [3L] } @>)
        test <@ item'.IntSet.Contains 3L @>

    let [<Fact>] ``Update int set with remove set`` () =
        let item = { mkItem() with IntSet = set [1L;2L] }
        let key = table.PutItem item
        let item' = table.UpdateItem(key, <@ fun (r : R) -> { r with IntSet = r.IntSet - set [1L;2L;3L] } @>)
        test <@ 0 = item'.IntSet.Count @>

    let [<Fact>] ``Update string set with add element`` () =
        let item = { mkItem() with StringSet = set ["1";"2"] }
        let key = table.PutItem item
        let item' = table.UpdateItem(key, <@ fun (r : R) -> { r with StringSet = r.StringSet |> Set.add "3" } @>)
        test <@ item'.StringSet.Contains "3" @>

    let [<Fact>] ``Update string set with remove element`` () =
        let item = { mkItem() with StringSet = set ["1";"2"] }
        let key = table.PutItem item
        let item' = table.UpdateItem(key, <@ fun (r : R) -> { r with StringSet = r.StringSet |> Set.remove "2" } @>)
        test <@ not (item'.StringSet.Contains "2") @>

    let [<Fact>] ``Update string set with append set`` () =
        let item = { mkItem() with StringSet = Set.empty }
        let key = table.PutItem item
        let item' = table.UpdateItem(key, <@ fun (r : R) -> { r with StringSet = r.StringSet + set ["3"] } @>)
        test <@ item'.StringSet.Contains "3" @>

    let [<Fact>] ``Update byte set with append set`` () =
        let item = { mkItem() with ByteSet = Set.empty }
        let key = table.PutItem item
        let item' = table.UpdateItem(key, <@ fun (r : R) -> { r with ByteSet = r.ByteSet + set [[|42uy|]] } @>)
        test <@ item'.ByteSet.Contains [|42uy|] @>

    let [<Fact>] ``Update string set with remove set`` () =
        let item = { mkItem() with StringSet = set ["1";"2"] }
        let key = table.PutItem item
        let item' = table.UpdateItem(key, <@ fun (r : R) -> { r with StringSet = r.StringSet - set ["1";"2";"3"] } @>)
        test <@ 0 = item'.StringSet.Count @>

    let [<Fact>] ``Update map with add element`` () =
        let item = { mkItem() with Map = Map.ofList [("A", 1L) ; ("B", 2L)] }
        let key = table.PutItem item
        let item' = table.UpdateItem(key, <@ fun (r : R) -> { r with Map = r.Map |> Map.add "C" 3L } @>)
        test <@ Some 3L = item'.Map.TryFind "C" @>

    let [<Fact>] ``Update map with remove element`` () =
        let item = { mkItem() with Map = Map.ofList [("A", 1L) ; ("B", 2L)] }
        let key = table.PutItem item
        let item' = table.UpdateItem(key, <@ fun (r : R) -> { r with Map = r.Map |> Map.remove "B" } @>)
        test <@ not (item'.Map.ContainsKey "B") @>

    let [<Fact>] ``Update map with remove element on existing`` () =
        let item = { mkItem() with Map = Map.ofList [("A", 1L) ; ("B", 2L)] }
        let key = table.PutItem item
        let item' = table.UpdateItem(key, <@ fun (r : R) -> { r with Map = r.Map |> Map.remove "C" } @>)
        test <@ 2 = item'.Map.Count @>

    let [<Fact>] ``Update map entry with Item access`` () =
        let item = { mkItem() with Map = Map.ofList [("A", 1L)] }
        let key = table.PutItem item
        let item' = table.UpdateItem(key, <@ fun (r : R) -> SET r.Map["A"] 2L @>)
        test <@ 2L = item'.Map["A"] @>

    let [<Fact>] ``Parametric map Item access`` () =
        let item = { mkItem() with Map = Map.ofList [("A", 1L)] }
        let key = table.PutItem item
        let uop = table.Template.PrecomputeUpdateExpr <@ fun i v (r : R) -> SET r.Map[i] v @>
        let item' = table.UpdateItem(key, uop "A" 2L)
        test <@ 2L = item'.Map["A"] @>

    let [<Fact>] ``Parametric map ContainsKey`` () =
        let item = { mkItem() with Map = Map.ofList [("A", 1L)] }
        let _key = table.PutItem item
        let cond = table.Template.PrecomputeConditionalExpr <@ fun i r -> r.Map |> Map.containsKey i @>
        let _item' = table.PutItem(item, cond "A")
        ()

    let [<Fact>] ``Combined update with successful precondition`` () =
        let item = mkItem()
        let key = table.PutItem item
        let item' = table.UpdateItem(key, <@ fun (r : R) -> { r with Value = r.Value + 1L } @>,
                                               precondition = <@ fun r -> r.Value = item.Value @>)

        test <@ item.Value + 1L = item'.Value @>

    let [<Fact>] ``Combined update with failed precondition`` () =
        let item = mkItem()
        let key = table.PutItem item
        fun () -> table.UpdateItem(key, <@ fun (r : R) -> { r with Value = r.Value + 1L } @>,
                                               precondition = <@ fun r -> r.Value = item.Value + 1L @>)

        |> shouldFailwith<_, ConditionalCheckFailedException>

        let item' = table.GetItem key
        test <@ item.Value = item'.Value @>

    let [<Fact>] ``SET an attribute`` () =
        let item = mkItem()
        let key = table.PutItem item
        let item' = table.UpdateItem(key, <@ fun r -> SET r.NestedList[0].NV item.HashKey &&&
                                                                 SET r.NestedList[1] { NV = item.HashKey ; NE = Enum.C } @>)

        test <@ item.HashKey = item'.NestedList[0].NV @>
        test <@ item.HashKey = item'.NestedList[1].NV @>

    let [<Fact>] ``SET a union attribute`` () =
        let item = { mkItem() with Unions = [UB(guid())] }
        let key = table.PutItem item
        let u = UA(rand())
        let item' = table.UpdateItem(key, <@ fun r -> SET r.Unions[0] u @>)

        test <@ 1 = item'.Unions.Length @>
        test <@ u = item'.Unions[0] @>

    let [<Fact>] ``REMOVE an attribute`` () =
        let item = { mkItem() with NestedList = [{NV = "foo" ; NE = Enum.A}] }
        let key = table.PutItem item
        let item' = table.UpdateItem(key, <@ fun r -> REMOVE r.NestedList[0] @>)

        test <@ 0 = item'.NestedList.Length @>

    let [<Fact>] ``ADD to set`` () =
        let item = mkItem()
        let key = table.PutItem item
        let item' = table.UpdateItem(key, <@ fun r -> ADD r.IntSet [42L] @>)

        test <@ item'.IntSet.Contains 42L @>

    let [<Fact>]``DELETE from set`` () =
        let item = { mkItem() with IntSet = set [1L ; 42L] }
        let key = table.PutItem item
        let item' = table.UpdateItem(key, <@ fun r -> DELETE r.IntSet [42L] @>)

        test <@ not (item'.IntSet.Contains 42L) @>
        test <@ 1 = item'.IntSet.Count @>

    let [<Fact>] ``Detect overlapping paths`` () =
        let item = mkItem()
        let key = table.PutItem item
        fun () -> table.UpdateItem(key, <@ fun r -> SET r.NestedList[0].NV "foo" &&&
                                                               REMOVE r.NestedList @>)

        |> shouldFailwith<_, ArgumentException>

        let item' = table.GetItem key
        test <@ item.Value = item'.Value @>

    let [<Fact>] ``Simple Parametric Updater 1`` () =
        let item = mkItem()
        let key = table.PutItem item
        let cond = table.Template.PrecomputeUpdateExpr <@ fun v1 v2 r -> { r with Value = v1 ; String = v2 } @>
        let v1 = rand()
        let v2 = guid()
        let result = table.UpdateItem(key, cond v1 v2)
        test <@ v1 = result.Value @>
        test <@ v2 = result.String @>

    let [<Fact>] ``Simple Parametric Updater 2`` () =
        let item = mkItem()
        let key = table.PutItem item
        let cond = table.Template.PrecomputeUpdateExpr <@ fun v1 v2 r -> SET r.Value v1 &&& ADD r.IntSet v2 @>
        let v1 = rand()
        let v2 = [ for _ in 1 .. 10 -> rand()]
        let result = table.UpdateItem(key, cond v1 v2)
        test <@ v1 = result.Value @>
        for v in v2 do test <@ result.IntSet.Contains v @>

    let [<Fact>] ``Parametric Updater with optional argument`` () =
        let item = { mkItem() with Optional = Some (guid()) }
        let key = table.PutItem item
        let cond = table.Template.PrecomputeUpdateExpr <@ fun opt (r : R) -> { r with Optional = opt } @>
        let result = table.UpdateItem(key, cond None)
        test <@ None = result.Optional @>

    let [<Fact>] ``Parametric Updater with heterogeneous argument consumption`` () =
        let item = mkItem()
        let key = table.PutItem item
        let values = [ for _ in 1 .. 10 -> rand()]
        let cond = table.Template.PrecomputeUpdateExpr <@ fun vs r -> SET r.List vs &&& ADD r.IntSet vs @>
        let result = table.UpdateItem(key, cond values)
        test <@ values = result.List @>
        for v in values do test <@ result.IntSet.Contains v @>

    let [<Fact>] ``Parametric Updater with invalid param usage`` () =
        let template = table.Template
        fun () -> template.PrecomputeUpdateExpr <@ fun v (r : R) -> { r with Value = List.head v } @>
        |> shouldFailwith<_, ArgumentException>

        fun () -> template.PrecomputeUpdateExpr <@ fun v (r : R) -> ADD r.IntSet (1L :: v) @>
        |> shouldFailwith<_, ArgumentException>

    let [<Fact>] ``Parametric Updater with map add element with constant key`` () =
        let item = { mkItem() with Map = Map.ofList [("A", 1L) ; ("B", 2L)] }
        let key = table.PutItem item
        let cond = table.Template.PrecomputeUpdateExpr <@ fun v (r : UpdateExprRecord) -> { r with Map = r.Map |> Map.add "C" v } @>
        let result = table.UpdateItem(key, cond 3L)
        test <@ 3 = result.Map.Count @>

    let [<Fact>] ``Parametric Updater with map add element with parametric key`` () =
        let item = { mkItem() with Map = Map.ofList [("A", 1L) ; ("B", 2L)] }
        let key = table.PutItem item
        let cond = table.Template.PrecomputeUpdateExpr <@ fun k v (r : UpdateExprRecord) -> { r with Map = r.Map |> Map.add k v } @>
        let result = table.UpdateItem(key, cond "C" 3L)
        test <@ 3 = result.Map.Count @>

    let [<Fact>] ``Parametric Updater with map remove element with parametric key`` () =
        let item = { mkItem() with Map = Map.ofList [("A", 1L) ; ("B", 2L)] }
        let key = table.PutItem item
        let cond = table.Template.PrecomputeUpdateExpr <@ fun k (r : UpdateExprRecord) -> { r with Map = r.Map |> Map.remove k } @>
        let result = table.UpdateItem(key, cond "A")
        test <@ 1 = result.Map.Count @>

    interface IClassFixture<TableFixture>
