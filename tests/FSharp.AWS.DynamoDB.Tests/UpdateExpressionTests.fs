namespace FSharp.AWS.DynamoDB.Tests

open System
open System.Threading

open Expecto

open FSharp.AWS.DynamoDB

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

type ``Update Expression Tests`` (fixture : TableFixture) =

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
            Map = seq { for i in 0L .. rand() % 5L -> "K" + guid(), rand() } |> Map.ofSeq
            IntSet = seq { for i in 0L .. rand() % 5L -> rand() } |> Set.ofSeq
            StringSet = seq { for i in 0L .. rand() % 5L -> guid() } |> Set.ofSeq
            ByteSet = seq { for i in 0L .. rand() % 5L -> bytes() } |> Set.ofSeq
            List = [for i in 0L .. rand() % 5L -> rand() ]
            Union = if rand() % 2L = 0L then UA (rand()) else UB(guid())
            Unions = [for i in 0L .. rand() % 5L -> if rand() % 2L = 0L then UA (rand()) else UB(guid()) ]
            Serialized = rand(), guid() ; Serialized2 = { NV = guid() ; NE = enum<Enum> (int (rand()) % 3) } ;
        }

    let table = TableContext.Create<UpdateExprRecord>(fixture.Client, fixture.TableName, createIfNotExists = true)

    member this.``Attempt to update HashKey`` () =
        let item = mkItem()
        let key = table.PutItem item
        fun () -> table.UpdateItem(key, <@ fun (r : R) -> { r with HashKey = guid() } @>)
        |> shouldFailwith<_, ArgumentException>

    member this.``Attempt to update RangeKey`` () =
        let item = mkItem()
        let key = table.PutItem item
        fun () -> table.UpdateItem(key, <@ fun (r : R) -> { r with RangeKey = guid() } @>)
        |> shouldFailwith<_, ArgumentException>

    member this.``Returning old value`` () =
        let item = mkItem()
        let key = table.PutItem item
        let item' = table.UpdateItem(key, <@ fun (r : R) -> { r with Value = r.Value + 1L } @>, returnLatest = false)
        Expect.equal item' item "item should be equal"

    member this.``Simple update DateTimeOffset`` () =
        let item = mkItem()
        let key = table.PutItem item
        let nv = DateTimeOffset.Now + TimeSpan.FromDays 366.
        let item' = table.UpdateItem(key, <@ fun (r : R) -> { r with DateTimeOffset = nv } @>)
        Expect.equal item'.DateTimeOffset nv ""

    member this.``Simple update TimeSpan`` () =
        let item = mkItem()
        let key = table.PutItem item
        let ts = TimeSpan.FromTicks(rand())
        let item' = table.UpdateItem(key, <@ fun (r : R) -> { r with TimeSpan = ts } @>)
        Expect.equal item'.TimeSpan ts ""

    member this.``Simple update Guid`` () =
        let item = mkItem()
        let key = table.PutItem item
        let g = Guid.NewGuid()
        let item' = table.UpdateItem(key, <@ fun (r : R) -> { r with Guid = g } @>)
        Expect.equal item'.Guid g ""

    member this.``Simple increment update`` () =
        let item = mkItem()
        let key = table.PutItem item
        let item' = table.UpdateItem(key, <@ fun (r : R) -> { r with Value = r.Value + 1L } @>)
        Expect.equal item'.Value (item.Value + 1L) ""

    member this.``Simple decrement update`` () =
        let item = mkItem()
        let key = table.PutItem item
        let item' = table.UpdateItem(key, <@ fun (r : R) -> { r with Value = r.Value - 10L } @>)
        Expect.equal item'.Value (item.Value - 10L) ""

    member this.``Simple update serialized value`` () =
        let item = mkItem()
        let key = table.PutItem item
        let value' = rand(), guid()
        let item' = table.UpdateItem(key, <@ fun (r : R) -> { r with Serialized = value' } @>)
        Expect.equal item'.Serialized value' ""

    member this.``Update using nested record values`` () =
        let item = mkItem()
        let key = table.PutItem item
        let item' = table.UpdateItem(key, <@ fun (r : R) -> { r with String = r.Nested.NV } @>)
        Expect.equal item'.String item.Nested.NV ""

    member this.``Update using nested union values`` () =
        let item = mkItem()
        let key = table.PutItem item
        let u = UB(guid())
        let item' = table.UpdateItem(key, <@ fun (r : R) -> { r with Union = u } @>)
        Expect.equal item'.Union u ""

    member this.``Update using nested list`` () =
        let item = mkItem()
        let key = table.PutItem item
        let item' = table.UpdateItem(key, <@ fun (r : R) -> { r with Nested = r.NestedList.[0] } @>)
        Expect.equal item'.Nested item.NestedList.[0] ""

    member this.``Update using tuple values`` () =
        let item = mkItem()
        let key = table.PutItem item
        let item' = table.UpdateItem(key, <@ fun (r : R) -> { r with Value = fst r.Tuple + 1L } @>)
        Expect.equal item'.Value (fst item.Tuple + 1L) ""

    member this.``Update optional field to None`` () =
        let item = { mkItem() with Optional = Some (guid()) }
        let key = table.PutItem item
        let item' = table.UpdateItem(key, <@ fun (r : R) -> { r with Optional = None } @>)
        Expect.equal item'.Optional None ""

    member this.``Update optional field to Some`` () =
        let item = { mkItem() with Optional = None }
        let key = table.PutItem item
        let item' = table.UpdateItem(key, <@ fun (r : R) -> { r with Optional = Some(guid()) } @>)
        Expect.equal item'.Optional.IsSome true ""

    member this.``Update list field to non-empty`` () =
        let item = { mkItem() with List = [1L] }
        let key = table.PutItem item
        let nv = [for i in 1 .. 10 -> rand() ]
        let item' = table.UpdateItem(key, <@ fun (r : R) -> { r with List = nv } @>)
        Expect.equal item'.List nv ""

    member this.``Update list field to empty`` () =
        let item = { mkItem() with List = [1L] }
        let key = table.PutItem item
        let item' = table.UpdateItem(key, <@ fun (r : R) -> { r with List = [] } @>)
        Expect.equal item'.List.Length 0 ""

    member this.``Update list with concatenation`` () =
        let item = { mkItem() with List = [1L] }
        let key = table.PutItem item
        let item' = table.UpdateItem(key, <@ fun (r : R) -> { r with List = r.List @ r.List } @>)
        Expect.equal item'.List (item.List @ item.List) ""

    member this.``Update list with consing`` () =
        let item = { mkItem() with List = [2L] }
        let key = table.PutItem item
        let item' = table.UpdateItem(key, <@ fun (r : R) -> { r with List = 1L :: r.List } @>)
        Expect.equal item'.List [1L ; 2L] ""

    member this.``Update using defaultArg combinator (Some)`` () =
        let item = { mkItem() with Optional = Some (guid()) }
        let key = table.PutItem item
        let item' = table.UpdateItem(key, <@ fun (r : R) -> { r with String = defaultArg r.Optional "<undefined>" } @>)
        Expect.equal item'.String item.Optional.Value ""

    member this.``Update using defaultArg combinator (None)`` () =
        let item = { mkItem() with Optional = None }
        let key = table.PutItem item
        let item' = table.UpdateItem(key, <@ fun (r : R) -> { r with String = defaultArg r.Optional "<undefined>" } @>)
        Expect.equal item'.String "<undefined>" ""

    member this.``Update int set with add element`` () =
        let item = { mkItem() with IntSet = set [1L;2L] }
        let key = table.PutItem item
        let item' = table.UpdateItem(key, <@ fun (r : R) -> { r with IntSet = r.IntSet |> Set.add 3L } @>)
        Expect.equal (item'.IntSet.Contains 3L) true ""

    member this.``Update int set with remove element`` () =
        let item = { mkItem() with IntSet = set [1L;2L] }
        let key = table.PutItem item
        let item' = table.UpdateItem(key, <@ fun (r : R) -> { r with IntSet = r.IntSet |> Set.remove 2L } @>)
        Expect.equal (item'.IntSet.Contains 2L) false ""

    member this.``Update int set with append set`` () =
        let item = { mkItem() with IntSet = Set.empty }
        let key = table.PutItem item
        let item' = table.UpdateItem(key, <@ fun (r : R) -> { r with IntSet = r.IntSet + set [3L] } @>)
        Expect.equal (item'.IntSet.Contains 3L) true ""

    member this.``Update int set with remove set`` () =
        let item = { mkItem() with IntSet = set [1L;2L] }
        let key = table.PutItem item
        let item' = table.UpdateItem(key, <@ fun (r : R) -> { r with IntSet = r.IntSet - set [1L;2L;3L] } @>)
        Expect.equal item'.IntSet.Count 0 ""

    member this.``Update string set with add element`` () =
        let item = { mkItem() with StringSet = set ["1";"2"] }
        let key = table.PutItem item
        let item' = table.UpdateItem(key, <@ fun (r : R) -> { r with StringSet = r.StringSet |> Set.add "3" } @>)
        Expect.equal (item'.StringSet.Contains "3") true ""

    member this.``Update string set with remove element`` () =
        let item = { mkItem() with StringSet = set ["1";"2"] }
        let key = table.PutItem item
        let item' = table.UpdateItem(key, <@ fun (r : R) -> { r with StringSet = r.StringSet |> Set.remove "2" } @>)
        Expect.equal (item'.StringSet.Contains "2") false ""

    member this.``Update string set with append set`` () =
        let item = { mkItem() with StringSet = Set.empty }
        let key = table.PutItem item
        let item' = table.UpdateItem(key, <@ fun (r : R) -> { r with StringSet = r.StringSet + set ["3"] } @>)
        Expect.equal (item'.StringSet.Contains "3") true ""

    member this.``Update byte set with append set`` () =
        let item = { mkItem() with ByteSet = Set.empty }
        let key = table.PutItem item
        let item' = table.UpdateItem(key, <@ fun (r : R) -> { r with ByteSet = r.ByteSet + set [[|42uy|]] } @>)
        Expect.equal (item'.ByteSet.Contains [|42uy|]) true ""

    member this.``Update string set with remove set`` () =
        let item = { mkItem() with StringSet = set ["1";"2"] }
        let key = table.PutItem item
        let item' = table.UpdateItem(key, <@ fun (r : R) -> { r with StringSet = r.StringSet - set ["1";"2";"3"] } @>)
        Expect.equal item'.StringSet.Count 0 ""

    member this.``Update map with add element`` () =
        let item = { mkItem() with Map = Map.ofList [("A", 1L) ; ("B", 2L)] }
        let key = table.PutItem item
        let item' = table.UpdateItem(key, <@ fun (r : R) -> { r with Map = r.Map |> Map.add "C" 3L } @>)
        Expect.equal (item'.Map.TryFind "C") (Some 3L) ""

    member this.``Update map with remove element`` () =
        let item = { mkItem() with Map = Map.ofList [("A", 1L) ; ("B", 2L)] }
        let key = table.PutItem item
        let item' = table.UpdateItem(key, <@ fun (r : R) -> { r with Map = r.Map |> Map.remove "B" } @>)
        Expect.equal (item'.Map.ContainsKey "B") false ""

    member this.``Update map with remove element on existing`` () =
        let item = { mkItem() with Map = Map.ofList [("A", 1L) ; ("B", 2L)] }
        let key = table.PutItem item
        let item' = table.UpdateItem(key, <@ fun (r : R) -> { r with Map = r.Map |> Map.remove "C" } @>)
        Expect.equal item'.Map.Count 2 ""

    member this.``Update map entry with Item access`` () =
        let item = { mkItem() with Map = Map.ofList [("A", 1L)] }
        let key = table.PutItem item
        let item' = table.UpdateItem(key, <@ fun (r : R) -> SET r.Map.["A"] 2L @>)
        Expect.equal item'.Map.["A"] 2L ""

    member this.``Parametric map Item access`` () =
        let item = { mkItem() with Map = Map.ofList [("A", 1L)] }
        let key = table.PutItem item
        let uop = table.Template.PrecomputeUpdateExpr <@ fun i v (r : R) -> SET r.Map.[i] v @>
        let item' = table.UpdateItem(key, uop "A" 2L)
        Expect.equal item'.Map.["A"] 2L ""

    member this.``Parametric map ContainsKey`` () =
        let item = { mkItem() with Map = Map.ofList [("A", 1L)] }
        let key = table.PutItem item
        let cond = table.Template.PrecomputeConditionalExpr <@ fun i r -> r.Map |> Map.containsKey i @>
        let item' = table.PutItem(item, cond "A")
        ()

    member this.``Combined update with succesful precondition`` () =
        let item = mkItem()
        let key = table.PutItem item
        let item' = table.UpdateItem(key, <@ fun (r : R) -> { r with Value = r.Value + 1L } @>,
                                               precondition = <@ fun r -> r.Value = item.Value @>)

        Expect.equal item'.Value (item.Value + 1L) ""

    member this.``Combined update with failed precondition`` () =
        let item = mkItem()
        let key = table.PutItem item
        fun () -> table.UpdateItem(key, <@ fun (r : R) -> { r with Value = r.Value + 1L } @>,
                                               precondition = <@ fun r -> r.Value = item.Value + 1L @>)

        |> shouldFailwith<_, ConditionalCheckFailedException>

        let item' = table.GetItem key
        Expect.equal item'.Value item.Value ""


    member this.``SET an attribute`` () =
        let item = mkItem()
        let key = table.PutItem item
        let item' = table.UpdateItem(key, <@ fun r -> SET r.NestedList.[0].NV item.HashKey &&&
                                                                 SET r.NestedList.[1] { NV = item.HashKey ; NE = Enum.C } @>)

        Expect.equal item'.NestedList.[0].NV item.HashKey "NV sould be equal to HashKey"
        Expect.equal item'.NestedList.[1].NV item.HashKey "NV sould be equal to HashKey"

    member this.``SET a union attribute`` () =
        let item = { mkItem() with Unions = [UB(guid())] }
        let key = table.PutItem item
        let u = UA(rand())
        let item' = table.UpdateItem(key, <@ fun r -> SET r.Unions.[0] u @>)

        Expect.equal item'.Unions.Length 1 "Unions length should be equal to 1"
        Expect.equal item'.Unions.[0] u "Union should be equal"

    member this.``REMOVE an attribute`` () =
        let item = { mkItem() with NestedList = [{NV = "foo" ; NE = Enum.A}] }
        let key = table.PutItem item
        let item' = table.UpdateItem(key, <@ fun r -> REMOVE r.NestedList.[0] @>)

        Expect.equal item'.NestedList.Length 0 ""

    member this.``ADD to set`` () =
        let item = mkItem()
        let key = table.PutItem item
        let item' = table.UpdateItem(key, <@ fun r -> ADD r.IntSet [42L] @>)

        Expect.equal (item'.IntSet.Contains 42L) true ""

    member this.``DELETE from set`` () =
        let item = { mkItem() with IntSet = set [1L ; 42L] }
        let key = table.PutItem item
        let item' = table.UpdateItem(key, <@ fun r -> DELETE r.IntSet [42L] @>)

        Expect.equal (item'.IntSet.Contains 42L) false "IntSet should not contains 42"
        Expect.equal item'.IntSet.Count 1 "IntSet count should be 1"

    member this.``Detect overlapping paths`` () =
        let item = mkItem()
        let key = table.PutItem item
        fun () -> table.UpdateItem(key, <@ fun r -> SET r.NestedList.[0].NV "foo" &&&
                                                               REMOVE r.NestedList @>)

        |> shouldFailwith<_, ArgumentException>

        let item' = table.GetItem key
        Expect.equal item'.Value item.Value ""

    member this.``Simple Parametric Updater 1`` () =
        let item = mkItem()
        let key = table.PutItem item
        let cond = table.Template.PrecomputeUpdateExpr <@ fun v1 v2 r -> { r with Value = v1 ; String = v2 } @>
        let v1 = rand()
        let v2 = guid()
        let result = table.UpdateItem(key, cond v1 v2)
        Expect.equal result.Value v1 "Value should be equal to v1"
        Expect.equal result.String v2 "String should be equal to v2"

    member this.``Simple Parametric Updater 2`` () =
        let item = mkItem()
        let key = table.PutItem item
        let cond = table.Template.PrecomputeUpdateExpr <@ fun v1 v2 r -> SET r.Value v1 &&& ADD r.IntSet v2 @>
        let v1 = rand()
        let v2 = [ for i in 1 .. 10 -> rand()]
        let result = table.UpdateItem(key, cond v1 v2)
        Expect.equal result.Value v1 "Value should be equal to v1"
        for v in v2 do Expect.equal (result.IntSet.Contains v) true "IntSet sould contains v"

    member this.``Parametric Updater with optional argument`` () =
        let item = { mkItem() with Optional = Some (guid()) }
        let key = table.PutItem item
        let cond = table.Template.PrecomputeUpdateExpr <@ fun opt (r : R) -> { r with Optional = opt } @>
        let result = table.UpdateItem(key, cond None)
        Expect.equal result.Optional None ""

    member this.``Parametric Updater with heterogeneous argument consumption`` () =
        let item = mkItem()
        let key = table.PutItem item
        let values = [ for i in 1 .. 10 -> rand()]
        let cond = table.Template.PrecomputeUpdateExpr <@ fun vs r -> SET r.List vs &&& ADD r.IntSet vs @>
        let result = table.UpdateItem(key, cond values)
        Expect.equal result.List values "List should be equal to values"
        for v in values do Expect.equal (result.IntSet.Contains v) true "IntSet sould contains v"

    member this.``Parametric Updater with invalid param usage`` () =
        let template = table.Template
        fun () -> template.PrecomputeUpdateExpr <@ fun v (r : R) -> { r with Value = List.head v } @>
        |> shouldFailwith<_, ArgumentException>

        fun () -> template.PrecomputeUpdateExpr <@ fun v (r : R) -> ADD r.IntSet (1L :: v) @>
        |> shouldFailwith<_, ArgumentException>

    member this.``Parametric Updater with map add element with constant key`` () =
        let item = { mkItem() with Map = Map.ofList [("A", 1L) ; ("B", 2L)] }
        let key = table.PutItem item
        let cond = table.Template.PrecomputeUpdateExpr <@ fun v (r : UpdateExprRecord) -> { r with Map = r.Map |> Map.add "C" v } @>
        let result = table.UpdateItem(key, cond 3L)
        Expect.equal result.Map.Count 3 ""

    member this.``Parametric Updater with map add element with parametric key`` () =
        let item = { mkItem() with Map = Map.ofList [("A", 1L) ; ("B", 2L)] }
        let key = table.PutItem item
        let cond = table.Template.PrecomputeUpdateExpr <@ fun k v (r : UpdateExprRecord) -> { r with Map = r.Map |> Map.add k v } @>
        let result = table.UpdateItem(key, cond "C" 3L)
        Expect.equal result.Map.Count 3 ""

    member this.``Parametric Updater with map remove element with parametric key`` () =
        let item = { mkItem() with Map = Map.ofList [("A", 1L) ; ("B", 2L)] }
        let key = table.PutItem item
        let cond = table.Template.PrecomputeUpdateExpr <@ fun k (r : UpdateExprRecord) -> { r with Map = r.Map |> Map.remove k } @>
        let result = table.UpdateItem(key, cond "A")
        Expect.equal result.Map.Count 1 ""
