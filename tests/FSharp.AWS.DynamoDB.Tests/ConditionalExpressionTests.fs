namespace FSharp.DynamoDB.Tests

open System
open System.Threading

open Xunit
open FsUnit.Xunit

open FSharp.DynamoDB

[<AutoOpen>]
module CondExprTypes =

    [<Flags>]
    type Enum = A = 1 | B = 2 | C = 4

    type Nested = { NV : string ; NE : Enum }

    type Union = UA of int64 | UB of string

    type CondExprRecord =
        {
            [<HashKey>]
            HashKey : string
            [<RangeKey>]
            RangeKey : int64

            Value : int64

            Tuple : int64 * int64

            Nested : Nested

            Union : Union

            NestedList : Nested list

            TimeSpan : TimeSpan

            DateTimeOffset : DateTimeOffset

            Guid : Guid

            Bool : bool

            Bytes : byte[]

            Ref : string ref

            Optional : string option

            List : int64 list

            Map : Map<string, int64>

            Set : Set<int64>

            [<BinaryFormatter>]
            Serialized : int64 * string
        }

type ``Conditional Expression Tests`` () =

    let client = getDynamoDBAccount()
    let tableName = getRandomTableName()

    let rand = let r = Random() in fun () -> int64 <| r.Next()
    let mkItem() = 
        { 
            HashKey = guid() ; RangeKey = rand() ; 
            Value = rand() ; Tuple = rand(), rand() ;
            TimeSpan = TimeSpan.FromTicks(rand()) ; DateTimeOffset = DateTimeOffset.Now ; Guid = Guid.NewGuid()
            Bool = false ; Optional = Some (guid()) ; Ref = ref (guid()) ; Bytes = Guid.NewGuid().ToByteArray()
            Nested = { NV = guid() ; NE = enum<Enum> (int (rand()) % 3) } ;
            NestedList = [{ NV = guid() ; NE = enum<Enum> (int (rand()) % 3) } ]
            Map = seq { for i in 0L .. rand() % 5L -> "K" + guid(), rand() } |> Map.ofSeq 
            Set = seq { for i in 0L .. rand() % 5L -> rand() } |> Set.ofSeq
            List = [for i in 0L .. rand() % 5L -> rand() ]
            Union = if rand() % 2L = 0L then UA (rand()) else UB(guid())
            Serialized = rand(), guid()
        }

    let table = TableContext.Create<CondExprRecord>(client, tableName, createIfNotExists = true)

    [<Fact>]
    let ``Item exists precondition`` () =
        let item = mkItem()
        fun () -> table.PutItem(item, precondition = itemExists)
        |> shouldFailwith<_, ConditionalCheckFailedException>

        let key = table.PutItem item
        table.PutItem(item, precondition = itemExists)

    [<Fact>]
    let ``Item not exists precondition`` () =
        let item = mkItem()
        let key = table.PutItem(item, precondition = itemDoesNotExist)
        fun () -> table.PutItem(item, precondition = itemDoesNotExist)
        |> shouldFailwith<_, ConditionalCheckFailedException>

    [<Fact>]
    let ``String precondition`` () =
        let item = mkItem()
        let key = table.PutItem item
        fun () -> table.PutItem(item, <@ fun r -> r.HashKey = guid() @>)
        |> shouldFailwith<_, ConditionalCheckFailedException>

        let hkey = item.HashKey
        table.PutItem(item, <@ fun r -> r.HashKey = hkey @>) |> ignore

    [<Fact>]
    let ``Number precondition`` () =
        let item = mkItem()
        let key = table.PutItem item
        fun () -> table.PutItem(item, <@ fun r -> r.Value = rand() @>)
        |> shouldFailwith<_, ConditionalCheckFailedException>

        let value = item.Value
        table.PutItem(item, <@ fun r -> r.Value = value @>) |> ignore

    [<Fact>]
    let ``Bool precondition`` () =
        let item = mkItem()
        let key = table.PutItem item
        let value = item.Bool
        fun () -> table.PutItem(item, <@ fun r -> r.Bool = not value @>)
        |> shouldFailwith<_, ConditionalCheckFailedException>

        table.PutItem(item, <@ fun r -> r.Bool = value @>) |> ignore

    [<Fact>]
    let ``Bytes precondition`` () =
        let item = mkItem()
        let key = table.PutItem item
        let value = item.Bool
        fun () -> table.PutItem(item, <@ fun r -> r.Bytes = Guid.NewGuid().ToByteArray() @>)
        |> shouldFailwith<_, ConditionalCheckFailedException>

        let value = item.Bytes
        table.PutItem(item, <@ fun r -> r.Bytes = value @>) |> ignore

    [<Fact>]
    let ``DateTimeOffset precondition`` () =
        let item = mkItem()
        let key = table.PutItem item
        fun () -> table.PutItem(item, <@ fun r -> r.DateTimeOffset > DateTimeOffset.Now + TimeSpan.FromDays(3.) @>)
        |> shouldFailwith<_, ConditionalCheckFailedException>

        let value = item.DateTimeOffset
        table.PutItem(item, <@ fun r -> r.DateTimeOffset <= DateTimeOffset.Now + TimeSpan.FromDays(3.) @>) |> ignore

    [<Fact>]
    let ``TimeSpan precondition`` () =
        let item = mkItem()
        let key = table.PutItem item
        let UB = item.TimeSpan + item.TimeSpan
        fun () -> table.PutItem(item, <@ fun r -> r.TimeSpan >= UB @>)
        |> shouldFailwith<_, ConditionalCheckFailedException>

        let value = item.DateTimeOffset
        table.PutItem(item, <@ fun r -> r.TimeSpan < UB @>) |> ignore

    [<Fact>]
    let ``Guid precondition`` () =
        let item = mkItem()
        let key = table.PutItem item
        fun () -> table.PutItem(item, <@ fun r -> r.Guid = Guid.NewGuid() @>)
        |> shouldFailwith<_, ConditionalCheckFailedException>

        let value = item.Guid
        table.PutItem(item, <@ fun r -> r.Guid = value @>) |> ignore

    [<Fact>]
    let ``Optional precondition`` () =
        let item = mkItem()
        let key = table.PutItem item
        fun () -> table.PutItem(item, <@ fun r -> r.Optional = None @>)
        |> shouldFailwith<_, ConditionalCheckFailedException>

        let value = item.Optional
        table.PutItem({ item with Optional = None }, <@ fun r -> r.Optional = value @>) |> ignore

        fun () -> table.PutItem(item, <@ fun r -> r.Optional = (guid() |> Some) @>)
        |> shouldFailwith<_, ConditionalCheckFailedException>

    [<Fact>]
    let ``Optional-Value precondition`` () =
        let item = { mkItem() with Optional = Some "foo" }
        let key = table.PutItem item
        fun () -> table.PutItem(item, <@ fun r -> r.Optional.Value = "bar" @>)
        |> shouldFailwith<_, ConditionalCheckFailedException>

        let value = item.Optional
        let _ = table.PutItem({ item with Optional = None }, <@ fun r -> r.Optional.Value = "foo" @>)

        fun () -> table.PutItem(item, <@ fun r -> r.Optional.Value = "foo" @>)
        |> shouldFailwith<_, ConditionalCheckFailedException>

    [<Fact>]
    let ``Ref precondition`` () =
        let item = mkItem()
        let key = table.PutItem item
        fun () -> table.PutItem(item, <@ fun r -> r.Ref = (guid() |> ref) @>)
        |> shouldFailwith<_, ConditionalCheckFailedException>

        let value = item.Ref.Value
        table.PutItem(item, <@ fun r -> r.Ref = ref value @>) |> ignore

    [<Fact>]
    let ``Tuple precondition`` () =
        let item = mkItem()
        let key = table.PutItem item
        fun () -> table.PutItem(item, <@ fun r -> fst r.Tuple = rand() @>)
        |> shouldFailwith<_, ConditionalCheckFailedException>

        let value = fst item.Tuple
        table.PutItem(item, <@ fun r -> fst r.Tuple = value @>) |> ignore

    [<Fact>]
    let ``Record precondition`` () =
        let item = mkItem()
        let key = table.PutItem item
        fun () -> table.PutItem(item, <@ fun r -> r.Nested = { NV = guid() ; NE = Enum.C } @>)
        |> shouldFailwith<_, ConditionalCheckFailedException>

        let value = item.Nested.NV
        let enum = item.Nested.NE
        table.PutItem(item, <@ fun r -> r.Nested = { NV = value ; NE = enum } @>) |> ignore

    [<Fact>]
    let ``Nested attribute precondition`` () =
        let item = mkItem()
        let key = table.PutItem item
        fun () -> table.PutItem(item, <@ fun r -> r.Nested.NV = guid() @>)
        |> shouldFailwith<_, ConditionalCheckFailedException>

        let value = item.Nested.NE
        table.PutItem(item, <@ fun r -> r.Nested.NE = value @>) |> ignore

    [<Fact>]
    let ``Nested union precondition`` () =
        let item = mkItem()
        let key = table.PutItem item
        fun () -> table.PutItem(item, <@ fun r -> r.Union = UA (rand()) @>)
        |> shouldFailwith<_, ConditionalCheckFailedException>

        table.PutItem(item, <@ fun r -> r.Union = item.Union @>) |> ignore

    [<Fact>]
    let ``String-Contains precondition`` () =
        let item = { mkItem() with Ref = ref "12-42-12" }
        let key = table.PutItem item
        let elem = item.HashKey
        fun () -> table.PutItem(item, <@ fun r -> r.Ref.Value.Contains "41" @>)
        |> shouldFailwith<_, ConditionalCheckFailedException>

        table.PutItem(item, <@ fun r -> r.Ref.Value.Contains "42" @>) |> ignore

    [<Fact>]
    let ``String-StartsWith precondition`` () =
        let item = { mkItem() with Ref = ref "12-42-12" }
        let key = table.PutItem item
        let elem = item.HashKey
        fun () -> table.PutItem(item, <@ fun r -> r.Ref.Value.StartsWith "41" @>)
        |> shouldFailwith<_, ConditionalCheckFailedException>

        table.PutItem(item, <@ fun r -> r.Ref.Value.StartsWith "12" @>) |> ignore

    [<Fact>]
    let ``String-length precondition`` () =
        let item = mkItem()
        let key = table.PutItem item
        let elem = item.HashKey
        fun () -> table.PutItem(item, <@ fun r -> r.HashKey.Length <> elem.Length  @>)
        |> shouldFailwith<_, ConditionalCheckFailedException>

        table.PutItem(item, <@ fun r -> r.HashKey.Length >= elem.Length @>) |> ignore


    [<Fact>]
    let ``Array-length precondition`` () =
        let item = mkItem()
        let key = table.PutItem item
        let bytes = item.Bytes
        fun () -> table.PutItem(item, <@ fun r -> r.Bytes.Length <> bytes.Length @>)
        |> shouldFailwith<_, ConditionalCheckFailedException>

        table.PutItem(item, <@ fun r -> r.Bytes.Length >= bytes.Length @>) |> ignore
        table.PutItem(item, <@ fun r -> r.Bytes |> Array.length >= bytes.Length @>) |> ignore

    [<Fact>]
    let ``Array index precondition`` () =
        let item = mkItem()
        let key = table.PutItem item
        let nested = item.NestedList.[0]
        fun () -> table.PutItem(item, <@ fun r -> r.NestedList.[0].NV = guid()  @>)
        |> shouldFailwith<_, ConditionalCheckFailedException>

        table.PutItem(item, <@ fun r -> r.NestedList.[0] = nested @>) |> ignore

    [<Fact>]
    let ``List-length precondition`` () =
        let item = mkItem()
        let key = table.PutItem item
        let list = item.List
        fun () -> table.PutItem(item, <@ fun r -> r.List.Length <> list.Length  @>)
        |> shouldFailwith<_, ConditionalCheckFailedException>

        table.PutItem(item, <@ fun r -> r.List.Length >= list.Length @>) |> ignore
        table.PutItem(item, <@ fun r -> List.length r.List >= list.Length @>) |> ignore

    [<Fact>]
    let ``List-isEmpty precondition`` () =
        let item = { mkItem() with List = [] }
        let key = table.PutItem item
        table.PutItem({item with List = [42L]}, <@ fun r -> List.isEmpty r.List @>) |> ignore
        
        fun () -> table.PutItem(item, <@ fun r -> List.isEmpty r.List  @>)
        |> shouldFailwith<_, ConditionalCheckFailedException>


    [<Fact>]
    let ``Set-count precondition`` () =
        let item = mkItem()
        let key = table.PutItem item
        let set = item.Set
        fun () -> table.PutItem(item, <@ fun r -> r.Set.Count <> set.Count  @>)
        |> shouldFailwith<_, ConditionalCheckFailedException>

        table.PutItem(item, <@ fun r -> r.Set.Count <= set.Count @>) |> ignore
        table.PutItem(item, <@ fun r -> r.Set |> Set.count >= Set.count set @>) |> ignore

    [<Fact>]
    let ``Set-contains precondition`` () =
        let item = mkItem()
        let key = table.PutItem item
        let elem = item.Set |> Seq.max
        fun () -> table.PutItem(item, <@ fun r -> r.Set.Contains (elem + 1L)  @>)
        |> shouldFailwith<_, ConditionalCheckFailedException>

        table.PutItem(item, <@ fun r -> r.Set.Contains elem @>) |> ignore
        table.PutItem(item, <@ fun r -> r.Set |> Set.contains elem @>) |> ignore

    [<Fact>]
    let ``Map-count precondition`` () =
        let item = mkItem()
        let key = table.PutItem item
        let map = item.Map
        fun () -> table.PutItem(item, <@ fun r -> r.Map.Count <> map.Count @>)
        |> shouldFailwith<_, ConditionalCheckFailedException>

        table.PutItem(item, <@ fun r -> r.Map.Count >= map.Count @>) |> ignore

    [<Fact>]
    let ``Map-contains precondition`` () =
        let item = mkItem()
        let key = table.PutItem item
        let elem = item.Map |> Map.toSeq |> Seq.head |> fst
        fun () -> table.PutItem(item, <@ fun r -> r.Map.ContainsKey (elem + "foo")  @>)
        |> shouldFailwith<_, ConditionalCheckFailedException>

        table.PutItem(item, <@ fun r -> r.Map.ContainsKey elem @>) |> ignore
        table.PutItem(item, <@ fun r -> r.Map |> Map.containsKey elem @>) |> ignore

    [<Fact>]
    let ``Map Item precondition`` () =
        let item = { mkItem() with Map = Map.ofList [("A", 42L)] }
        let key = table.PutItem item
        fun () -> table.PutItem(item, <@ fun r -> r.Map.["A"] = 41L @>)
        |> shouldFailwith<_, ConditionalCheckFailedException>

        table.PutItem(item, <@ fun r -> r.Map.["A"] = 42L @>) |> ignore

    [<Fact>]
    let ``Map Item parametric precondition`` () =
        let item = { mkItem() with Map = Map.ofList [("A", 42L)] }
        let key = table.PutItem item
        let cond = table.Template.PrecomputeConditionalExpr <@ fun k v r -> r.Map.[k] = v @>
        fun () -> table.PutItem(item, cond "A" 41L)
        |> shouldFailwith<_, ConditionalCheckFailedException>

        table.PutItem(item, cond "A" 42L) |> ignore


    [<Fact>]
    let ``Fail on identical comparands`` () =
        fun () -> table.Template.PrecomputeConditionalExpr <@ fun r -> r.Guid < r.Guid @>
        |> shouldFailwith<_, ArgumentException>

        fun () -> table.Template.PrecomputeConditionalExpr <@ fun r -> r.Bytes.Length = r.Bytes.Length @>
        |> shouldFailwith<_, ArgumentException>


    [<Fact>]
    let ``Serializable precondition`` () =
        let item = mkItem()
        let key = table.PutItem item
        fun () -> table.PutItem(item, <@ fun r -> r.Serialized = (0L,"")  @>)
        |> shouldFailwith<_, ArgumentException>

    [<Fact>]
    let ``EXISTS precondition`` () =
        let item = { mkItem() with List = [1L] }
        let key = table.PutItem item
        let _ = table.PutItem(item, precondition = <@ fun r -> EXISTS r.List.[0] @>)
        fun () -> table.PutItem(item, precondition = <@ fun r -> EXISTS r.List.[1] @>)
        |> shouldFailwith<_, ConditionalCheckFailedException>

    [<Fact>]
    let ``NOT_EXISTS precondition`` () =
        let item = { mkItem() with List = [1L] }
        let key = table.PutItem item
        let _ = table.PutItem(item, precondition = <@ fun r -> NOT_EXISTS r.List.[1] @>)
        fun () -> table.PutItem(item, precondition = <@ fun r -> NOT_EXISTS r.List.[0] @>)
        |> shouldFailwith<_, ConditionalCheckFailedException>

    [<Fact>]
    let ``Boolean precondition`` () =
        let item = mkItem()
        let key = table.PutItem item
        table.PutItem(item, <@ fun r -> false || r.HashKey = item.HashKey && not(not(r.RangeKey = item.RangeKey || r.Bool = item.Bool)) @>) |> ignore
        table.PutItem(item, <@ fun r -> r.HashKey = item.HashKey || (true && r.RangeKey = item.RangeKey) @>) |> ignore

    [<Fact>]
    let ``Simple Query Expression`` () =
        let hKey = guid()

        seq { for i in 1 .. 200 -> { mkItem() with HashKey = hKey ; RangeKey = int64 i }}
        |> Seq.splitInto 25
        |> Seq.map table.BatchPutItemsAsync
        |> Async.Parallel
        |> Async.Ignore
        |> Async.RunSynchronously
       

        let results = table.Query(<@ fun r -> r.HashKey = hKey && BETWEEN r.RangeKey 50L 149L @>)
        results.Length |> should equal 100

    [<Fact>]
    let ``Simple Query/Filter Expression`` () =
        let hKey = guid()

        seq { for i in 1 .. 200 -> { mkItem() with HashKey = hKey ; RangeKey = int64 i ; Bool = i % 2 = 0}}
        |> Seq.splitInto 25
        |> Seq.map table.BatchPutItemsAsync
        |> Async.Parallel
        |> Async.Ignore
        |> Async.RunSynchronously

        let results = table.Query(<@ fun r -> r.HashKey = hKey && BETWEEN r.RangeKey 50L 149L @>,
                                        filterCondition = <@ fun r -> r.Bool = true @>)

        results.Length |> should equal 50

    [<Fact>]
    let ``Detect incompatible key conditions`` () =
        let test outcome q = table.Template.PrecomputeConditionalExpr(q).IsKeyConditionCompatible |> should equal outcome

        test true <@ fun r -> r.HashKey = "2" @>
        test true <@ fun r -> r.HashKey = "2" && r.RangeKey < 2L @>
        test true <@ fun r -> r.HashKey = "2" && BETWEEN r.RangeKey 1L 2L @>
        test false <@ fun r -> r.HashKey < "2" @>
        test false <@ fun r -> r.HashKey >= "2" @>
        test false <@ fun r -> BETWEEN r.HashKey "2" "3" @>
        test false <@ fun r -> r.HashKey = "2" && r.HashKey = "4" @>
        test false <@ fun r -> r.RangeKey = 2L @>
        test false <@ fun r -> r.HashKey = "2" && r.RangeKey = 2L && r.RangeKey < 10L @>
        test false <@ fun r -> r.HashKey = "2" || r.RangeKey = 2L @>
        test false <@ fun r -> r.HashKey = "2" && not (r.RangeKey = 2L) @>
        test false <@ fun r -> r.HashKey = "2" && r.Bool = true @>
        test false <@ fun r -> r.HashKey = "2" && BETWEEN 1L r.RangeKey 2L @>

    [<Fact>]
    let ``Detect incompatible comparisons`` () =
        let test outcome q = 
            let f () = table.Template.PrecomputeConditionalExpr(q)
            if outcome then f () |> ignore
            else shouldFailwith<_, ArgumentException> f |> ignore

        test true <@ fun r -> r.Guid > Guid.Empty @>
        test true <@ fun r -> r.Bool > false @>
        test true <@ fun r -> r.Optional >= Some "1" @>
        test false <@ fun r -> r.Map > Map.empty @>
        test false <@ fun r -> r.Set > Set.empty @>
        test false <@ fun r -> r.Ref > ref "12" @>
        test false <@ fun r -> r.Serialized <= (1L, "32") @>
        test false <@ fun r -> r.Tuple <= (1L, 2L) @>
        test false <@ fun r -> r.Nested <= r.Nested @>

    [<Fact>]
    let ``Simple Scan Expression`` () =
        let hKey = guid()

        seq { for i in 1 .. 200 -> { mkItem() with HashKey = hKey ; RangeKey = int64 i ; Bool = i % 2 = 0}}
        |> Seq.splitInto 25
        |> Seq.map table.BatchPutItemsAsync
        |> Async.Parallel
        |> Async.Ignore
        |> Async.RunSynchronously

        let results = table.Scan(<@ fun r -> r.HashKey = hKey && r.RangeKey <= 100L && r.Bool = true @>)
        results.Length |> should equal 50

    [<Fact>]
    let ``Simple Parametric Conditional`` () =
        let item = mkItem()
        let key = table.PutItem item
        let cond = table.Template.PrecomputeConditionalExpr <@ fun hk rk r -> r.HashKey = hk && r.RangeKey = rk @>
        table.PutItem(item, cond item.HashKey item.RangeKey)

    [<Fact>]
    let ``Parametric Conditional with optional argument`` () =
        let item = { mkItem() with Optional = None }
        let key = table.PutItem item
        let cond = table.Template.PrecomputeConditionalExpr <@ fun opt r -> r.Optional = opt @>
        table.PutItem(item, cond None)

    [<Fact>]
    let ``Parametric Conditional with invalid param usage`` () =
        let template = table.Template
        fun () -> template.PrecomputeConditionalExpr <@ fun v r -> r.Value = v + 1L @>
        |> shouldFailwith<_, ArgumentException>

        fun () -> template.PrecomputeConditionalExpr <@ fun v r -> r.Value = Option.get v @>
        |> shouldFailwith<_, ArgumentException>

    interface IDisposable with
        member __.Dispose() =
            ignore <| client.DeleteTable(tableName)