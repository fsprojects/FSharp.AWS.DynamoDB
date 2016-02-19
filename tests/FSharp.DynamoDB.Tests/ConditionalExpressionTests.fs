namespace FSharp.DynamoDB.Tests

open System
open System.Threading

open Xunit
open FsUnit.Xunit

open FSharp.DynamoDB

type Enum = A = 0 | B = 1 | C = 2

type Nested = { NV : string ; NE : Enum }

type CondExprRecord =
    {
        [<HashKey>]
        HashKey : string
        [<RangeKey>]
        RangeKey : string

        Value : int64

        Tuple : int64 * int64

        Nested : Nested

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
            HashKey = guid() ; RangeKey = guid() ; 
            Value = rand() ; Tuple = rand(), rand() ;
            TimeSpan = TimeSpan.FromTicks(rand()) ; DateTimeOffset = DateTimeOffset.Now ; Guid = Guid.NewGuid()
            Bool = false ; Optional = Some (guid()) ; Ref = ref (guid()) ; Bytes = Guid.NewGuid().ToByteArray()
            Nested = { NV = guid() ; NE = enum<Enum> (int (rand()) % 3) } ;
            Map = seq { for i in 0L .. rand() % 5L -> "K" + guid(), rand() } |> Map.ofSeq 
            Set = seq { for i in 0L .. rand() % 5L -> rand() } |> Set.ofSeq
            List = [for i in 0L .. rand() % 5L -> rand() ]
            Serialized = rand(), guid()
        }

    let run = Async.RunSynchronously

    let table = TableContext.GetTableContext<CondExprRecord>(client, tableName, createIfNotExists = true) |> run

    [<Fact>]
    let ``String precondition`` () =
        let item = mkItem()
        let key = table.PutItemAsync item |> run
        fun () -> table.PutItemAsync(item, <@ fun r -> r.HashKey = guid() @>) |> run
        |> shouldFailwith<_, ConditionalCheckFailedException>

        let hkey = item.HashKey
        table.PutItemAsync(item, <@ fun r -> r.HashKey = hkey @>) |> run |> ignore

    [<Fact>]
    let ``Number precondition`` () =
        let item = mkItem()
        let key = table.PutItemAsync item |> run
        fun () -> table.PutItemAsync(item, <@ fun r -> r.Value = rand() @>) |> run
        |> shouldFailwith<_, ConditionalCheckFailedException>

        let value = item.Value
        table.PutItemAsync(item, <@ fun r -> r.Value = value @>) |> run |> ignore

    [<Fact>]
    let ``Bool precondition`` () =
        let item = mkItem()
        let key = table.PutItemAsync item |> run
        let value = item.Bool
        fun () -> table.PutItemAsync(item, <@ fun r -> r.Bool = not value @>) |> run
        |> shouldFailwith<_, ConditionalCheckFailedException>

        table.PutItemAsync(item, <@ fun r -> r.Bool = value @>) |> run |> ignore

    [<Fact>]
    let ``Bytes precondition`` () =
        let item = mkItem()
        let key = table.PutItemAsync item |> run
        let value = item.Bool
        fun () -> table.PutItemAsync(item, <@ fun r -> r.Bytes = Guid.NewGuid().ToByteArray() @>) |> run
        |> shouldFailwith<_, ConditionalCheckFailedException>

        let value = item.Bytes
        table.PutItemAsync(item, <@ fun r -> r.Bytes = value @>) |> run |> ignore

    [<Fact>]
    let ``DateTimeOffset precondition`` () =
        let item = mkItem()
        let key = table.PutItemAsync item |> run
        fun () -> table.PutItemAsync(item, <@ fun r -> r.DateTimeOffset > DateTimeOffset.Now + TimeSpan.FromDays(3.) @>) |> run
        |> shouldFailwith<_, ConditionalCheckFailedException>

        let value = item.DateTimeOffset
        table.PutItemAsync(item, <@ fun r -> r.DateTimeOffset <= DateTimeOffset.Now + TimeSpan.FromDays(3.) @>) |> run |> ignore

    [<Fact>]
    let ``TimeSpan precondition`` () =
        let item = mkItem()
        let key = table.PutItemAsync item |> run
        let UB = item.TimeSpan + item.TimeSpan
        fun () -> table.PutItemAsync(item, <@ fun r -> r.TimeSpan >= UB @>) |> run
        |> shouldFailwith<_, ConditionalCheckFailedException>

        let value = item.DateTimeOffset
        table.PutItemAsync(item, <@ fun r -> r.TimeSpan < UB @>) |> run |> ignore

    [<Fact>]
    let ``Guid precondition`` () =
        let item = mkItem()
        let key = table.PutItemAsync item |> run
        fun () -> table.PutItemAsync(item, <@ fun r -> r.Guid = Guid.NewGuid() @>) |> run
        |> shouldFailwith<_, ConditionalCheckFailedException>

        let value = item.Guid
        table.PutItemAsync(item, <@ fun r -> r.Guid = value @>) |> run |> ignore

    [<Fact>]
    let ``Optional precondition`` () =
        let item = mkItem()
        let key = table.PutItemAsync item |> run
        fun () -> table.PutItemAsync(item, <@ fun r -> r.Optional = None @>) |> run
        |> shouldFailwith<_, ConditionalCheckFailedException>

        let value = item.Optional
        table.PutItemAsync({ item with Optional = None }, <@ fun r -> r.Optional = value @>) |> run |> ignore

        fun () -> table.PutItemAsync(item, <@ fun r -> r.Optional = (guid() |> Some) @>) |> run
        |> shouldFailwith<_, ConditionalCheckFailedException>

    [<Fact>]
    let ``Ref precondition`` () =
        let item = mkItem()
        let key = table.PutItemAsync item |> run
        fun () -> table.PutItemAsync(item, <@ fun r -> r.Ref = (guid() |> ref) @>) |> run
        |> shouldFailwith<_, ConditionalCheckFailedException>

        let value = item.Ref.Value
        table.PutItemAsync(item, <@ fun r -> r.Ref = ref value @>) |> run |> ignore

    [<Fact>]
    let ``Tuple precondition`` () =
        let item = mkItem()
        let key = table.PutItemAsync item |> run
        fun () -> table.PutItemAsync(item, <@ fun r -> fst r.Tuple = rand() @>) |> run
        |> shouldFailwith<_, ConditionalCheckFailedException>

        let value = fst item.Tuple
        table.PutItemAsync(item, <@ fun r -> fst r.Tuple = value @>) |> run |> ignore

    [<Fact>]
    let ``Record precondition`` () =
        let item = mkItem()
        let key = table.PutItemAsync item |> run
        fun () -> table.PutItemAsync(item, <@ fun r -> r.Nested = { NV = guid() ; NE = Enum.C } @>) |> run
        |> shouldFailwith<_, ConditionalCheckFailedException>

        let value = item.Nested.NV
        let enum = item.Nested.NE
        table.PutItemAsync(item, <@ fun r -> r.Nested = { NV = value ; NE = enum } @>) |> run |> ignore

    [<Fact>]
    let ``Nested attribute precondition`` () =
        let item = mkItem()
        let key = table.PutItemAsync item |> run
        fun () -> table.PutItemAsync(item, <@ fun r -> r.Nested.NV = guid() @>) |> run
        |> shouldFailwith<_, ConditionalCheckFailedException>

        let value = item.Nested.NE
        table.PutItemAsync(item, <@ fun r -> r.Nested.NE = value @>) |> run |> ignore

    [<Fact>]
    let ``String-length precondition`` () =
        let item = mkItem()
        let key = table.PutItemAsync item |> run
        let elem = item.HashKey
        fun () -> table.PutItemAsync(item, <@ fun r -> r.HashKey.Length <> elem.Length  @>) |> run
        |> shouldFailwith<_, ConditionalCheckFailedException>

        table.PutItemAsync(item, <@ fun r -> r.HashKey.Length >= elem.Length @>) |> run |> ignore


    [<Fact>]
    let ``Array-length precondition`` () =
        let item = mkItem()
        let key = table.PutItemAsync item |> run
        let bytes = item.Bytes
        fun () -> table.PutItemAsync(item, <@ fun r -> r.Bytes.Length <> bytes.Length @>) |> run
        |> shouldFailwith<_, ConditionalCheckFailedException>

        table.PutItemAsync(item, <@ fun r -> r.Bytes.Length >= bytes.Length @>) |> run |> ignore
        table.PutItemAsync(item, <@ fun r -> r.Bytes |> Array.length >= bytes.Length @>) |> run |> ignore

    [<Fact>]
    let ``List-length precondition`` () =
        let item = mkItem()
        let key = table.PutItemAsync item |> run
        let list = item.List
        fun () -> table.PutItemAsync(item, <@ fun r -> r.List.Length <> list.Length  @>) |> run
        |> shouldFailwith<_, ConditionalCheckFailedException>

        table.PutItemAsync(item, <@ fun r -> r.List.Length >= list.Length @>) |> run |> ignore
        table.PutItemAsync(item, <@ fun r -> List.length r.List >= list.Length @>) |> run |> ignore

    [<Fact>]
    let ``Set-count precondition`` () =
        let item = mkItem()
        let key = table.PutItemAsync item |> run
        let set = item.Set
        fun () -> table.PutItemAsync(item, <@ fun r -> r.Set.Count <> set.Count  @>) |> run
        |> shouldFailwith<_, ConditionalCheckFailedException>

        table.PutItemAsync(item, <@ fun r -> r.Set.Count <= set.Count @>) |> run |> ignore
        table.PutItemAsync(item, <@ fun r -> r.Set |> Set.count >= Set.count set @>) |> run |> ignore

    [<Fact>]
    let ``Set-contains precondition`` () =
        let item = mkItem()
        let key = table.PutItemAsync item |> run
        let elem = item.Set |> Seq.max
        fun () -> table.PutItemAsync(item, <@ fun r -> r.Set.Contains (elem + 1L)  @>) |> run
        |> shouldFailwith<_, ConditionalCheckFailedException>

        table.PutItemAsync(item, <@ fun r -> r.Set.Contains elem @>) |> run |> ignore
        table.PutItemAsync(item, <@ fun r -> r.Set |> Set.contains elem @>) |> run |> ignore

    [<Fact>]
    let ``Map-count precondition`` () =
        let item = mkItem()
        let key = table.PutItemAsync item |> run
        let map = item.Map
        fun () -> table.PutItemAsync(item, <@ fun r -> r.Map.Count <> map.Count @>) |> run
        |> shouldFailwith<_, ConditionalCheckFailedException>

        table.PutItemAsync(item, <@ fun r -> r.Map.Count >= map.Count @>) |> run |> ignore

    [<Fact>]
    let ``Map-contains precondition`` () =
        let item = mkItem()
        let key = table.PutItemAsync item |> run
        let elem = item.Map |> Map.toSeq |> Seq.head |> fst
        fun () -> table.PutItemAsync(item, <@ fun r -> r.Map.ContainsKey (elem + "foo")  @>) |> run
        |> shouldFailwith<_, ConditionalCheckFailedException>

        table.PutItemAsync(item, <@ fun r -> r.Map.ContainsKey elem @>) |> run |> ignore
        table.PutItemAsync(item, <@ fun r -> r.Map |> Map.containsKey elem @>) |> run |> ignore


    [<Fact>]
    let ``Serializable precondition`` () =
        let item = mkItem()
        let key = table.PutItemAsync item |> run
        fun () -> table.PutItemAsync(item, <@ fun r -> r.Serialized = (0L,"")  @>) |> run
        |> shouldFailwith<_, ArgumentException>

    [<Fact>]
    let ``Boolean precondition`` () =
        let item = mkItem()
        let key = table.PutItemAsync item |> run
        table.PutItemAsync(item, <@ fun r -> false || r.HashKey = item.HashKey && not(not(r.RangeKey = item.RangeKey || r.Bool = item.Bool)) @>) |> run |> ignore
        table.PutItemAsync(item, <@ fun r -> r.HashKey = item.HashKey || (true && r.RangeKey = item.RangeKey) @>) |> run |> ignore

    interface IDisposable with
        member __.Dispose() =
            ignore <| client.DeleteTable(tableName)