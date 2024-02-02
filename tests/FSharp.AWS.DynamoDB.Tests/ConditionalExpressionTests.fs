namespace FSharp.AWS.DynamoDB.Tests

open System

open Swensen.Unquote
open Xunit

open FSharp.AWS.DynamoDB
open FSharp.AWS.DynamoDB.Scripting

[<AutoOpen>]
module CondExprTypes =

    [<Flags>]
    type Enum =
        | A = 1
        | B = 2
        | C = 4

    type Nested = { NV: string; NE: Enum }

    type Union =
        | UA of int64
        | UB of string

    type CondExprRecord =
        { [<HashKey>]
          HashKey: string
          [<RangeKey>]
          RangeKey: int64

          Value: int64

          Tuple: int64 * int64

          Nested: Nested

          Union: Union

          NestedList: Nested list

          TimeSpan: TimeSpan

          DateTimeOffset: DateTimeOffset

          [<LocalSecondaryIndex>]
          LSI: int64
          [<GlobalSecondaryHashKey(indexName = "GSI")>]
          GSIH: string
          [<GlobalSecondaryRangeKey(indexName = "GSI")>]
          GSIR: int

          Guid: Guid

          Bool: bool

          Bytes: byte[]

          Ref: string ref

          Optional: string option

          List: int64 list
          Array: int64 array

          Map: Map<string, int64>

          Set: Set<int64> }

type ``Conditional Expression Tests``(fixture: TableFixture) =

    let rand = let r = Random.Shared in fun () -> int64 <| r.Next()

    let mkItem () =
        { HashKey = guid ()
          RangeKey = rand ()
          Value = rand ()
          Tuple = rand (), rand ()
          TimeSpan = TimeSpan.FromTicks(rand ())
          DateTimeOffset = DateTimeOffset.Now
          Guid = Guid.NewGuid()
          Bool = false
          Optional = Some(guid ())
          Ref = ref (guid ())
          Bytes = Guid.NewGuid().ToByteArray()
          Nested =
            { NV = guid ()
              NE = enum<Enum> (int (rand ()) % 3) }
          NestedList =
            [ { NV = guid ()
                NE = enum<Enum> (int (rand ()) % 3) } ]
          LSI = rand ()
          GSIH = guid ()
          GSIR = int (rand ())
          Map = seq { for _ in 0L .. rand () % 5L -> "K" + guid (), rand () } |> Map.ofSeq
          Set = seq { for _ in 0L .. rand () % 5L -> rand () } |> Set.ofSeq
          List = [ for _ in 0L .. rand () % 5L -> rand () ]
          Array = [| for _ in 0L .. rand () % 5L -> rand () |]
          Union = if rand () % 2L = 0L then UA(rand ()) else UB(guid ()) }

    let table = fixture.CreateEmpty<CondExprRecord>()

    [<Fact>]
    let ``Item exists precondition`` () =
        let item = mkItem ()

        fun () -> table.PutItem(item, precondition = itemExists)
        |> shouldFailwith<_, ConditionalCheckFailedException>

        let _key = table.PutItem item
        table.PutItem(item, precondition = itemExists) =! _key

    [<Fact>]
    let ``Item not exists precondition`` () =
        let item = mkItem ()
        let _key = table.PutItem(item, precondition = itemDoesNotExist)

        fun () -> table.PutItem(item, precondition = itemDoesNotExist)
        |> shouldFailwith<_, ConditionalCheckFailedException>

    [<Fact(Skip = "Does not appear to work against dynamodb-local")>]
    let ``Item not exists failure should add conflicting item data to the exception`` () =
        let item = mkItem ()
        let _key = table.PutItem(item, precondition = itemDoesNotExist)

        raisesWith<ConditionalCheckFailedException>
            <@
                table.PutItemAsync(item, precondition = itemDoesNotExist)
                |> Async.RunSynchronously
            @>
            (fun e -> <@ e.Item.Count > 0 @>)

    [<Fact>]
    let ``String precondition`` () =
        let item = mkItem ()
        let _key = table.PutItem item

        fun () -> table.PutItem(item, <@ fun r -> r.HashKey = guid () @>)
        |> shouldFailwith<_, ConditionalCheckFailedException>

        let hkey = item.HashKey
        table.PutItem(item, <@ fun r -> r.HashKey = hkey @>) =! _key

    [<Fact>]
    let ``Number precondition`` () =
        let item = mkItem ()
        let _key = table.PutItem item

        fun () -> table.PutItem(item, <@ fun r -> r.Value = rand () @>)
        |> shouldFailwith<_, ConditionalCheckFailedException>

        let value = item.Value
        table.PutItem(item, <@ fun r -> r.Value = value @>) =! _key

    [<Fact>]
    let ``Bool precondition`` () =
        let item = mkItem ()
        let _key = table.PutItem item
        let value = item.Bool

        fun () -> table.PutItem(item, <@ fun r -> r.Bool = not value @>)
        |> shouldFailwith<_, ConditionalCheckFailedException>

        table.PutItem(item, <@ fun r -> r.Bool = value @>) =! _key

    [<Fact>]
    let ``Bytes precondition`` () =
        let item = mkItem ()
        let _key = table.PutItem item
        let _value = item.Bool

        fun () -> table.PutItem(item, <@ fun r -> r.Bytes = Guid.NewGuid().ToByteArray() @>)
        |> shouldFailwith<_, ConditionalCheckFailedException>

        let value = item.Bytes
        table.PutItem(item, <@ fun r -> r.Bytes = value @>) =! _key

    [<Fact>]
    let ``DateTimeOffset precondition`` () =
        let item = mkItem ()
        let _key = table.PutItem item

        fun () -> table.PutItem(item, <@ fun r -> r.DateTimeOffset > DateTimeOffset.Now + TimeSpan.FromDays(3.) @>)
        |> shouldFailwith<_, ConditionalCheckFailedException>

        let _value = item.DateTimeOffset

        table.PutItem(item, <@ fun r -> r.DateTimeOffset <= DateTimeOffset.Now + TimeSpan.FromDays(3.) @>)
        =! _key

    [<Fact>]
    let ``TimeSpan precondition`` () =
        let item = mkItem ()
        let _key = table.PutItem item
        let UB = item.TimeSpan + item.TimeSpan

        fun () -> table.PutItem(item, <@ fun r -> r.TimeSpan >= UB @>)
        |> shouldFailwith<_, ConditionalCheckFailedException>

        let _value = item.DateTimeOffset
        table.PutItem(item, <@ fun r -> r.TimeSpan < UB @>) =! _key

    [<Fact>]
    let ``Guid precondition`` () =
        let item = mkItem ()
        let _key = table.PutItem item

        fun () -> table.PutItem(item, <@ fun r -> r.Guid = Guid.NewGuid() @>)
        |> shouldFailwith<_, ConditionalCheckFailedException>

        let value = item.Guid
        table.PutItem(item, <@ fun r -> r.Guid = value @>) =! _key

    [<Fact>]
    let ``Guid not equal precondition`` () =
        let item = mkItem ()
        let _key = table.PutItem item
        let value = item.Guid

        fun () -> table.PutItem(item, <@ fun r -> r.Guid <> value @>)
        |> shouldFailwith<_, ConditionalCheckFailedException>


        table.PutItem(item, <@ fun r -> r.Guid <> Guid.NewGuid() @>) =! _key

    [<Fact>]
    let ``Optional precondition`` () =
        let item = mkItem ()
        let _key = table.PutItem item

        fun () -> table.PutItem(item, <@ fun r -> r.Optional = None @>)
        |> shouldFailwith<_, ConditionalCheckFailedException>

        let value = item.Optional

        table.PutItem({ item with Optional = None }, <@ fun r -> r.Optional = value @>)
        =! _key

        fun () -> table.PutItem(item, <@ fun r -> r.Optional = (guid () |> Some) @>)
        |> shouldFailwith<_, ConditionalCheckFailedException>

    [<Fact>]
    let ``Optional-Value precondition`` () =
        let item = { mkItem () with Optional = Some "foo" }
        let _key = table.PutItem item

        fun () -> table.PutItem(item, <@ fun r -> r.Optional.Value = "bar" @>)
        |> shouldFailwith<_, ConditionalCheckFailedException>

        let _value = item.Optional

        let _ =
            table.PutItem({ item with Optional = None }, <@ fun r -> r.Optional.Value = "foo" @>)

        fun () -> table.PutItem(item, <@ fun r -> r.Optional.Value = "foo" @>)
        |> shouldFailwith<_, ConditionalCheckFailedException>

    [<Fact>]
    let ``Ref precondition`` () =
        let item = mkItem ()
        let _key = table.PutItem item

        fun () -> table.PutItem(item, <@ fun r -> r.Ref = (guid () |> ref) @>)
        |> shouldFailwith<_, ConditionalCheckFailedException>

        let value = item.Ref.Value
        table.PutItem(item, <@ fun r -> r.Ref = ref value @>) =! _key

    [<Fact>]
    let ``Tuple precondition`` () =
        let item = mkItem ()
        let _key = table.PutItem item

        fun () -> table.PutItem(item, <@ fun r -> fst r.Tuple = rand () @>)
        |> shouldFailwith<_, ConditionalCheckFailedException>

        let value = fst item.Tuple
        table.PutItem(item, <@ fun r -> fst r.Tuple = value @>) =! _key

    [<Fact>]
    let ``Record precondition`` () =
        let item = mkItem ()
        let _key = table.PutItem item

        fun () -> table.PutItem(item, <@ fun r -> r.Nested = { NV = guid (); NE = Enum.C } @>)
        |> shouldFailwith<_, ConditionalCheckFailedException>

        let value = item.Nested.NV
        let enum = item.Nested.NE
        table.PutItem(item, <@ fun r -> r.Nested = { NV = value; NE = enum } @>) =! _key

    [<Fact>]
    let ``Nested attribute precondition`` () =
        let item = mkItem ()
        let _key = table.PutItem item

        fun () -> table.PutItem(item, <@ fun r -> r.Nested.NV = guid () @>)
        |> shouldFailwith<_, ConditionalCheckFailedException>

        let value = item.Nested.NE
        table.PutItem(item, <@ fun r -> r.Nested.NE = value @>) =! _key

    [<Fact>]
    let ``Nested union precondition`` () =
        let item = mkItem ()
        let _key = table.PutItem item

        fun () -> table.PutItem(item, <@ fun r -> r.Union = UA(rand ()) @>)
        |> shouldFailwith<_, ConditionalCheckFailedException>

        table.PutItem(item, <@ fun r -> r.Union = item.Union @>) =! _key

    [<Fact>]
    let ``String-Contains precondition`` () =
        let item = { mkItem () with Ref = ref "12-42-12" }
        let _key = table.PutItem item

        fun () -> table.PutItem(item, <@ fun r -> r.Ref.Value.Contains "41" @>)
        |> shouldFailwith<_, ConditionalCheckFailedException>

        table.PutItem(item, <@ fun r -> r.Ref.Value.Contains "42" @>) =! _key

    [<Fact>]
    let ``String-StartsWith precondition`` () =
        let item = { mkItem () with Ref = ref "12-42-12" }
        let _key = table.PutItem item

        fun () -> table.PutItem(item, <@ fun r -> r.Ref.Value.StartsWith "41" @>)
        |> shouldFailwith<_, ConditionalCheckFailedException>

        table.PutItem(item, <@ fun r -> r.Ref.Value.StartsWith "12" @>) =! _key

    [<Fact>]
    let ``String-length precondition`` () =
        let item = mkItem ()
        let _key = table.PutItem item
        let elem = item.HashKey

        fun () -> table.PutItem(item, <@ fun r -> r.HashKey.Length <> elem.Length @>)
        |> shouldFailwith<_, ConditionalCheckFailedException>

        table.PutItem(item, <@ fun r -> r.HashKey.Length >= elem.Length @>) =! _key


    [<Fact>]
    let ``Array-length precondition`` () =
        let item = mkItem ()
        let _key = table.PutItem item
        let bytes = item.Bytes

        fun () -> table.PutItem(item, <@ fun r -> r.Bytes.Length <> bytes.Length @>)
        |> shouldFailwith<_, ConditionalCheckFailedException>

        table.PutItem(item, <@ fun r -> r.Bytes.Length >= bytes.Length @>) =! _key

        table.PutItem(item, <@ fun r -> r.Bytes |> Array.length >= bytes.Length @>)
        =! _key

    [<Fact>]
    let ``Array index precondition`` () =
        let item = mkItem ()
        let _key = table.PutItem item
        let nested = item.NestedList[0]

        fun () -> table.PutItem(item, <@ fun r -> r.NestedList[0].NV = guid () @>)
        |> shouldFailwith<_, ConditionalCheckFailedException>

        table.PutItem(item, <@ fun r -> r.NestedList[0] = nested @>) =! _key

    [<Fact>]
    let ``List-length precondition`` () =
        let item = mkItem ()
        let _key = table.PutItem item
        let list = item.List

        fun () -> table.PutItem(item, <@ fun r -> r.List.Length <> list.Length @>)
        |> shouldFailwith<_, ConditionalCheckFailedException>

        table.PutItem(item, <@ fun r -> r.List.Length >= list.Length @>) =! _key
        table.PutItem(item, <@ fun r -> List.length r.List >= list.Length @>) =! _key

    [<Fact>]
    let ``List-isEmpty precondition`` () =
        let item = { mkItem () with List = [] }
        let _key = table.PutItem item

        table.PutItem({ item with List = [ 42L ] }, <@ fun r -> List.isEmpty r.List @>)
        =! _key

        fun () -> table.PutItem(item, <@ fun r -> List.isEmpty r.List @>)
        |> shouldFailwith<_, ConditionalCheckFailedException>


    [<Fact>]
    let ``Set-count precondition`` () =
        let item = mkItem ()
        let _key = table.PutItem item
        let set = item.Set

        fun () -> table.PutItem(item, <@ fun r -> r.Set.Count <> set.Count @>)
        |> shouldFailwith<_, ConditionalCheckFailedException>

        table.PutItem(item, <@ fun r -> r.Set.Count <= set.Count @>) =! _key
        table.PutItem(item, <@ fun r -> r.Set |> Set.count >= Set.count set @>) =! _key

    [<Fact>]
    let ``Set-contains precondition`` () =
        let item = mkItem ()
        let _key = table.PutItem item
        let elem = item.Set |> Seq.max

        fun () -> table.PutItem(item, <@ fun r -> r.Set.Contains(elem + 1L) @>)
        |> shouldFailwith<_, ConditionalCheckFailedException>

        table.PutItem(item, <@ fun r -> r.Set.Contains elem @>) =! _key
        table.PutItem(item, <@ fun r -> r.Set |> Set.contains elem @>) =! _key

    [<Fact>]
    let ``Map-count precondition`` () =
        let item = mkItem ()
        let _key = table.PutItem item
        let map = item.Map

        fun () -> table.PutItem(item, <@ fun r -> r.Map.Count <> map.Count @>)
        |> shouldFailwith<_, ConditionalCheckFailedException>

        table.PutItem(item, <@ fun r -> r.Map.Count >= map.Count @>) =! _key

    [<Fact>]
    let ``Map-contains precondition`` () =
        let item = mkItem ()
        let _key = table.PutItem item
        let elem = item.Map |> Map.toSeq |> Seq.head |> fst

        fun () -> table.PutItem(item, <@ fun r -> r.Map.ContainsKey(elem + "foo") @>)
        |> shouldFailwith<_, ConditionalCheckFailedException>

        table.PutItem(item, <@ fun r -> r.Map.ContainsKey elem @>) =! _key
        table.PutItem(item, <@ fun r -> r.Map |> Map.containsKey elem @>) =! _key

    [<Fact>]
    let ``Map Item precondition`` () =
        let item =
            { mkItem () with
                Map = Map.ofList [ ("A", 42L) ] }

        let _key = table.PutItem item

        fun () -> table.PutItem(item, <@ fun r -> r.Map["A"] = 41L @>)
        |> shouldFailwith<_, ConditionalCheckFailedException>

        table.PutItem(item, <@ fun r -> r.Map["A"] = 42L @>) |> ignore

    [<Fact>]
    let ``Map Item parametric precondition`` () =
        let item =
            { mkItem () with
                Map = Map.ofList [ ("A", 42L) ] }

        let _key = table.PutItem item
        let cond = table.Template.PrecomputeConditionalExpr <@ fun k v r -> r.Map[k] = v @>

        fun () -> table.PutItem(item, cond "A" 41L)
        |> shouldFailwith<_, ConditionalCheckFailedException>

        table.PutItem(item, cond "A" 42L) =! _key


    [<Fact>]
    let ``Fail on identical comparands`` () =
        fun () -> table.Template.PrecomputeConditionalExpr <@ fun r -> r.Guid < r.Guid @>
        |> shouldFailwith<_, ArgumentException>

        fun () -> table.Template.PrecomputeConditionalExpr <@ fun r -> r.Bytes.Length = r.Bytes.Length @>
        |> shouldFailwith<_, ArgumentException>

    [<Fact>]
    let ``EXISTS precondition`` () =
        let item = { mkItem () with List = [ 1L ] }
        let _key = table.PutItem item
        let _ = table.PutItem(item, precondition = <@ fun r -> EXISTS r.List[0] @>)

        fun () -> table.PutItem(item, precondition = <@ fun r -> EXISTS r.List[1] @>)
        |> shouldFailwith<_, ConditionalCheckFailedException>

    [<Fact>]
    let ``NOT_EXISTS precondition`` () =
        let item = { mkItem () with List = [ 1L ] }
        let _key = table.PutItem item
        let _ = table.PutItem(item, precondition = <@ fun r -> NOT_EXISTS r.List[1] @>)

        fun () -> table.PutItem(item, precondition = <@ fun r -> NOT_EXISTS r.List[0] @>)
        |> shouldFailwith<_, ConditionalCheckFailedException>

    [<Fact>]
    let ``Boolean precondition`` () =
        let item = mkItem ()
        let _key = table.PutItem item

        table.PutItem(
            item,
            <@
                fun r ->
                    false
                    || r.HashKey = item.HashKey
                       && not (not (r.RangeKey = item.RangeKey || r.Bool = item.Bool))
            @>
        )
        =! _key

        table.PutItem(item, <@ fun r -> r.HashKey = item.HashKey || (true && r.RangeKey = item.RangeKey) @>)
        =! _key

    [<Fact>]
    let ``Simple Query Expression`` () =
        let hKey = guid ()

        seq {
            for i in 1..200 ->
                { mkItem () with
                    HashKey = hKey
                    RangeKey = int64 i }
        }
        |> Seq.splitInto 25
        |> Seq.map table.BatchPutItemsAsync
        |> Async.Parallel
        |> Async.Ignore
        |> Async.RunSynchronously


        let results =
            table.Query(<@ fun r -> r.HashKey = hKey && BETWEEN r.RangeKey 50L 149L @>)

        test <@ 100 = results.Length @>

    [<Fact>]
    let ``Simple Query/Filter Expression`` () =
        let hKey = guid ()

        seq {
            for i in 1..200 ->
                { mkItem () with
                    HashKey = hKey
                    RangeKey = int64 i
                    Bool = i % 2 = 0 }
        }
        |> Seq.splitInto 25
        |> Seq.map table.BatchPutItemsAsync
        |> Async.Parallel
        |> Async.Ignore
        |> Async.RunSynchronously

        let results =
            table.Query(
                <@ fun r -> r.HashKey = hKey && BETWEEN r.RangeKey 50L 149L @>,
                filterCondition = <@ fun r -> r.Bool = true @>
            )

        test <@ 50 = results.Length @>

    [<Fact>]
    let ``Detect incompatible key conditions`` () =
        let test outcome q =
            test <@ outcome = table.Template.PrecomputeConditionalExpr(q).IsKeyConditionCompatible @>

        test true <@ fun r -> r.HashKey = "2" @>
        test true <@ fun r -> r.HashKey = "2" && r.RangeKey < 2L @>
        test true <@ fun r -> r.HashKey = "2" && BETWEEN r.RangeKey 1L 2L @>
        test true <@ fun r -> r.HashKey = "1" && r.LSI > 1L @>
        test true <@ fun r -> r.GSIH = "1" && r.GSIR < 1 @>
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
        test false <@ fun r -> r.HashKey = "2" && r.GSIR = 2 @>
        test false <@ fun r -> r.GSIH = "1" && r.LSI > 1L @>

    [<Fact>]
    let ``Detect incompatible comparisons`` () =
        let test outcome q =
            let f () =
                table.Template.PrecomputeConditionalExpr(q)

            if outcome then
                f () |> ignore
            else
                shouldFailwith<_, ArgumentException> f

        test true <@ fun r -> r.Guid > Guid.Empty @>
        test true <@ fun r -> r.Bool > false @>
        test true <@ fun r -> r.Optional >= Some "1" @>
        test false <@ fun r -> r.Map > Map.empty @>
        test false <@ fun r -> r.Set > Set.empty @>
        test false <@ fun r -> r.Ref > ref "12" @>
        test false <@ fun r -> r.Tuple <= (1L, 2L) @>
        test false <@ fun r -> r.Nested <= r.Nested @>

    [<Fact>]
    let ``Simple Scan Expression`` () =
        let hKey = guid ()

        seq {
            for i in 1..200 ->
                { mkItem () with
                    HashKey = hKey
                    RangeKey = int64 i
                    Bool = i % 2 = 0 }
        }
        |> Seq.splitInto 25
        |> Seq.map table.BatchPutItemsAsync
        |> Async.Parallel
        |> Async.Ignore
        |> Async.RunSynchronously

        let results =
            table.Scan(<@ fun r -> r.HashKey = hKey && r.RangeKey <= 100L && r.Bool = true @>)

        test <@ 50 = results.Length @>

    [<Fact>]
    let ``Simple Parametric Conditional`` () =
        let item = mkItem ()
        let _key = table.PutItem item

        let cond =
            table.Template.PrecomputeConditionalExpr <@ fun hk rk r -> r.HashKey = hk && r.RangeKey = rk @>

        table.PutItem(item, cond item.HashKey item.RangeKey) =! _key

    [<Fact>]
    let ``Parametric Conditional with optional argument`` () =
        let item = { mkItem () with Optional = None }
        let _key = table.PutItem item

        let cond =
            table.Template.PrecomputeConditionalExpr <@ fun opt r -> r.Optional = opt @>

        table.PutItem(item, cond None) =! _key

    [<Fact>]
    let ``Parametric Conditional with invalid param usage`` () =
        let template = table.Template

        fun () -> template.PrecomputeConditionalExpr <@ fun v r -> r.Value = v + 1L @>
        |> shouldFailwith<_, ArgumentException>

        fun () -> template.PrecomputeConditionalExpr <@ fun v r -> r.Value = Option.get v @>
        |> shouldFailwith<_, ArgumentException>

    [<Fact>]
    let ``Global Secondary index query`` () =
        let hKey = guid ()

        seq { for i in 1..200 -> { mkItem () with GSIH = hKey; GSIR = i } }
        |> Seq.splitInto 25
        |> Seq.map table.BatchPutItemsAsync
        |> Async.Parallel
        |> Async.Ignore
        |> Async.RunSynchronously

        let result = table.Query <@ fun r -> r.GSIH = hKey && BETWEEN r.GSIR 101 200 @>
        test <@ 100 = result.Length @>


    [<Fact>]
    let ``Local Secondary index query`` () =
        let hKey = guid ()

        seq {
            for i in 1..200 ->
                { mkItem () with
                    HashKey = hKey
                    LSI = int64 i }
        }
        |> Seq.splitInto 25
        |> Seq.map table.BatchPutItemsAsync
        |> Async.Parallel
        |> Async.Ignore
        |> Async.RunSynchronously

        let result = table.Query <@ fun r -> r.HashKey = hKey && BETWEEN r.LSI 101L 200L @>
        test <@ 100 = result.Length @>

    let testScan items (expr: Quotations.Expr<(CondExprRecord -> bool)>) =
        let res = table.Scan expr
        test <@ res.Length = items @>

    [<Fact>]
    let ``Can check if table value is contained in a list or array of values`` () =
        let item = mkItem ()
        let elem = [| item.Value + 10L; item.Value - 10L; item.Value |]
        let elemL = [ item.Value + 10L; item.Value - 10L; item.Value ]

        let _key = table.PutItem item

        testScan 1 <@ fun r -> [| item.Value + 10L; item.Value - 10L; item.Value |] |> Array.contains r.Value @>
        testScan 1 <@ fun r -> elem |> Array.contains r.Value @>
        testScan 1 <@ fun r -> elemL |> List.contains r.Value @>

    [<Fact>]
    let ``Contains doesn't break with 1 element`` () =
        let item = mkItem ()
        let elem = [| item.Value |]

        let _key = table.PutItem item

        testScan 1 <@ fun r -> elem |> Array.contains r.Value @>

    [<Fact>]
    let ``Table List or Array contains item`` () =
        let item = mkItem ()

        let _key = table.PutItem item

        testScan 1 <@ fun r -> r.List |> List.contains item.List[0] @>
        testScan 1 <@ fun r -> r.Array |> Array.contains item.Array[0] @>

    interface IClassFixture<TableFixture>
