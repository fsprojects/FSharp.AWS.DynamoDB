#I "../../bin"
#r "AWSSDK.Core.dll"
#r "AWSSDK.DynamoDBv2.dll"
#r "FSharp.DynamoDB.dll"

open System

open Amazon
open Amazon.Util
open Amazon.DynamoDBv2
open Amazon.DynamoDBv2.Model

open FSharp.DynamoDB

let account = AWSCredentialsProfile.LoadFrom("default").Credentials
let ddb = new AmazonDynamoDBClient(account, RegionEndpoint.EUCentral1) :> IAmazonDynamoDB

type Nested = { A : string ; B : System.Reflection.BindingFlags }

type Union = A of int | B of string * int

type Test =
    {
        [<HashKey>]
        HashKey : Guid
        [<RangeKey>]
        RangeKey : string
        Value : int
        Unions : Union list
        String : string ref
        Value2 : int option
        Values : Nested []
        Date : DateTimeOffset
        Map : Map<string, int>
        Set : Set<int64> list
        Bytes : byte[]
    }


let table = TableContext.Create<Test>(ddb, "test")
do table.CreateIfNotExists()

let value = { HashKey = Guid.NewGuid() ; RangeKey = "2" ; Value = 40 ; Date = DateTimeOffset.Now + TimeSpan.FromDays 2. ; Value2 = None ; Values = [|{ A = "foo" ; B = System.Reflection.BindingFlags.Instance }|] ; Map = Map.ofList [("A1",1)] ; Set = [set [1L];set [2L]] ; Bytes = [|1uy..10uy|]; String = ref "1a" ; Unions = [A 42; B("42",3)]}

let key = table.PutItem value
table.GetItem key

table.Scan(<@ fun r -> r.Value2.Value < 42 @>)

table.Scan <@ fun t -> t.Date < DateTimeOffset.Now @>

#time "on"

// Real: 00:00:08.917, CPU: 00:00:08.968, GC gen0: 214, gen1: 2, gen2: 1
for i = 1 to 1000 do
    let _ = table.Template.PrecomputeUpdateExpr <@ fun r -> { r with Value2 = Some 42} @>
    ()

// Real: 00:02:09.249, CPU: 00:00:16.171, GC gen0: 242, gen1: 13, gen2: 1
// Real: 00:01:54.275, CPU: 00:00:18.843, GC gen0: 242, gen1: 12, gen2: 1
// Real: 00:02:45.919, CPU: 00:00:09.953, GC gen0: 489, gen1: 17, gen2: 2
// Real: 00:02:42.569, CPU: 00:00:09.562, GC gen0: 489, gen1: 18, gen2: 2
for i = 1 to 1000 do
    let _ = table.UpdateItem(key, <@ fun r -> { r with Value2 = Some 42} @>)
    ()

// Real: 00:02:36.154, CPU: 00:00:02.281, GC gen0: 51, gen1: 4, gen2: 0
// Real: 00:02:29.609, CPU: 00:00:03.000, GC gen0: 52, gen1: 6, gen2: 1
let uexpr = table.Template.PrecomputeUpdateExpr <@ fun r -> { r with Value2 = Some 42} @>
for i = 1 to 1000 do
    let _ = table.UpdateItem(key, uexpr)
    ()

// Real: 00:02:20.947, CPU: 00:00:02.187, GC gen0: 53, gen1: 5, gen2: 0
// Real: 00:02:33.710, CPU: 00:00:02.265, GC gen0: 54, gen1: 6, gen2: 1
let uexpr2 = table.Template.PrecomputeUpdateExpr <@ fun v r -> { r with Value2 = v } @>
for i = 1 to 1000 do
    let _ = table.UpdateItem(key, uexpr2 (Some 42))
    ()