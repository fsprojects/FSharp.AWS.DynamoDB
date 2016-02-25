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
        Bytes : string[]
    }


let table = TableContext.Create<Test>(ddb, "test")
do table.CreateIfNotExists()

let value = { HashKey = Guid.NewGuid() ; RangeKey = "2" ; Value = 40 ; Date = DateTimeOffset.Now + TimeSpan.FromDays 2. ; Value2 = None ; Values = [|{ A = "foo" ; B = System.Reflection.BindingFlags.Instance }|] ; Map = Map.ofList [("A1",1)] ; Set = [set [1L];set [2L]] ; Bytes = [|"a";null|]; String = ref "1a" ; Unions = [A 42; B("42",3)]}

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
for i = 1 to 1000 do
    let _ = table.UpdateItem(key, <@ fun r -> { r with Value2 = Some 42} @>)
    ()

// Real: 00:01:29.459, CPU: 00:00:01.578, GC gen0: 27, gen1: 3, gen2: 1
// Real: 00:01:25.446, CPU: 00:00:02.468, GC gen0: 27, gen1: 3, gen2: 1
let uexpr = table.Template.PrecomputeUpdateExpr <@ fun r -> { r with Value2 = Some 42} @>
for i = 1 to 1000 do
    let _ = table.UpdateItem(key, uexpr)
    ()