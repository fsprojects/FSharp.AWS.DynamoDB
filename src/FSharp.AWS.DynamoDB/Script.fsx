#if USE_PUBLISHED_NUGET // If you don't want to do a local build first
#r "nuget: FSharp.AWS.DynamoDB, *-*" // *-* to white-list the fact that all releases to date have been `-beta` sufficed
#else
#I "../../bin/net5.0/"
#r "AWSSDK.Core.dll"
#r "AWSSDK.DynamoDBv2.dll"
#r "FSharp.AWS.DynamoDB.dll"
#endif

open System

open Amazon
open Amazon.Util
open Amazon.DynamoDBv2
open Amazon.DynamoDBv2.Model

open FSharp.AWS.DynamoDB

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
        [<LocalSecondaryIndex>]
        Value : float
        List : int64 list
        Unions : Union list
        String : string ref
        Value2 : int option
        Values : Nested []
        Date : DateTimeOffset
        Map : Map<string, int>
        Set : Set<int64> list
        Bytes : byte[]
    }


let table = TableContext.Create<Test>(ddb, "test", createIfNotExists = true)

let value = { HashKey = Guid.NewGuid() ; List = [] ; RangeKey = "2" ; Value = 3.1415926 ; Date = DateTimeOffset.Now + TimeSpan.FromDays 2. ; Value2 = None ; Values = [|{ A = "foo" ; B = System.Reflection.BindingFlags.Instance }|] ; Map = Map.ofList [("A1",1)] ; Set = [set [1L];set [2L]] ; Bytes = [|1uy..10uy|]; String = ref "1a" ; Unions = [A 42; B("42",3)]}

let key = table.PutItem value
table.GetItem key

table.PrimaryKey
table.LocalSecondaryIndices

table.Query <@ fun r -> r.HashKey = value.HashKey && r.Value >= value.Value @>

let query = table.Template.PrecomputeConditionalExpr <@ fun r -> r.HashKey = value.HashKey && r.Value >= value.Value @>

query.IndexName

#time "on"

// Real: 00:00:07.996, CPU: 00:00:07.937, GC gen0: 213, gen1: 1, gen2: 0
for i = 1 to 1000 do
    let _ = table.Template.PrecomputeUpdateExpr <@ fun r -> { r with Value2 = Some 42} @>
    ()

// Real: 00:01:57.405, CPU: 00:00:19.750, GC gen0: 241, gen1: 13, gen2: 1
for i = 1 to 1000 do
    let _ = table.UpdateItem(key, <@ fun r -> { r with Value2 = Some 42} @>)
    ()

// Real: 00:01:35.912, CPU: 00:00:01.921, GC gen0: 27, gen1: 3, gen2: 1
let uexpr = table.Template.PrecomputeUpdateExpr <@ fun r -> { r with Value2 = Some 42} @>
for i = 1 to 1000 do
    let _ = table.UpdateItem(key, uexpr)
    ()

// Real: 00:01:35.107, CPU: 00:00:02.078, GC gen0: 26, gen1: 2, gen2: 0
let uexpr2 = table.Template.PrecomputeUpdateExpr <@ fun v r -> { r with Value2 = v } @>
for i = 1 to 1000 do
    let _ = table.UpdateItem(key, uexpr2 (Some 42))
    ()