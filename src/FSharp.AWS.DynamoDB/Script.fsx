#if USE_PUBLISHED_NUGET // If you don't want to do a local build first
#r "nuget: FSharp.AWS.DynamoDB, *-*" // *-* to white-list the fact that all releases to date have been `-beta` sufficed
#else
#I "../../tests/FSharp.AWS.DynamoDB.Tests/bin/Debug/net6.0/"
#r "AWSSDK.Core.dll"
#r "AWSSDK.DynamoDBv2.dll"
#r "FSharp.AWS.DynamoDB.dll"
#endif

open System

open Amazon.DynamoDBv2

open FSharp.AWS.DynamoDB
open FSharp.AWS.DynamoDB.Scripting // non-Async overloads

#if USE_CLOUD
open Amazon
open Amazon.Util
let account = AWSCredentialsProfile.LoadFrom("default").Credentials
let ddb = new AmazonDynamoDBClient(account, RegionEndpoint.EUCentral1) :> IAmazonDynamoDB
#else // Use Docker-hosted dynamodb-local instance
// See https://docs.aws.amazon.com/amazondynamodb/latest/developerguide/DynamoDBLocal.DownloadingAndRunning.html#docker for details of how to deploy a simulator instance
#if USE_CREDS_FROM_ENV_VARS // 'AWS_ACCESS_KEY_ID' and 'AWS_SECRET_ACCESS_KEY' must be set for this to work
let credentials = AWSCredentials.FromEnvironmentVariables()
#else
// Credentials are not validated if connecting to local instance so anything will do (this avoids it looking for profiles to be configured)
let credentials = Amazon.Runtime.BasicAWSCredentials("A", "A")
#endif
let clientConfig = AmazonDynamoDBConfig(ServiceURL = "http://localhost:8000")
let ddb = new AmazonDynamoDBClient(credentials, clientConfig) :> IAmazonDynamoDB
#endif

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

let throughput = ProvisionedThroughput(readCapacityUnits = 10L, writeCapacityUnits = 10L)
let table = TableContext.Initialize<Test>(ddb, "test", Throughput.Provisioned throughput)

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

(* Expanded version of README sample that illustrates how one can better split Table initialization from application logic *)

type internal CounterEntry = { [<HashKey>] Id : Guid ; Value : int64 }

/// Represents a single Item in a Counters Table
type Counter internal (table : TableContext<CounterEntry>, key : TableKey) =

    static member internal Start(table : TableContext<CounterEntry>) = async {
        let initialEntry = { Id = Guid.NewGuid() ; Value = 0L }
        let! key = table.PutItemAsync(initialEntry)
        return Counter(table, key)
    }

    member _.Value = async {
        let! current = table.GetItemAsync(key)
        return current.Value
    }

    member _.Incr() = async {
        let! updated = table.UpdateItemAsync(key, <@ fun (e : CounterEntry) -> { e with Value = e.Value + 1L } @>)
        return updated.Value
    }

/// Wrapper that creates/verifies the table only once per call to Create()
/// This does assume that your application will be sufficiently privileged to create tables on the fly
type EasyCounters private (table : TableContext<CounterEntry>) =

    // We only want to do the initialization bit once per instance of our application
    static member Create(client : IAmazonDynamoDB, tableName : string) : Async<EasyCounters> = async {
        let table = TableContext<CounterEntry>(client, tableName)
        // Create the table if necessary. Verifies schema is correct if it has already been created
        // NOTE the hard coded initial throughput provisioning - arguably this belongs outside of your application logic
        let throughput = ProvisionedThroughput(readCapacityUnits = 10L, writeCapacityUnits = 10L)
        do! table.InitializeTableAsync(Throughput.Provisioned throughput)
        return EasyCounters(table)
    }

    member _.StartCounter() : Async<Counter> =
        Counter.Start table

/// Variant of EasyCounters that splits the provisioning step from the (optional) validation that the table is present
type SimpleCounters private (table : TableContext<CounterEntry>) =

    static member Provision(client : IAmazonDynamoDB, tableName : string, readCapacityUnits, writeCapacityUnits) : Async<unit> =
        let table = TableContext<CounterEntry>(client, tableName)
        // normally, RCU/WCU provisioning only happens first time the Table is created and is then considered an external concern
        // here we use `ProvisionTableAsync` instead of `InitializeTableAsync` to reset it each time we deploy the app
        let provisionedThroughput = ProvisionedThroughput(readCapacityUnits, writeCapacityUnits)
        table.ProvisionTableAsync(Throughput.Provisioned provisionedThroughput)

    static member ProvisionOnDemand(client : IAmazonDynamoDB, tableName : string) : Async<unit> =
        let table = TableContext<CounterEntry>(client, tableName)
        table.ProvisionTableAsync(Throughput.OnDemand)

    /// We only want to do the initialization bit once per instance of our application
    /// Similar to EasyCounters.Create in that it ensures the table is provisioned correctly
    /// However it will never actually create the table
    static member CreateWithVerify(client : IAmazonDynamoDB, tableName : string) : Async<SimpleCounters> = async {
        let table = TableContext<CounterEntry>(client, tableName)
        // This validates the Table has been created correctly
        // (in general this is a good idea, but it is an optional step so it can be skipped, i.e. see Create() below)
        do! table.VerifyTableAsync()
        return SimpleCounters(table)
    }

    /// Assumes the table has been provisioned externally via Provision()
    static member Create(client : IAmazonDynamoDB, tableName : string) : SimpleCounters =
        // NOTE we are skipping
        SimpleCounters(TableContext<CounterEntry>(client, tableName))

    member _.StartCounter() : Async<Counter> =
        Counter.Start table

let e = EasyCounters.Create(ddb, "testing") |> Async.RunSynchronously
let e1 = e.StartCounter() |> Async.RunSynchronously
let e2 = e.StartCounter() |> Async.RunSynchronously
e1.Incr() |> Async.RunSynchronously
e2.Incr() |> Async.RunSynchronously

// First, we create it in On-Demand mode
SimpleCounters.ProvisionOnDemand(ddb, "testing-pre-provisioned") |> Async.RunSynchronously
// Then we flip it to Provisioned mode
SimpleCounters.Provision(ddb, "testing-pre-provisioned", readCapacityUnits = 10L, writeCapacityUnits = 10L) |> Async.RunSynchronously
// The consuming code can assume the provisioning has been carried out as part of the deploy
// that allows the creation to be synchronous (and not impede application startup)
let s = SimpleCounters.Create(ddb, "testing-pre-provisioned")
let s1 = s.StartCounter() |> Async.RunSynchronously // Throws if Provision step has not been executed
s1.Incr() |> Async.RunSynchronously

// Alternately, we can have the app do an extra call (and have some asynchronous initialization work) to check the table is ready
let v = SimpleCounters.CreateWithVerify(ddb, "testing-not-present") |> Async.RunSynchronously // Throws, as table not present
let v2 = v.StartCounter() |> Async.RunSynchronously
v2.Incr() |> Async.RunSynchronously

// (TOCONSIDER: Illustrate how to use AsyncCacheCell from https://github.com/jet/equinox/blob/master/src/Equinox.Core/AsyncCacheCell.fs to make Verify call lazy)
