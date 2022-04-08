# FSharp.AWS.DynamoDB

![](https://github.com/fsprojects/FSharp.AWS.DynamoDB/workflows/Build/badge.svg) [![NuGet Badge](https://buildstats.info/nuget/FSharp.AWS.DynamoDB?includePreReleases=true)](https://www.nuget.org/packages/FSharp.AWS.DynamoDB)

`FSharp.AWS.DynamoDB` is an F# wrapper over the standard `AWSSDK.DynamoDBv2` library that
represents Table Items as F# records, enabling one to perform updates, queries and scans
using F# quotation expressions.

The API draws heavily on the corresponding [FSharp.Azure.Storage](https://github.com/fsprojects/FSharp.Azure.Storage)
wrapper for Azure table storage.

## Introduction

Table items can be represented using F# records:

```fsharp
open FSharp.AWS.DynamoDB

type WorkItemInfo =
    {
        [<HashKey>]
        ProcessId : int64
        [<RangeKey>]
        WorkItemId : int64

        Name : string
        UUID : Guid
        Dependencies : Set<string>
        Started : DateTimeOffset option
    }
```

We can now perform table operations on DynamoDB like so:

```fsharp
open Amazon.DynamoDBv2
open FSharp.AWS.DynamoDB.Scripting // Expose non-Async methods, e.g. PutItem/GetItem

let client : IAmazonDynamoDB = ``your DynamoDB client instance``
let table = TableContext.Initialize<WorkItemInfo>(client, tableName = "workItems", Throughput.OnDemand)

let workItem = { ProcessId = 0L ; WorkItemId = 1L ; Name = "Test" ; UUID = guid() ; Dependencies = set ["mscorlib"] ; Started = None }

let key : TableKey = table.PutItem(workItem)
let workItem' = table.GetItem(key)
```

Queries and scans can be performed using quoted predicates:

```fsharp
let qResults = table.Query(keyCondition = <@ fun r -> r.ProcessId = 0 @>, 
                           filterCondition = <@ fun r -> r.Name = "test" @>)
                            
let sResults = table.Scan <@ fun r -> r.Started.Value >= DateTimeOffset.Now - TimeSpan.FromMinutes 1.  @>
```

Values can be updated using quoted update expressions:

```fsharp
let updated = table.UpdateItem(<@ fun r -> { r with Started = Some DateTimeOffset.Now } @>, 
                               preCondition = <@ fun r -> r.DateTimeOffset = None @>)
```

Or they can be updated using [the `SET`, `ADD`, `REMOVE` and `DELETE` operations of the UpdateOp` DSL](./src/FSharp.AWS.DynamoDB/Types.fs#263),
which is closer to the underlying DynamoDB API:

```fsharp
let updated = table.UpdateItem <@ fun r -> SET r.Name "newName" &&& ADD r.Dependencies ["MBrace.Core.dll"] @>
```

## Supported Field Types

`FSharp.AWS.DynamoDB` supports the following field types:
* Numerical types, enumerations and strings.
* Array, Nullable, Guid, DateTimeOffset and TimeSpan.
* F# lists
* F# sets with elements of type number, string or `byte[]`.
* F# maps with key of type string.
* F# records and unions (recursive types not supported, nested ones are).

## Supported operators in Query Expressions

Query expressions support the following F# operators in their predicates:
* `Array.length`, `List.length`, `Set.count` and `Map.Count`.
* `String.StartsWith` and `String.Contains`.
* `Set.contains` and `Map.containsKey`.
* `Array.isEmpty` and `List.isEmpty`.
* `Option.isSome`, `Option.isNone`, `Option.Value` and `Option.get`.
* `fst` and `snd` for tuple records.

## Supported operators in Update Expressions

Update expressions support the following F# value constructors:
* `(+)` and `(-)` in numerical and set types.
* `Array.append` and `List.append` (or `@`).
* List consing (`::`).
* `defaultArg` on optional fields.
* `Set.add` and `Set.remove`.
* `Map.add` and `Map.remove`.
* `Option.Value` and `Option.get`.
* `fst` and `snd` for tuple records.

## Example: Representing an atomic counter as an Item in a DynamoDB Table 

```fsharp
type private CounterEntry = { [<HashKey>] Id : Guid ; Value : int64 }

type Counter private (table : TableContext<CounterEntry>, key : TableKey) =
    
    member _.Value = async {
        let! current = table.GetItemAsync(key)
        return current.Value
    }
    
    member _.Incr() = async { 
        let! updated = table.UpdateItemAsync(key, <@ fun e -> { e with Value = e.Value + 1L } @>)
        return updated.Value
    }

    static member Create(client : IAmazonDynamoDB, tableName : string) = async {
        let table = TableContext<CounterEntry>(client, tableName)
        let throughput = ProvisionedThroughput(readCapacityUnits = 10L, writeCapacityUnits = 10L)        
        do! table.VerifyOrCreateTableAsync(Throughput.Provisioned throughput)
        let initialEntry = { Id = Guid.NewGuid() ; Value = 0L }
        let! key = table.PutItemAsync(initialEntry)
        return Counter(table, key)
    }
```

_NOTE: It's advised to split single time initialization/verification of table creation from the application logic, see [`Script.fsx`](src/FSharp.AWS.DynamoDB/Script.fsx#99) for further details_.

## Projection Expressions

Projection expressions can be used to fetch a subset of table attributes, which can be useful when performing large queries:

```fsharp
table.QueryProjected(<@ fun r -> r.HashKey = "Foo" @>, <@ fun r -> r.HashKey, r.Values.Nested.[0] @>)
```

the resulting value is a tuple of the specified attributes. Tuples can be of any arity but must contain non-conflicting document paths.

## Secondary Indices

[Global Secondary Indices](http://docs.aws.amazon.com/amazondynamodb/latest/developerguide/GSI.html) can be defined using the `GlobalSecondaryHashKey` and `GlobalSecondaryRangeKey` attributes:
```fsharp
type Record =
    {
        [<HashKey>] HashKey : string
        ...
        [<GlobalSecondaryHashKey(indexName = "Index")>] GSIH : string
        [<GlobalSecondaryRangeKey(indexName = "Index")>] GSIR : string
    }
```

Queries can now be performed on the `GSIH` and `GSIR` fields as if they were regular hashkey and rangekey attributes.

_NOTE: Global secondary indices are created using the same provisioned throughput as for the primary keys_.

[Local Secondary Indices](http://docs.aws.amazon.com/amazondynamodb/latest/developerguide/LSI.html) can be defined using the `LocalSecondaryIndex` attribute:
```fsharp
type Record =
    {
        [<HashKey>] HashKey : string
        [<RangeKey>] RangeKey : Guid
        ...
        [<LocalSecondaryIndex>] LSI : double
    }
```

Queries can now be performed using LSI as a secondary RangeKey.

NB: Due to API restrictions, secondary indices in the scope of `FSharp.AWS.DynamoDB` always project *all* table attributes,
which can incur additional costs from Amazon.

### Pagination

Pagination is supported on both scans & queries:
```fsharp
let firstPage = table.ScanPaginated(limit = 100)
printfn "First 100 results = %A" firstPage.Records
match firstPage.LastEvaluatedKey with
| Some key ->
    let nextPage = table.ScanPaginated(limit = 100, exclusiveStartKey = key)
```
Note that the `exclusiveStartKey` on paginated queries must include both the table key fields and the index fields (if querying an LSI or GSI).
This is accomplished via the `IndexKey` type - if constructing manually (eg deserialising a start key from an API call):
```fsharp
let startKey = IndexKey.Combined(gsiHashValue, gsiRangeValue, TableKey.Hash(primaryKey))
let page = table.QueryPaginated(<@ fun t -> t.GsiHash = gsiHashValue @>, limit = 100, exclusiveStartKey = startKey)
```

## Notes on value representation

Due to restrictions of DynamoDB, it may sometimes be the case that objects are not persisted faithfully.
For example, consider the following record definition:

```fsharp
type Record = 
    {         
        [<HashKey>]
        HashKey : Guid

        Optional : int option option
        Lists : int list list
    }
    
let item = { HashKey = Guid.NewGuid() ; Optional = Some None ; Lists = [[1;2];[];[3;4]] }
let key = table.PutItem item
```

Subsequently recovering the given key will result in the following value:

```fsharp
> table.GetItem key
val it : Record = {HashKey = 8d4f0678-6def-4bc9-a0ff-577a53c1337c;
                   Optional = None;
                   Lists = [[1;2]; [3;4]];}
```

## Precomputing DynamoDB Expressions

It is possible to precompute a DynamoDB expression as follows:

```fsharp
let precomputedConditional = table.Template.PrecomputeConditionalExpr <@ fun w -> w.Name <> "test" && w.Dependencies.Contains "mscorlib" @>
```

This precomputed conditional can now be used in place of the original expression in the `FSharp.AWS.DynamoDB` API:

```fsharp
let results = table.Scan precomputedConditional
```

`FSharp.AWS.DynamoDB` also supports precomputation of parametric expressions:

```fsharp
let startedBefore = table.Template.PrecomputeConditionalExpr <@ fun time w -> w.StartTime.Value <= time @>
table.Scan(startedBefore (DateTimeOffset.Now - TimeSpan.FromDays 1.))
```

(See [`Script.fsx`](src/FSharp.AWS.DynamoDB/Script.fsx) for example timings showing the relative efficiency.)

## Observability

A hook is provided so metrics can be published via your preferred Observability provider. For example, using [Prometheus.NET](https://github.com/prometheus-net/prometheus-net):

```fsharp
let dbCounter = Prometheus.Metrics.CreateCounter("aws_dynamodb_requests_total", "Count of all DynamoDB requests", "table", "operation")
let processMetrics (m : RequestMetrics) =
    dbCounter.WithLabels(m.TableName, string m.Operation).Inc()
let table = TableContext<WorkItemInfo>(client, tableName = "workItems", metricsCollector = processMetrics)
```

If `metricsCollector` is supplied, the requests will set `ReturnConsumedCapacity` to `ReturnConsumedCapacity.INDEX` 
and the `RequestMetrics` parameter will contain a list of `ConsumedCapacity` objects returned from the DynamoDB operations.

### Building & Running Tests

To build using the dotnet SDK:

`dotnet tool restore`
`dotnet build`

Tests are run using dynamodb-local on port 8000. Using the docker image is recommended:

`docker run -p 8000:8000 amazon/dynamodb-local`

then

`dotnet test -c Release`

## Maintainer(s)

- [@samritchie](https://github.com/samritchie)

The default maintainer account for projects under "fsprojects" is [@fsprojectsgit](https://github.com/fsprojectsgit) - F# Community Project Incubation Space (repo management)
