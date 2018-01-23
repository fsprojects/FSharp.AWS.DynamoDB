# FSharp.AWS.DynamoDB

FSharp.AWS.DynamoDB an F# wrapper over the standard Amazon.DynamoDB library which
allows you to represent table items using F# records and perform updates, queries and scans
using F# quotation expressions.

The API draws heavily on the corresponding [FSharp.Azure.Storage](https://github.com/fsprojects/FSharp.Azure.Storage)
wrapper for Azure table storage.

## NuGet [![NuGet Badge](https://buildstats.info/nuget/FSharp.AWS.DynamoDB?includePreReleases=true)](https://www.nuget.org/packages/FSharp.AWS.DynamoDB)
`Install-Package FSharp.AWS.DynamoDB`

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
We can now perfom table operations on DynamoDB like so
```fsharp
open Amazon.DynamoDBv2

let client : IAmazonDynamoDB = ``your DynamoDB client instance``
let table = TableContext.Create<WorkItemInfo>(client, tableName = "workItems", createIfNotExists = true)

let workItem = { ProcessId = 0L ; WorkItemId = 1L ; Name = "Test" ; UUID = guid() ; Dependencies = set ["mscorlib"] ; Started = None }

let key : TableKey = table.PutItem(workItem)
let workItem' = table.GetItem(key)
```

Queries and scans can be performed using quoted predicates

```fsharp
let qResults = table.Query(keyCondition = <@ fun r -> r.ProcessId = 0 @>, 
                            filterCondition = <@ fun r -> r.Name = "test" @>)
                            
let sResults = table.Scan <@ fun r -> r.Started.Value >= DateTimeOffset.Now - TimeSpan.FromMinutes 1.  @>
```

Values can be updated using quoted update expressions

```fsharp
let updated = table.UpdateItem(<@ fun r -> { r with Started = Some DateTimeOffset.Now } @>, 
                                preCondition = <@ fun r -> r.DateTimeOffset = None @>)
```

Or they can be updated using the `UpdateOp` DSL
which is closer to the underlying DynamoDB API

```fsharp
let updated = table.UpdateItem <@ fun r -> SET r.Name "newName" &&& ADD r.Dependencies ["MBrace.Core.dll"] @>
```

## Supported Field Types

FSharp.AWS.DynamoDB supports the following field types:
* Numerical types, enumerations and strings.
* Array, Nullable, Guid, DateTimeOffset and TimeSpan.
* F# lists
* F# sets with elements of type number, string or `byte[]`.
* F# maps with key of type string.
* F# records and unions (recursive types not supported).

## Supported methods in Query Expressions

Query expressions support the following F# methods in their predicates:
* `Array.length`, `List.length`, `Set.count` and `Map.Count`.
* `String.StartsWith` and `String.Contains`.
* `Set.contains` and `Map.containsKey`.
* `Array.isEmpty` and `List.isEmpty`.
* `Option.isSome`, `Option.isNone`, `Option.Value` and `Option.get`.
* `fst` and `snd` for tuple records.

## Supported methods in Update Expressions

Update expressions support the following F# value constructors:
* `(+)` and `(-)` in numerical and set types.
* `Array.append` and `List.append` (or `@`).
* List consing (`::`).
* `defaultArg` on optional fields.
* `Set.add` and `Set.remove`.
* `Map.add` and `Map.remove`.
* `Option.Value` and `Option.get`.
* `fst` and `snd` for tuple records.

## Example: Creating an atomic counter

```fsharp
type private CounterEntry = { [<HashKey>]Id : Guid ; Value : int64 }

type Counter private (table : TableContext<CounterEntry>, key : TableKey) =
    member __.Value = table.GetItem(key).Value
    member __.Incr() = 
        let updated = table.UpdateItem(key, <@ fun e -> { e with Value = e.Value + 1L } @>)
        updated.Value

    static member Create(client : IAmazonDynamoDB, table : string) =
        let table = TableContext.Create<CounterEntry>(client, table, createIfNotExists = true)
        let entry = { Id = Guid.NewGuid() ; Value = 0L }
        let key = table.PutItem entry
        new Counter(table, key)
```

## Projection Expressions

Projection expressions can be used to fetch a subset of table attributes, which can be useful when performing large queries:

```fsharp
table.QueryProjected(<@ fun r -> r.HashKey = "Foo" @>, <@ fun r -> r.HashKey, r.Values.Nested.[0] @>)
```
which returns a tuple of specified attributes. Tuples can be of any arity and must contain non-conflicting document paths.

## Secondary Indices

[Global Secondary Indices](http://docs.aws.amazon.com/amazondynamodb/latest/developerguide/GSI.html) can be defined using the `GlobalSecondaryHashKey` and `GlobalSecondaryRangeKey` attributes:
```fsharp
type Record =
    {
        [<HashKey>] HashKey : string
        ...
        [<GlobalSecondaryHashKey(indexName = "Index")>]GSIH : string
        [<GlobalSecondaryRangeKey(indexName = "Index")>]GSIR : string
    }
```
Queries can now be performed on the `GSIH` and `GSIR` fields as if they were regular hashkey and rangekey attributes.
Global secondary indices are created using the same provisioned throughput as the primary keys.

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

NB: Due to API restrictions, secondary indices in the scope of FSharp.AWS.DynamoDB always project *all* table attributes
which can incur additional costs from Amazon.

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
This precomputed conditional can now be used in place of the original expression in the FSharp.AWS.DynamoDB API:
```fsharp
let results = table.Scan precomputedConditional
```
FSharp.AWS.DynamoDB also supports precomputation of parametric expressions:
```fsharp
let startedBefore = table.Template.PrecomputeConditionalExpr <@ fun time w -> w.StartTime.Value <= time @>
table.Scan(startedBefore (DateTimeOffset.Now - TimeSpan.FromDays 1.))
```

### Building & Running Tests

Depending on your platform, you can build and run tests running `build.bat` or `build.cmd`. 
To successfully run unit tests, you need to have credentials set to your `default` profile in 
your local credentials store. Alternative, you could set the following environment variables:
```bash
export AWS_REGION="eu-central-1"                   # defaults to eu-central-1
export AWS_CREDENTIAL_STORE_PROFILE=<profile name> # uses "default" if unset
export AWS_ACCESS_KEY_ID=<your access key>         # your account's access key
export AWS_SECRET_ACCESS_KEY=<your secret key>     # your account's secret key
```

### Build Status

* Windows/.NET [![Build status](https://ci.appveyor.com/api/projects/status/n72o2uj09lr9o3sa/branch/master?svg=true)](https://ci.appveyor.com/project/nessos/fsharp-aws-dynamodb/branch/master)
* Linux/Mono 4.2 [![Build Status](https://travis-ci.org/fsprojects/FSharp.AWS.DynamoDB.png?branch=master)](https://travis-ci.org/fsprojects/FSharp.AWS.DynamoDB/branches)

## Maintainer(s)

- [@eiriktsarpalis](https://github.com/eiriktsarpalis)
- [@AviAvni](https://github.com/AviAvni)

The default maintainer account for projects under "fsprojects" is [@fsprojectsgit](https://github.com/fsprojectsgit) - F# Community Project Incubation Space (repo management)
