# FSharp.DynamoDB

FSharp.DynamoDB an F# wrapper over the standard Amazon.DynamoDB library which
allows you to represent table items using F# records and perform updates, queries and scans
using F# quotation expressions.

The API draws heavily on the corresponding [FSharp.Azure.Storage](https://github.com/fsprojects/FSharp.Azure.Storage)
wrapper for Azure table storage.

## NuGet [![NuGet Status](http://img.shields.io/nuget/v/FSharp.DynamoDB.svg?style=flat)](https://www.nuget.org/packages/FSharp.DynamoDB/)
`Install-Package FSharp.DynamoDB`

## Introduction

Table items can be represented using F# records:

```fsharp
open FSharp.DynamoDB

type WorkItemInfo =
	{
		[<HashKey>]
		ProcessId : int64
		[<RangeKey>]
		WorkItemId : int64

		Name : string
		UUID : Guid
		Dependencies : string list
		Started : DateTimeOffset option
	}
```
We can now perfom table operations on DynamoDB like so
```fsharp
open Amazon.DynamoDBv2

let client : IAmazonDynamoDB = ``your DynamoDB client instance``
let table = TableContext.Create<WorkItemInfo>(client, tableName = "workItems", createIfNotExists = true)

let workItem = { ProcessId = 0L ; WorkItemId = 1L ; Name = "Test" ; UUID = guid() ; Dependencies = ["mscorlib"] ; Started = None }

let key : TableKey = table.PutItem(workItem)
let workItem' = table.GetItem(key)
```

Queries and scans can be performeds using quoted predicates

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

## Supported Field Types

FSharp.DynamoDB supports the following field types:
* Numerical types, enumerations and strings.
* Array, Nullable, Guid, DateTimeOffset and TimeSpan.
* F# lists
* F# sets with elements of type number, string or byte[].
* F# maps with key of type string.
* F# records and unions (recursive types not supported).

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
