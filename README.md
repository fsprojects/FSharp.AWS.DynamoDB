# FSharp.DynamoDB

FSharp.DynamoDB an F# wrapper over the standard Amazon.DynamoDB library which
allows you to represent table items using F# records and perform updates, queries and scans
using F# quotation expressions.

The API draws heavily on the corresponding [FSharp.Azure.Storage](https://github.com/fsprojects/FSharp.Azure.Storage)
wrapper for Azure table storage.

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
                            
let sResults = table.Scan <@ fun r -> r.Started.Value < DateTimeOffset.Now - TimeSpan.FromMinutes 1.  @>
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
* F# sets with elements of type number, string or byte[].
* F# Maps with key of type string.
* F# records and unions (recursive types not supported).
