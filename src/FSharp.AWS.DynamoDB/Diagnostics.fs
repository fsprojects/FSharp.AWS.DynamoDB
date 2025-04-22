namespace FSharp.AWS.DynamoDB

module internal Activity =
    open System.Diagnostics

    let private activitySource = new ActivitySource "FSharp.AWS.DynamoDB"

    let hasListeners () : bool = activitySource.HasListeners()

    let private addTag (tag: string) (value: obj) (activity: Activity) =
        if activity <> null then
            activity.AddTag(tag, value)
        else
            activity

    /// Adds the hard-coded `db.system`, `rpc.system`, and `rpc.service` tags to the `Activity`.
    let addStandardTags (activity: Activity) =
        activity
        |> addTag "db.system" "dynamodb"
        |> addTag "rpc.system" "aws-api"
        |> addTag "rpc.service" "DynamoDB"

    let addTableName (tableName: string) = addTag "aws.dynamodb.table_names" [| tableName |]

    let addTableNames (tableNames: string seq) = addTag "aws.dynamodb.table_names" (Array.ofSeq tableNames)

    let addOperation (operation: string) = addTag "rpc.method" operation

    let addConsistentRead (consistentRead: bool) = addTag "aws.dynamodb.consistent_read" consistentRead

    let addScanIndexForward (scanIndexForward: bool) = addTag "aws.dynamodb.scan_forward" scanIndexForward

    let addLimit (limit: int option) (activity: Activity) =
        match limit with
        | Some l -> addTag "aws.dynamodb.limit" l activity
        | None -> activity

    let addIndexName (indexName: string option) (activity: Activity) =
        match indexName with
        | Some i -> addTag "aws.dynamodb.index_name" i activity
        | None -> activity

    let addProjection (projection: string) = addTag "aws.dynamodb.projection" projection

    let addCount (count: int) = addTag "aws.dynamodb.count" count

    let addScannedCount (scannedCount: int) = addTag "aws.dynamodb.scanned_count" scannedCount

    /// Starts a new activity for a DynamoDB operation named "{operation} {tableName}" (eg "GetItem MyTable").
    /// Sets the standard tags for all table operations and returns the `Activity` for further customization.
    let startTableActivity (tableName: string) (operation: string) : Activity =
        activitySource.StartActivity(sprintf "%s %s" operation tableName, ActivityKind.Client)
        |> addStandardTags
        |> addTableName tableName
        |> addOperation operation

    /// Starts a new activity for a multi-table DynamoDB operation (eg "BatchGetItem").
    /// Sets the standard tags for all table operations and returns the `Activity` for further customization.
    let startMultipleTableActivity (tableNames: string seq) (operation: string) : Activity =
        activitySource.StartActivity(operation, ActivityKind.Client)
        |> addStandardTags
        |> addTableNames tableNames
        |> addOperation operation

    let addException (ex: exn) (activity: Activity) =
        if activity <> null then
            activity.AddException ex
        else
            activity

    let stop (activity: Activity) =
        if activity <> null then
            activity.Stop()

module internal Meter =
    open System.Diagnostics.Metrics
    open System.Collections.Generic

    let private meter = new Meter "FSharp.AWS.DynamoDB"

    let private consumedReadCapacity =
        meter.CreateHistogram<float>(
            "db.client.operation.consumed_read_capacity",
            unit = "RCU",
            description = "Consumed read capacity units (RCU) for the DynamoDB operation",
            tags = [ KeyValuePair("db.system.name", "aws.dynamodb" :> obj) ]
        )

    let private consumedWriteCapacity =
        meter.CreateHistogram<float>(
            "db.client.operation.consumed_write_capacity",
            unit = "WCU",
            description = "Consumed write capacity units (WCU) for the DynamoDB operation",
            tags = [ KeyValuePair("db.system.name", "aws.dynamodb" :> obj) ]
        )

    let recordConsumedReadCapacity (tableName: string) (operation: string) (rcu: float) =
        if rcu > 0.0 then
            consumedReadCapacity.Record(
                rcu,
                KeyValuePair("db.collection.name", tableName :> obj),
                KeyValuePair("db.operation.name", operation :> obj)
            )

    let recordConsumedWriteCapacity (tableName: string) (operation: string) (wcu: float) =
        if wcu > 0.0 then
            consumedWriteCapacity.Record(
                wcu,
                KeyValuePair("db.collection.name", tableName :> obj),
                KeyValuePair("db.operation.name", operation :> obj)
            )

    let recordConsumedCapacity (tableName: string) (operation: string) (capacity : Amazon.DynamoDBv2.Model.ConsumedCapacity) =
        if capacity <> null then
            recordConsumedReadCapacity tableName operation capacity.ReadCapacityUnits
            recordConsumedWriteCapacity tableName operation capacity.WriteCapacityUnits
            
module internal Task =
    open System.Threading.Tasks
    open System.Diagnostics

    /// Adds an exception to the activity if the task is faulted
    /// Handles unwrapping the inner exception if present
    let addActivityException (activity: Activity) (task: Task<'a>) =
        if task.IsFaulted then
            let exn =
                if task.Exception.InnerExceptions.Count = 1 then
                    task.Exception.InnerExceptions[0]
                else
                    task.Exception
            Activity.addException exn activity |> ignore
        task
