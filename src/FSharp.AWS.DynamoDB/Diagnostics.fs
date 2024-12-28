namespace FSharp.AWS.DynamoDB

module Diagnostics =
    open System.Diagnostics
    open Amazon.DynamoDBv2.Model
    open System.Text.Json
    
    let private activitySource = new ActivitySource("FSharp.AWS.DynamoDB")

    let hasListeners () : bool = activitySource.HasListeners()

    let addActivityTags (tags: (string * obj) list) (activity: Activity) =
        if notNull activity then
            tags |> List.iter (fun (k, v) -> activity.AddTag(k, v) |> ignore)

    let startTableActivity (tableName: string) (operation: string) (tags: (string * obj) list) : Activity =
        let activity = activitySource.StartActivity(sprintf "%s %s" operation tableName, ActivityKind.Client)
        activity
        |> addActivityTags (
            tags
            |> List.append
                [ ("db.system", "dynamodb")
                  ("aws.dynamodb.table_names", [| tableName |])
                  ("rpc.system", "aws-api")
                  ("rpc.service", "DynamoDB")
                  ("rpc.method", operation) ]
        )
        activity

    let startMultipleTableActivity (tableNames: string seq) (operation: string) (tags: (string * obj) list) : Activity =
        let activity = activitySource.StartActivity(operation, ActivityKind.Client)
        activity
        |> addActivityTags (
            tags
            |> List.append
                [ ("db.system", "dynamodb")
                  ("aws.dynamodb.table_names", tableNames |> Seq.toArray :> obj)
                  ("rpc.system", "aws-api")
                  ("rpc.service", "DynamoDB")
                  ("rpc.method", operation) ]
        )
        activity

    let addActivityCapacity (capacity: ConsumedCapacity seq) (activity: Activity) =
        if notNull activity then
            let value = capacity |> Seq.choose (function | null -> None | c -> Some (JsonSerializer.Serialize c)) |> Seq.toArray
            activity.AddTag("aws.dynamodb.consumed_capacity", value :> obj) |> ignore

    let addActivityException (ex: exn) (activity: Activity) =
        if notNull activity then
            activity.AddException ex |> ignore

    let stopActivity (activity: Activity) =
        if notNull activity then
            activity.Stop()

module internal Task =
    open System.Threading.Tasks
    open System.Diagnostics

    /// Adds an exception to the activity if the task is faulted
    let addActivityException (activity: Activity) (task: Task<'a>) =
        if notNull activity && task.IsFaulted then
            let exn =
                if task.Exception.InnerExceptions.Count = 1 then
                    task.Exception.InnerExceptions[0]
                else
                    task.Exception
            Diagnostics.addActivityException exn activity
        task
