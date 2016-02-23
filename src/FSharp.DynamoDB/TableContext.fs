namespace FSharp.DynamoDB

open System.Collections.Generic
open System.Net

open Microsoft.FSharp.Quotations

open Amazon.DynamoDBv2
open Amazon.DynamoDBv2.Model

open FSharp.DynamoDB.RecordSchema

type ConditionalCheckFailedException = Amazon.DynamoDBv2.Model.ConditionalCheckFailedException

type UpdateReturnValue =
    | Old = 1
    | New = 2

type TableContext<'TRecord> internal (client : IAmazonDynamoDB, tableName : string, record : RecordDescriptor<'TRecord>) =

    member __.Client = client
    member __.KeySchema = record.KeySchema
    member __.TableName = tableName

    member __.WithRecordType<'TRecord2>() =
        let rd = RecordDescriptor.Create<'TRecord2>()
        if record.KeySchema <> rd.KeySchema then
            invalidArg (string typeof<'TRecord2>) "incompatible key schema."
        
        new TableContext<'TRecord2>(client, tableName, rd)

    member __.ExtractKey(item : 'TRecord) =
        record.ExtractKey item

    member __.ExtractConditionalExpr(expr : Expr<'TRecord -> bool>) =
        record.ExtractConditional expr

    member __.ExtractRecExprUpdater(expr : Expr<'TRecord -> 'TRecord>) =
        record.ExtractRecExprUpdater expr

    member __.ExtractOpExprUpdater(expr : Expr<'TRecord -> UpdateOp>) =
        record.ExtractOpExprUpdater expr

    member __.PutItemAsync(item : 'TRecord, ?precondition : ConditionExpression<'TRecord>) : Async<TableKey> = async {
        let attrValues = record.ToAttributeValues(item)
        let request = new PutItemRequest(tableName, attrValues)
        request.ReturnValues <- ReturnValue.NONE
        match precondition with
        | Some pc ->
            let cond = pc.Conditional
            cond.WriteAttributesTo request.ExpressionAttributeNames 
            cond.WriteValuesTo request.ExpressionAttributeValues
            request.ConditionExpression <- cond.Expression
        | _ -> ()

        let! ct = Async.CancellationToken
        let! response = client.PutItemAsync(request, ct) |> Async.AwaitTaskCorrect
        if response.HttpStatusCode <> HttpStatusCode.OK then
            failwithf "PutItem request returned error %O" response.HttpStatusCode

        return record.ExtractKey item
    }

    member __.PutItemAsync(item : 'TRecord, precondition : Expr<'TRecord -> bool>) = async {
        return! __.PutItemAsync(item, __.ExtractConditionalExpr precondition)
    }

    member __.PutItemsAsync(items : seq<'TRecord>) : Async<TableKey[]> = async {
        let mkWriteRequest (item : 'TRecord) =
            let attrValues = record.ToAttributeValues(item)
            let pr = new PutRequest(attrValues)
            new WriteRequest(pr)

        let items = Seq.toArray items
        if items.Length > 25 then invalidArg "items" "item length must be less than or equal to 25."
        let writeRequests = items |> Seq.map mkWriteRequest |> rlist
        let pbr = new BatchWriteItemRequest()
        pbr.RequestItems.[tableName] <- writeRequests
        let! ct = Async.CancellationToken
        let! response = client.BatchWriteItemAsync(pbr, ct) |> Async.AwaitTaskCorrect
        if response.HttpStatusCode <> HttpStatusCode.OK then
            failwithf "PutItem request returned error %O" response.HttpStatusCode

        return items |> Array.map record.ExtractKey
    }

    member __.UpdateItemAsync(key : TableKey, updater : UpdateExpression<'TRecord>, 
                                ?returnValue : UpdateReturnValue, 
                                ?precondition : ConditionExpression<'TRecord>) : Async<'TRecord> = async {

        let kav = record.ToAttributeValues(key)
        let request = new UpdateItemRequest()
        request.Key <- kav
        request.TableName <- tableName
        request.UpdateExpression <- updater.Updater.Expression
        updater.Updater.WriteAttributesTo request.ExpressionAttributeNames
        updater.Updater.WriteValuesTo request.ExpressionAttributeValues
        request.ReturnValues <- 
            match defaultArg returnValue UpdateReturnValue.New with 
            | UpdateReturnValue.New -> ReturnValue.ALL_NEW
            | UpdateReturnValue.Old -> ReturnValue.ALL_OLD
            | _ -> invalidArg "returnValue" "invalid update return value."

        match precondition with
        | Some pc ->
            let cond = pc.Conditional
            cond.WriteAttributesTo request.ExpressionAttributeNames
            cond.WriteValuesTo request.ExpressionAttributeValues
            request.ConditionExpression <- cond.Expression
        | _ -> ()

        let! ct = Async.CancellationToken
        let! response = client.UpdateItemAsync(request, ct) |> Async.AwaitTaskCorrect
        if response.HttpStatusCode <> HttpStatusCode.OK then
            failwithf "PutItem request returned error %O" response.HttpStatusCode

        return record.OfAttributeValues response.Attributes
    }

    member __.UpdateItemRecExprAsync(key : TableKey, updateExpr : Expr<'TRecord -> 'TRecord>, 
                                        ?returnValue : UpdateReturnValue,
                                        ?precondition : Expr<'TRecord -> bool>) = async {
        let updater = __.ExtractRecExprUpdater updateExpr
        let precondition = precondition |> Option.map __.ExtractConditionalExpr
        return! __.UpdateItemAsync(key, updater, ?returnValue = returnValue, ?precondition = precondition)
    }

    member __.UpdateItemOpExprAsync(key : TableKey, updateExpr : Expr<'TRecord -> UpdateOp>, 
                                        ?returnValue : UpdateReturnValue,
                                        ?precondition : Expr<'TRecord -> bool>) = async {
        let updater = __.ExtractOpExprUpdater updateExpr
        let precondition = precondition |> Option.map __.ExtractConditionalExpr
        return! __.UpdateItemAsync(key, updater, ?returnValue = returnValue, ?precondition = precondition)
    }

    member __.ContainsKeyAsync(key : TableKey) = async {
        let kav = record.ToAttributeValues(key)
        let request = new GetItemRequest(tableName, kav)
        let! ct = Async.CancellationToken
        let! response = client.GetItemAsync(request, ct) |> Async.AwaitTaskCorrect
        return response.IsItemSet
    }

    member __.GetItemAsync(key : TableKey) : Async<'TRecord> = async {
        let kav = record.ToAttributeValues(key)
        let request = new GetItemRequest(tableName, kav)
        let! ct = Async.CancellationToken
        let! response = client.GetItemAsync(request, ct) |> Async.AwaitTaskCorrect
        if response.HttpStatusCode <> HttpStatusCode.OK then
            failwithf "GetItem request returned error %O" response.HttpStatusCode
        elif not response.IsItemSet then
            failwithf "Could not find item %O" key

        return record.OfAttributeValues response.Item
    }

    member __.GetItemsAsync(keys : seq<TableKey>, ?consistentRead : bool) : Async<'TRecord[]> = async {
        let consistentRead = defaultArg consistentRead false
        let kna = new KeysAndAttributes()
        kna.AttributesToGet.AddRange(record.Info.Properties |> Seq.map (fun p -> p.Name))
        kna.Keys.AddRange(keys |> Seq.map record.ToAttributeValues)
        kna.ConsistentRead <- consistentRead
        let request = new BatchGetItemRequest()
        request.RequestItems.[tableName] <- kna

        let! ct = Async.CancellationToken
        let! response = client.BatchGetItemAsync(request, ct) |> Async.AwaitTaskCorrect
        if response.HttpStatusCode <> HttpStatusCode.OK then
            failwithf "GetItem request returned error %O" response.HttpStatusCode
        
        return response.Responses.[tableName] |> Seq.map record.OfAttributeValues |> Seq.toArray
    }

    member __.DeleteItemAsync(key : TableKey) : Async<unit> = async {
        let kav = record.ToAttributeValues key
        let request = new DeleteItemRequest(tableName, kav)
        request.ReturnValues <- ReturnValue.NONE
        let! ct = Async.CancellationToken
        let! response = client.DeleteItemAsync(request, ct) |> Async.AwaitTaskCorrect
        if response.HttpStatusCode <> HttpStatusCode.OK then
            failwithf "DeleteItem request returned error %O" response.HttpStatusCode
    }

    member __.DeleteItemsAsync(keys : seq<TableKey>) = async {
        let mkDeleteRequest (key : TableKey) =
            let kav = record.ToAttributeValues(key)
            let pr = new DeleteRequest(kav)
            new WriteRequest(pr)

        let keys = Seq.toArray keys
        if keys.Length > 25 then invalidArg "items" "key length must be less than or equal to 25."
        let request = new BatchWriteItemRequest()
        let deleteRequests = keys |> Seq.map mkDeleteRequest |> rlist
        request.RequestItems.[tableName] <- deleteRequests

        let! ct = Async.CancellationToken
        let! response = client.BatchWriteItemAsync(request, ct) |> Async.AwaitTaskCorrect
        if response.HttpStatusCode <> HttpStatusCode.OK then
            failwithf "PutItem request returned error %O" response.HttpStatusCode    
    }

    member __.QueryAsync(keyCondition : ConditionExpression<'TRecord>, ?filterCondition : ConditionExpression<'TRecord>, 
                            ?limit: int, ?consistentRead : bool, ?scanIndexForward : bool) : Async<'TRecord []> = async {

        if not keyCondition.IsQueryCompatible then 
            invalidArg "keyCondition" 
                """key conditions must satisfy the following constraints:
* Must only reference HashKey & RangeKey attributes.
* Must reference HashKey attribute exactly once.
* Must reference RangeKey attribute at most once.
* HashKey comparison must be equality comparison only.
* Must not contain OR and NOT clauses.
* Must not contain nested operands.              
"""

        let downloaded = new ResizeArray<_>()
        let rec aux last = async {
            let request = new QueryRequest(tableName)
            let cond = keyCondition.Conditional
            request.KeyConditionExpression <- cond.Expression
            cond.WriteAttributesTo request.ExpressionAttributeNames
            cond.WriteValuesTo request.ExpressionAttributeValues

            match filterCondition with
            | Some fc ->
                let cond = fc.Conditional
                request.FilterExpression <- 
                    cond.BuildAppendedConditional(request.ExpressionAttributeNames, 
                                                    request.ExpressionAttributeValues)
            | None -> ()

            limit |> Option.iter (fun l -> request.Limit <- l - downloaded.Count)
            consistentRead |> Option.iter (fun cr -> request.ConsistentRead <- cr)
            scanIndexForward |> Option.iter (fun sif -> request.ScanIndexForward <- sif)
            last |> Option.iter (fun l -> request.ExclusiveStartKey <- l)

            let! ct = Async.CancellationToken
            let! response = client.QueryAsync(request, ct) |> Async.AwaitTaskCorrect
            if response.HttpStatusCode <> HttpStatusCode.OK then
                failwithf "Query request returned error %O" response.HttpStatusCode
                
            downloaded.AddRange response.Items
            if response.LastEvaluatedKey.Count > 0 &&
               limit |> Option.forall (fun l -> downloaded.Count < l)
            then 
                do! aux (Some response.LastEvaluatedKey)
        } 

        do! aux None
        
        return downloaded |> Seq.map record.OfAttributeValues |> Seq.toArray
    }

    member __.QueryAsync(keyCondition : Expr<'TRecord -> bool>, ?filterCondition : Expr<'TRecord -> bool>, 
                            ?limit : int, ?consistentRead : bool, ?scanIndexForward : bool) : Async<'TRecord []> = async {

        let kc = __.ExtractConditionalExpr keyCondition
        let fc = filterCondition |> Option.map __.ExtractConditionalExpr 
        return! __.QueryAsync(kc, ?filterCondition = fc, ?limit = limit, ?consistentRead = consistentRead, ?scanIndexForward = scanIndexForward)
    }

    member __.ScanAsync(filterCondition : ConditionExpression<'TRecord>, ?limit : int, ?consistentRead : bool) : Async<'TRecord []> = async {

        let downloaded = new ResizeArray<_>()
        let rec aux last = async {
            let request = new ScanRequest(tableName)
            let cond = filterCondition.Conditional
            request.FilterExpression <- cond.Expression
            cond.WriteAttributesTo request.ExpressionAttributeNames
            cond.WriteValuesTo request.ExpressionAttributeValues

            limit |> Option.iter (fun l -> request.Limit <- l - downloaded.Count)
            consistentRead |> Option.iter (fun cr -> request.ConsistentRead <- cr)
            last |> Option.iter (fun l -> request.ExclusiveStartKey <- l)

            let! ct = Async.CancellationToken
            let! response = client.ScanAsync(request, ct) |> Async.AwaitTaskCorrect
            if response.HttpStatusCode <> HttpStatusCode.OK then
                failwithf "Query request returned error %O" response.HttpStatusCode
                
            downloaded.AddRange response.Items
            if response.LastEvaluatedKey.Count > 0 &&
                limit |> Option.forall (fun l -> downloaded.Count < l)
            then 
                do! aux (Some response.LastEvaluatedKey)
        } 

        do! aux None
        
        return downloaded |> Seq.map record.OfAttributeValues |> Seq.toArray
    }

    member __.ScanAsync(filterExpr : Expr<'TRecord -> bool>, ?limit : int, ?consistentRead : bool) : Async<'TRecord []> = async {
        let cond = __.ExtractConditionalExpr filterExpr
        return! __.ScanAsync(cond, ?limit = limit, ?consistentRead = consistentRead)
    }


type TableContext =

    static member GetTableContext<'TRecord>(client : IAmazonDynamoDB, tableName : string, ?createIfNotExists : bool, ?provisionedThroughPut : ProvisionedThroughput) = async {
        let createIfNotExists = defaultArg createIfNotExists false
        let rd = RecordDescriptor.Create<'TRecord> ()
        try
            let dtr = new DescribeTableRequest(tableName)
            let! ct = Async.CancellationToken
            let! response = client.DescribeTableAsync(dtr, ct) |> Async.AwaitTaskCorrect
            let existingSchema = TableKeySchema.OfTableDescription response.Table
            if existingSchema <> rd.KeySchema then 
                sprintf "table '%s' contains incompatible key schema %A, which is incompatible with record '%O'." 
                    tableName existingSchema typeof<'TRecord>
                |> invalidOp

        with :? ResourceNotFoundException when createIfNotExists ->
            let provisionedThroughPut = defaultArg provisionedThroughPut (new ProvisionedThroughput(10L,10L))
            let ctr = rd.KeySchema.CreateCreateTableRequest (tableName, provisionedThroughPut)
            let! ct = Async.CancellationToken
            let! response = client.CreateTableAsync(ctr, ct) |> Async.AwaitTaskCorrect
            if response.HttpStatusCode <> HttpStatusCode.OK then
                failwithf "CreateTable request returned error %O" response.HttpStatusCode

            let rec awaitReady retries = async {
                if retries = 0 then return failwithf "Failed to create table '%s'" tableName
                let! descr = client.DescribeTableAsync(tableName, ct) |> Async.AwaitTaskCorrect
                if descr.Table.TableStatus <> TableStatus.ACTIVE then
                    do! Async.Sleep 1000
                    return! awaitReady (retries - 1)
            }

            do! awaitReady 20

        return new TableContext<'TRecord>(client, tableName, rd)
    }


[<AutoOpen>]
module TableContextUtils =
    
    let inline cond (ctx : TableContext<'TRecord>) expr = ctx.ExtractConditionalExpr expr
    let inline updateOp (ctx : TableContext<'TRecord>) expr = ctx.ExtractOpExprUpdater expr
    let inline updateRec (ctx : TableContext<'TRecord>) expr = ctx.ExtractRecExprUpdater expr