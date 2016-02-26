namespace FSharp.DynamoDB

open System.Collections.Generic
open System.Net

open Microsoft.FSharp.Quotations

open Amazon.DynamoDBv2
open Amazon.DynamoDBv2.Model

open FSharp.DynamoDB.KeySchema
open FSharp.DynamoDB.ExprCommon

/// Exception raised by DynamoDB in case where write preconditions are not satisfied
type ConditionalCheckFailedException = Amazon.DynamoDBv2.Model.ConditionalCheckFailedException

/// DynamoDB client object for performing table operations 
/// in the context of given F# record representationss
[<Sealed; AutoSerializable(false)>]
type TableContext<'TRecord> internal (client : IAmazonDynamoDB, tableName : string, template : RecordTemplate<'TRecord>) =

    /// DynamoDB client instance used for the table operations
    member __.Client = client
    /// DynamoDB table name targeted by the context
    member __.TableName = tableName
    /// Key schema used by the current record/table
    member __.KeySchema = template.KeySchema
    /// Record-induced table template
    member __.Template = template

    /// Creates a new table context instance that uses
    /// a new F# record type. The new F# record type
    /// must define a compatible key schema.
    member __.WithRecordType<'TRecord2>() : TableContext<'TRecord2> =
        let rd = RecordTemplate.Define<'TRecord2>()
        if template.KeySchema <> rd.KeySchema then
            invalidArg (string typeof<'TRecord2>) "incompatible key schema."
        
        new TableContext<'TRecord2>(client, tableName, rd)

    /// <summary>
    ///     Asynchronously puts a record item in the table.
    /// </summary>
    /// <param name="item">Item to be written.</param>
    /// <param name="precondition">Precondition to satisfy in case item already exists.</param>
    member __.PutItemAsync(item : 'TRecord, ?precondition : ConditionExpression<'TRecord>) : Async<TableKey> = async {
        let attrValues = template.ToAttributeValues(item)
        let request = new PutItemRequest(tableName, attrValues)
        request.ReturnValues <- ReturnValue.NONE
        match precondition with
        | Some pc ->
            let writer = new AttributeWriter(request.ExpressionAttributeNames, request.ExpressionAttributeValues)
            request.ConditionExpression <- pc.Conditional.Write writer
        | _ -> ()

        let! ct = Async.CancellationToken
        let! response = client.PutItemAsync(request, ct) |> Async.AwaitTaskCorrect
        if response.HttpStatusCode <> HttpStatusCode.OK then
            failwithf "PutItem request returned error %O" response.HttpStatusCode

        return template.ExtractKey item
    }

    /// <summary>
    ///     Asynchronously puts a record item in the table.
    /// </summary>
    /// <param name="item">Item to be written.</param>
    /// <param name="precondition">Precondition to satisfy in case item already exists.</param>
    member __.PutItemAsync(item : 'TRecord, precondition : Expr<'TRecord -> bool>) = async {
        return! __.PutItemAsync(item, template.PrecomputeConditionalExpr precondition)
    }

    /// <summary>
    ///     Puts a record item in the table.
    /// </summary>
    /// <param name="item">Item to be written.</param>
    /// <param name="precondition">Precondition to satisfy in case item already exists.</param>
    member __.PutItem(item : 'TRecord, ?precondition : ConditionExpression<'TRecord>) =
        __.PutItemAsync(item, ?precondition = precondition) |> Async.RunSynchronously

    /// <summary>
    ///     Puts a record item in the table.
    /// </summary>
    /// <param name="item">Item to be written.</param>
    /// <param name="precondition">Precondition to satisfy in case item already exists.</param>
    member __.PutItem(item : 'TRecord, precondition : Expr<'TRecord -> bool>) =
        __.PutItemAsync(item, precondition) |> Async.RunSynchronously

    /// <summary>
    ///     Asynchronously puts a collection of items to the table as a batch write operation.
    ///     At most 25 items can be written in a single batch write operation.
    /// </summary>
    /// <param name="items">Items to be written.</param>
    member __.BatchPutItemsAsync(items : seq<'TRecord>) : Async<TableKey[]> = async {
        let mkWriteRequest (item : 'TRecord) =
            let attrValues = template.ToAttributeValues(item)
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

        return items |> Array.map template.ExtractKey
    }

    /// <summary>
    ///     Puts a collection of items to the table as a batch write operation.
    ///     At most 25 items can be written in a single batch write operation.
    /// </summary>
    /// <param name="items">Items to be written.</param>
    member __.BatchPutItems(items : seq<'TRecord>) =
        __.BatchPutItemsAsync(items) |> Async.RunSynchronously

    /// <summary>
    ///     Asynchronously updates item with supplied key using provided update expression.
    /// </summary>
    /// <param name="key">Key of item to be updated.</param>
    /// <param name="updater">Table update expression.</param>
    /// <param name="precondition">Specifies a precondition expression that existing item should satisfy.</param>
    /// <param name="returnLatest">Specifies the operation should return the latest (true) or older (false) version of the item. Defaults to latest.</param>
    member __.UpdateItemAsync(key : TableKey, updater : UpdateExpression<'TRecord>, 
                                ?precondition : ConditionExpression<'TRecord>, ?returnLatest : bool) : Async<'TRecord> = async {

        let kav = template.ToAttributeValues(key)
        let request = new UpdateItemRequest(Key = kav, TableName = tableName)
        request.ReturnValues <- 
            if defaultArg returnLatest true then ReturnValue.ALL_NEW
            else ReturnValue.ALL_OLD

        let writer = new AttributeWriter(request.ExpressionAttributeNames, request.ExpressionAttributeValues)
        request.UpdateExpression <- updater.UpdateOps.Write(writer)

        match precondition with
        | Some pc -> request.ConditionExpression <- pc.Conditional.Write writer
        | _ -> ()

        let! ct = Async.CancellationToken
        let! response = client.UpdateItemAsync(request, ct) |> Async.AwaitTaskCorrect
        if response.HttpStatusCode <> HttpStatusCode.OK then
            failwithf "PutItem request returned error %O" response.HttpStatusCode

        return template.OfAttributeValues response.Attributes
    }

    /// <summary>
    ///     Asynchronously updates item with supplied key using provided record update expression.
    /// </summary>
    /// <param name="key">Key of item to be updated.</param>
    /// <param name="updater">Table update expression.</param>
    /// <param name="precondition">Specifies a precondition expression that existing item should satisfy.</param>
    /// <param name="returnLatest">Specifies the operation should return the latest (true) or older (false) version of the item. Defaults to latest.</param>
    member __.UpdateItemAsync(key : TableKey, updateExpr : Expr<'TRecord -> 'TRecord>, 
                                ?precondition : Expr<'TRecord -> bool>, ?returnLatest : bool) = async {
        let updater = template.PrecomputeUpdateExpr updateExpr
        let precondition = precondition |> Option.map template.PrecomputeConditionalExpr
        return! __.UpdateItemAsync(key, updater, ?returnLatest = returnLatest, ?precondition = precondition)
    }

    /// <summary>
    ///     Asynchronously updates item with supplied key using provided update operation expression.
    /// </summary>
    /// <param name="key">Key of item to be updated.</param>
    /// <param name="updater">Table update expression.</param>
    /// <param name="precondition">Specifies a precondition expression that existing item should satisfy.</param>
    /// <param name="returnLatest">Specifies the operation should return the latest (true) or older (false) version of the item. Defaults to latest.</param>
    member __.UpdateItemAsync(key : TableKey, updateExpr : Expr<'TRecord -> UpdateOp>, 
                                ?precondition : Expr<'TRecord -> bool>, ?returnLatest : bool) = async {

        let updater = template.PrecomputeUpdateExpr updateExpr
        let precondition = precondition |> Option.map template.PrecomputeConditionalExpr
        return! __.UpdateItemAsync(key, updater, ?returnLatest = returnLatest, ?precondition = precondition)
    }

    /// <summary>
    ///     Updates item with supplied key using provided update expression.
    /// </summary>
    /// <param name="key">Key of item to be updated.</param>
    /// <param name="updater">Table update expression.</param>
    /// <param name="precondition">Specifies a precondition expression that existing item should satisfy.</param>
    /// <param name="returnLatest">Specifies the operation should return the latest (true) or older (false) version of the item. Defaults to latest.</param>
    member __.UpdateItem(key : TableKey, updater : UpdateExpression<'TRecord>, 
                                ?precondition : ConditionExpression<'TRecord>, ?returnLatest : bool) =
        __.UpdateItemAsync(key, updater, ?precondition = precondition, ?returnLatest = returnLatest)
        |> Async.RunSynchronously

    /// <summary>
    ///     Updates item with supplied key using provided record update expression.
    /// </summary>
    /// <param name="key">Key of item to be updated.</param>
    /// <param name="updater">Table update expression.</param>
    /// <param name="precondition">Specifies a precondition expression that existing item should satisfy.</param>
    /// <param name="returnLatest">Specifies the operation should return the latest (true) or older (false) version of the item. Defaults to latest.</param>
    member __.UpdateItem(key : TableKey, updater : Expr<'TRecord -> 'TRecord>, 
                                ?precondition : Expr<'TRecord -> bool>, ?returnLatest : bool) =
        __.UpdateItemAsync(key, updater, ?precondition = precondition, ?returnLatest = returnLatest)
        |> Async.RunSynchronously

    /// <summary>
    ///     Updates item with supplied key using provided record update operation.
    /// </summary>
    /// <param name="key">Key of item to be updated.</param>
    /// <param name="updater">Table update expression.</param>
    /// <param name="precondition">Specifies a precondition expression that existing item should satisfy.</param>
    /// <param name="returnLatest">Specifies the operation should return the latest (true) or older (false) version of the item. Defaults to latest.</param>
    member __.UpdateItem(key : TableKey, updater : Expr<'TRecord -> UpdateOp>, 
                                ?precondition : Expr<'TRecord -> bool>, ?returnLatest : bool) =
        __.UpdateItemAsync(key, updater, ?precondition = precondition, ?returnLatest = returnLatest)
        |> Async.RunSynchronously

    /// <summary>
    ///     Asynchronously checks whether item of supplied key exists in table.
    /// </summary>
    /// <param name="key">Key to be checked.</param>
    member __.ContainsKeyAsync(key : TableKey) : Async<bool> = async {
        let kav = template.ToAttributeValues(key)
        let request = new GetItemRequest(tableName, kav)
        let! ct = Async.CancellationToken
        let! response = client.GetItemAsync(request, ct) |> Async.AwaitTaskCorrect
        return response.IsItemSet
    }

    /// <summary>
    ///     Checks whether item of supplied key exists in table.
    /// </summary>
    /// <param name="key">Key to be checked.</param>
    member __.ContainsKey(key : TableKey) = 
        __.ContainsKeyAsync(key) |> Async.RunSynchronously

    /// <summary>
    ///     Asynchronously fetches item of given key from table.
    /// </summary>
    /// <param name="key">Key of item to be fetched.</param>
    member __.GetItemAsync(key : TableKey) : Async<'TRecord> = async {
        let kav = template.ToAttributeValues(key)
        let request = new GetItemRequest(tableName, kav)
        let! ct = Async.CancellationToken
        let! response = client.GetItemAsync(request, ct) |> Async.AwaitTaskCorrect
        if response.HttpStatusCode <> HttpStatusCode.OK then
            failwithf "GetItem request returned error %O" response.HttpStatusCode
        elif not response.IsItemSet then
            failwithf "Could not find item %O" key

        return template.OfAttributeValues response.Item
    }

    /// <summary>
    ///     Asynchronously fetches item of given key from table.
    /// </summary>
    /// <param name="key">Key of item to be fetched.</param>
    member __.GetItem(key : TableKey) = __.GetItemAsync(key) |> Async.RunSynchronously

    /// <summary>
    ///     Asynchronously performs a batch fetch of items with supplied keys.
    /// </summary>
    /// <param name="keys">Keys of items to be fetched.</param>
    /// <param name="consistentRead">Perform consistent read. Defaults to false.</param>
    member __.BatchGetItemsAsync(keys : seq<TableKey>, ?consistentRead : bool) : Async<'TRecord[]> = async {
        let consistentRead = defaultArg consistentRead false
        let kna = new KeysAndAttributes()
        kna.AttributesToGet.AddRange(template.Info.Properties |> Seq.map (fun p -> p.Name))
        kna.Keys.AddRange(keys |> Seq.map template.ToAttributeValues)
        kna.ConsistentRead <- consistentRead
        let request = new BatchGetItemRequest()
        request.RequestItems.[tableName] <- kna

        let! ct = Async.CancellationToken
        let! response = client.BatchGetItemAsync(request, ct) |> Async.AwaitTaskCorrect
        if response.HttpStatusCode <> HttpStatusCode.OK then
            failwithf "GetItem request returned error %O" response.HttpStatusCode
        
        return response.Responses.[tableName] |> Seq.map template.OfAttributeValues |> Seq.toArray
    }

    /// <summary>
    ///     Performs a batch fetch of items with supplied keys.
    /// </summary>
    /// <param name="keys">Keys of items to be fetched.</param>
    /// <param name="consistentRead">Perform consistent read. Defaults to false.</param>
    member __.BatchGetItems(keys : seq<TableKey>, ?consistentRead : bool) =
        __.BatchGetItemsAsync(keys, ?consistentRead = consistentRead)
        |> Async.RunSynchronously

    /// <summary>
    ///     Asynchronously deletes item of given key from table.
    /// </summary>
    /// <param name="key">Key of item to be deleted.</param>
    member __.DeleteItemAsync(key : TableKey) : Async<unit> = async {
        let kav = template.ToAttributeValues key
        let request = new DeleteItemRequest(tableName, kav)
        request.ReturnValues <- ReturnValue.NONE
        let! ct = Async.CancellationToken
        let! response = client.DeleteItemAsync(request, ct) |> Async.AwaitTaskCorrect
        if response.HttpStatusCode <> HttpStatusCode.OK then
            failwithf "DeleteItem request returned error %O" response.HttpStatusCode
    }

    /// <summary>
    ///     Deletes item of given key from table.
    /// </summary>
    /// <param name="key">Key of item to be deleted.</param>
    member __.DeleteItem(key : TableKey) =
        __.DeleteItemAsync(key) |> Async.RunSynchronously

    /// <summary>
    ///     Asynchronously performs batch delete operation on items of given keys.
    /// </summary>
    /// <param name="keys">Keys of items to be deleted.</param>
    member __.BatchDeleteItemsAsync(keys : seq<TableKey>) = async {
        let mkDeleteRequest (key : TableKey) =
            let kav = template.ToAttributeValues(key)
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

    /// <summary>
    ///     Performs batch delete operation on items of given keys.
    /// </summary>
    /// <param name="keys">Keys of items to be deleted.</param>
    member __.BatchDeleteItems(keys : seq<TableKey>) = 
        __.BatchDeleteItemsAsync(keys) |> Async.RunSynchronously

    /// <summary>
    ///     Asynchronously queries table with given condition expressions.
    /// </summary>
    /// <param name="keyCondition">Key condition expression.</param>
    /// <param name="filterCondition">Filter condition expression.</param>
    /// <param name="limit">Maximum number of items to evaluate.</param>
    /// <param name="consistentRead">Specify whether to perform consistent read operation.</param>
    /// <param name="scanIndexForward">Specifies the order in which to evaluate results. Either ascending (true) or descending (false).</param>
    member __.QueryAsync(keyCondition : ConditionExpression<'TRecord>, ?filterCondition : ConditionExpression<'TRecord>, 
                            ?limit: int, ?consistentRead : bool, ?scanIndexForward : bool) : Async<'TRecord []> = async {

        if not keyCondition.IsKeyConditionCompatible then 
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
            let writer = new AttributeWriter(request.ExpressionAttributeNames, request.ExpressionAttributeValues)
            request.KeyConditionExpression <- cond.Write writer

            match filterCondition with
            | Some fc ->
                let cond = fc.Conditional
                request.FilterExpression <- cond.Write writer

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
        
        return downloaded |> Seq.map template.OfAttributeValues |> Seq.toArray
    }

    /// <summary>
    ///     Asynchronously queries table with given condition expressions.
    /// </summary>
    /// <param name="keyCondition">Key condition expression.</param>
    /// <param name="filterCondition">Filter condition expression.</param>
    /// <param name="limit">Maximum number of items to evaluate.</param>
    /// <param name="consistentRead">Specify whether to perform consistent read operation.</param>
    /// <param name="scanIndexForward">Specifies the order in which to evaluate results. Either ascending (true) or descending (false).</param>
    member __.QueryAsync(keyCondition : Expr<'TRecord -> bool>, ?filterCondition : Expr<'TRecord -> bool>, 
                            ?limit : int, ?consistentRead : bool, ?scanIndexForward : bool) : Async<'TRecord []> = async {

        let kc = template.PrecomputeConditionalExpr keyCondition
        let fc = filterCondition |> Option.map template.PrecomputeConditionalExpr
        return! __.QueryAsync(kc, ?filterCondition = fc, ?limit = limit, ?consistentRead = consistentRead, ?scanIndexForward = scanIndexForward)
    }

    /// <summary>
    ///     Queries table with given condition expressions.
    /// </summary>
    /// <param name="keyCondition">Key condition expression.</param>
    /// <param name="filterCondition">Filter condition expression.</param>
    /// <param name="limit">Maximum number of items to evaluate.</param>
    /// <param name="consistentRead">Specify whether to perform consistent read operation.</param>
    /// <param name="scanIndexForward">Specifies the order in which to evaluate results. Either ascending (true) or descending (false).</param>
    member __.Query(keyCondition : ConditionExpression<'TRecord>, ?filterCondition : ConditionExpression<'TRecord>, 
                            ?limit: int, ?consistentRead : bool, ?scanIndexForward : bool) : 'TRecord[] =
        __.QueryAsync(keyCondition, ?filterCondition = filterCondition, ?limit = limit, 
                        ?consistentRead = consistentRead, ?scanIndexForward = scanIndexForward)
        |> Async.RunSynchronously

    /// <summary>
    ///     Queries table with given condition expressions.
    /// </summary>
    /// <param name="keyCondition">Key condition expression.</param>
    /// <param name="filterCondition">Filter condition expression.</param>
    /// <param name="limit">Maximum number of items to evaluate.</param>
    /// <param name="consistentRead">Specify whether to perform consistent read operation.</param>
    /// <param name="scanIndexForward">Specifies the order in which to evaluate results. Either ascending (true) or descending (false).</param>
    member __.Query(keyCondition : Expr<'TRecord -> bool>, ?filterCondition : Expr<'TRecord -> bool>, 
                            ?limit: int, ?consistentRead : bool, ?scanIndexForward : bool) : 'TRecord[] =
        __.QueryAsync(keyCondition, ?filterCondition = filterCondition, ?limit = limit, 
                        ?consistentRead = consistentRead, ?scanIndexForward = scanIndexForward)
        |> Async.RunSynchronously

    /// <summary>
    ///     Asynchronously scans table with given condition expressions.
    /// </summary>
    /// <param name="filterCondition">Filter condition expression.</param>
    /// <param name="limit">Maximum number of items to evaluate.</param>
    /// <param name="consistentRead">Specify whether to perform consistent read operation.</param>
    member __.ScanAsync(filterCondition : ConditionExpression<'TRecord>, ?limit : int, ?consistentRead : bool) : Async<'TRecord []> = async {

        let downloaded = new ResizeArray<_>()
        let rec aux last = async {
            let request = new ScanRequest(tableName)
            let writer = new AttributeWriter(request.ExpressionAttributeNames, request.ExpressionAttributeValues)
            request.FilterExpression <- filterCondition.Conditional.Write writer

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
        
        return downloaded |> Seq.map template.OfAttributeValues |> Seq.toArray
    }

    /// <summary>
    ///     Asynchronously scans table with given condition expressions.
    /// </summary>
    /// <param name="filterCondition">Filter condition expression.</param>
    /// <param name="limit">Maximum number of items to evaluate.</param>
    /// <param name="consistentRead">Specify whether to perform consistent read operation.</param>
    member __.ScanAsync(filterExpr : Expr<'TRecord -> bool>, ?limit : int, ?consistentRead : bool) : Async<'TRecord []> = async {
        let cond = template.PrecomputeConditionalExpr filterExpr
        return! __.ScanAsync(cond, ?limit = limit, ?consistentRead = consistentRead)
    }

    /// <summary>
    ///     Scans table with given condition expressions.
    /// </summary>
    /// <param name="filterCondition">Filter condition expression.</param>
    /// <param name="limit">Maximum number of items to evaluate.</param>
    /// <param name="consistentRead">Specify whether to perform consistent read operation.</param>
    member __.Scan(filterCondition : ConditionExpression<'TRecord>, ?limit : int, ?consistentRead : bool) : 'TRecord [] =
        __.ScanAsync(filterCondition, ?limit = limit, ?consistentRead = consistentRead)
        |> Async.RunSynchronously

    /// <summary>
    ///     Scans table with given condition expressions.
    /// </summary>
    /// <param name="filterCondition">Filter condition expression.</param>
    /// <param name="limit">Maximum number of items to evaluate.</param>
    /// <param name="consistentRead">Specify whether to perform consistent read operation.</param>
    member __.Scan(filterCondition : Expr<'TRecord -> bool>, ?limit : int, ?consistentRead : bool) : 'TRecord [] =
        __.ScanAsync(filterCondition, ?limit = limit, ?consistentRead = consistentRead)
        |> Async.RunSynchronously


    /// <summary>
    ///     Asynchronously verify that the table exists and is compatible with record key schema.
    /// </summary>
    /// <param name="createIfNotExists">Create the table instance now instance if it does not exist. Defaults to false.</param>
    /// <param name="provisionedThroughput">Provisioned throughput for the table if newly created.</param>
    member __.VerifyTableAsync(?createIfNotExists : bool, ?provisionedThroughput : ProvisionedThroughput) : Async<unit> = async {
        let createIfNotExists = defaultArg createIfNotExists false
        try
            let dtr = new DescribeTableRequest(tableName)
            let! ct = Async.CancellationToken
            let! response = client.DescribeTableAsync(dtr, ct) |> Async.AwaitTaskCorrect
            let existingSchema = TableKeySchema.OfTableDescription response.Table
            if existingSchema <> template.KeySchema then 
                sprintf "table '%s' exists with key schema %A, which is incompatible with record '%O'." 
                    tableName existingSchema typeof<'TRecord>
                |> invalidOp

        with :? ResourceNotFoundException when createIfNotExists ->
            let provisionedThroughput = defaultArg provisionedThroughput (new ProvisionedThroughput(10L,10L))
            let ctr = template.KeySchema.CreateCreateTableRequest (tableName, provisionedThroughput)
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

            do! awaitReady 30
    }

    /// <summary>
    ///     Asynchronously verify that the table exists and is compatible with record key schema.
    /// </summary>
    /// <param name="createIfNotExists">Create the table instance now if it does not exist. Defaults to false.</param>
    /// <param name="provisionedThroughput">Provisioned throughput for the table if newly created.</param>
    member __.VerifyTable(?createIfNotExists : bool, ?provisionedThroughput : ProvisionedThroughput) =
        __.VerifyTableAsync(?provisionedThroughput = provisionedThroughput)
        |> Async.RunSynchronously

/// Table context factory methods
type TableContext =

    /// <summary>
    ///     Creates a DynamoDB client instance for given F# record and table name.
    /// </summary>
    /// <param name="client">DynamoDB client instance.</param>
    /// <param name="tableName">Table name to target.</param>
    /// <param name="verifyTable">Verify that the table exists and is compatible with supplied record schema. Defaults to true.</param>
    /// <param name="createIfNotExists">Create the table now instance if it does not exist. Defaults to false.</param>
    static member CreateAsync<'TRecord>(client : IAmazonDynamoDB, tableName : string, ?verifyTable : bool, ?createIfNotExists : bool) = async {
        if not <| isValidTableName tableName then invalidArg tableName "unsupported DynamoDB table name."
        let verifyTable = defaultArg verifyTable true
        let createIfNotExists = defaultArg createIfNotExists false
        let context = new TableContext<'TRecord>(client, tableName, RecordTemplate.Define<'TRecord>())
        if verifyTable || createIfNotExists then
            do! context.VerifyTableAsync(createIfNotExists = createIfNotExists)

        return context
    }

    /// <summary>
    ///     Creates a DynamoDB client instance for given F# record and table name.
    /// </summary>
    /// <param name="client">DynamoDB client instance.</param>
    /// <param name="tableName">Table name to target.</param>
    /// <param name="verifyTable">Verify that the table exists and is compatible with supplied record schema. Defaults to true.</param>
    /// <param name="createIfNotExists">Create the table now instance if it does not exist. Defaults to false.</param>
    static member Create<'TRecord>(client : IAmazonDynamoDB, tableName : string, ?verifyTable : bool, ?createIfNotExists : bool) =
        TableContext.CreateAsync<'TRecord>(client, tableName, ?verifyTable = verifyTable, ?createIfNotExists = createIfNotExists)
        |> Async.RunSynchronously


[<AutoOpen>]
module TableContextUtils =

    let template<'TRecord> = RecordTemplate.Define<'TRecord>()
    
    /// Precomputes a conditional expression
    let inline cond (tmp : RecordTemplate<'TRecord>) expr = tmp.PrecomputeConditionalExpr expr
    /// Precomputes an update expression
    let inline updateOp (tmp : RecordTemplate<'TRecord>) (expr : Expr<'TRecord -> UpdateOp>) = tmp.PrecomputeUpdateExpr expr
    /// Precomputes an update expression
    let inline updateRec (tmp : RecordTemplate<'TRecord>) (expr : Expr<'TRecord -> 'TRecord>) = tmp.PrecomputeUpdateExpr expr