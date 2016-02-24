namespace FSharp.DynamoDB

open System.Collections.Generic
open System.Net

open Microsoft.FSharp.Quotations

open Amazon.DynamoDBv2
open Amazon.DynamoDBv2.Model

open FSharp.DynamoDB.RecordSchema

/// Exception raised by DynamoDB in case where write preconditions are not satisfied
type ConditionalCheckFailedException = Amazon.DynamoDBv2.Model.ConditionalCheckFailedException

/// DynamoDB client object for performing table operations 
/// in the context of given F# record representationss
type TableContext<'TRecord> internal (client : IAmazonDynamoDB, tableName : string, record : RecordDescriptor<'TRecord>) =

    /// DynamoDB client instance used for the table operations
    member __.Client = client
    /// DynamoDB table name targeted by the context
    member __.TableName = tableName
    /// Key schema used by the current record/table
    member __.KeySchema = record.KeySchema

    /// Creates a new table context instance that uses
    /// a new F# record type. The new F# record type
    /// must define a compatible key schema.
    member __.WithRecordType<'TRecord2>() : TableContext<'TRecord2> =
        let rd = RecordDescriptor.Create<'TRecord2>()
        if record.KeySchema <> rd.KeySchema then
            invalidArg (string typeof<'TRecord2>) "incompatible key schema."
        
        new TableContext<'TRecord2>(client, tableName, rd)

    /// <summary>
    ///     Extracts the key that corresponds to supplied record instance.
    /// </summary>
    /// <param name="item">Input record instance.</param>
    member __.ExtractKey(item : 'TRecord) : TableKey =
        record.ExtractKey item

    /// <summary>
    ///     Precomputes a DynamoDB conditional expression using
    ///     supplied quoted record predicate.
    /// </summary>
    /// <param name="expr">Quoted record predicate.</param>
    member __.PrecomputeConditionalExpr(expr : Expr<'TRecord -> bool>) =
        record.ExtractConditional expr

    /// <summary>
    ///     Precomputes a DynamoDB update expression using
    ///     supplied quoted record update expression.
    /// </summary>
    /// <param name="expr">Quoted record update expression.</param>
    member __.PrecomputeUpdateExpr(expr : Expr<'TRecord -> 'TRecord>) =
        record.ExtractRecExprUpdater expr

    /// <summary>
    ///     Precomputes a DynamoDB update expression using
    ///     supplied quoted record update operations.
    /// </summary>
    /// <param name="expr">Quoted record update operations.</param>
    member __.PrecomputeUpdateExpr(expr : Expr<'TRecord -> UpdateOp>) =
        record.ExtractOpExprUpdater expr

    /// <summary>
    ///     Asynchronously puts a record item in the table.
    /// </summary>
    /// <param name="item">Item to be written.</param>
    /// <param name="precondition">Precondition to satisfy in case item already exists.</param>
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

    /// <summary>
    ///     Asynchronously puts a record item in the table.
    /// </summary>
    /// <param name="item">Item to be written.</param>
    /// <param name="precondition">Precondition to satisfy in case item already exists.</param>
    member __.PutItemAsync(item : 'TRecord, precondition : Expr<'TRecord -> bool>) = async {
        return! __.PutItemAsync(item, __.PrecomputeConditionalExpr precondition)
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

        let kav = record.ToAttributeValues(key)
        let request = new UpdateItemRequest()
        request.Key <- kav
        request.TableName <- tableName
        request.UpdateExpression <- updater.Updater.Expression
        updater.Updater.WriteAttributesTo request.ExpressionAttributeNames
        updater.Updater.WriteValuesTo request.ExpressionAttributeValues
        request.ReturnValues <- 
            if defaultArg returnLatest true then ReturnValue.ALL_NEW
            else ReturnValue.ALL_OLD

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

    /// <summary>
    ///     Asynchronously updates item with supplied key using provided record update expression.
    /// </summary>
    /// <param name="key">Key of item to be updated.</param>
    /// <param name="updater">Table update expression.</param>
    /// <param name="precondition">Specifies a precondition expression that existing item should satisfy.</param>
    /// <param name="returnLatest">Specifies the operation should return the latest (true) or older (false) version of the item. Defaults to latest.</param>
    member __.UpdateItemAsync(key : TableKey, updateExpr : Expr<'TRecord -> 'TRecord>, 
                                ?precondition : Expr<'TRecord -> bool>, ?returnLatest : bool) = async {
        let updater = record.ExtractRecExprUpdater updateExpr
        let precondition = precondition |> Option.map record.ExtractConditional
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

        let updater = record.ExtractOpExprUpdater updateExpr
        let precondition = precondition |> Option.map record.ExtractConditional
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
        let kav = record.ToAttributeValues(key)
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
        let kav = record.ToAttributeValues key
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

        let kc = record.ExtractConditional keyCondition
        let fc = filterCondition |> Option.map record.ExtractConditional
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

    /// <summary>
    ///     Asynchronously scans table with given condition expressions.
    /// </summary>
    /// <param name="filterCondition">Filter condition expression.</param>
    /// <param name="limit">Maximum number of items to evaluate.</param>
    /// <param name="consistentRead">Specify whether to perform consistent read operation.</param>
    member __.ScanAsync(filterExpr : Expr<'TRecord -> bool>, ?limit : int, ?consistentRead : bool) : Async<'TRecord []> = async {
        let cond = record.ExtractConditional filterExpr
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


type TableContext =

    /// <summary>
    ///     Asynchronously generates a DynamoDB client instance for given F# record, table name.
    /// </summary>
    /// <param name="client">DynamoDB client instance.</param>
    /// <param name="tableName">Table name to target.</param>
    /// <param name="createIfNotExists">Create table if it does not already exist. Defaults to false.</param>
    /// <param name="provisionedThroughput">Provisioned throughput for the table if newly created.</param>
    static member GetTableContextAsync<'TRecord>(client : IAmazonDynamoDB, tableName : string, ?createIfNotExists : bool, ?provisionedThroughput : ProvisionedThroughput) = async {
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
            let provisionedThroughput = defaultArg provisionedThroughput (new ProvisionedThroughput(10L,10L))
            let ctr = rd.KeySchema.CreateCreateTableRequest (tableName, provisionedThroughput)
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

        return new TableContext<'TRecord>(client, tableName, rd)
    }

    /// <summary>
    ///     Generates a DynamoDB client instance for given F# record, table name.
    /// </summary>
    /// <param name="client">DynamoDB client instance.</param>
    /// <param name="tableName">Table name to target.</param>
    /// <param name="createIfNotExists">Create table if it does not already exist. Defaults to false.</param>
    /// <param name="provisionedThroughput">Provisioned throughput for the table if newly created.</param>
    static member GetTableContext<'TRecord>(client : IAmazonDynamoDB, tableName : string, ?createIfNotExists : bool, ?provisionedThroughput : ProvisionedThroughput) =
        TableContext.GetTableContextAsync<'TRecord>(client, tableName, ?createIfNotExists = createIfNotExists)
        |> Async.RunSynchronously


[<AutoOpen>]
module TableContextUtils =
    
    /// Precomputes a conditional expression
    let inline cond (ctx : TableContext<'TRecord>) expr = ctx.PrecomputeConditionalExpr expr
    /// Precomputes an update expression
    let inline updateOp (ctx : TableContext<'TRecord>) (expr : Expr<'TRecord -> UpdateOp>) = ctx.PrecomputeUpdateExpr expr
    /// Precomputes an update expression
    let inline updateRec (ctx : TableContext<'TRecord>) (expr : Expr<'TRecord -> 'TRecord>) = ctx.PrecomputeUpdateExpr expr