namespace FSharp.DynamoDB

open System.Collections.Generic
open System.Net

open Amazon.DynamoDBv2
open Amazon.DynamoDBv2.Model

open FSharp.DynamoDB.TableOps

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

    member __.PutItemAsync(item : 'TRecord) : Async<TableKey> = async {
        let attrValues = record.ToAttributeValues(item)
        let request = new PutItemRequest(tableName, attrValues)
        request.ReturnValues <- ReturnValue.NONE
        let! ct = Async.CancellationToken
        let! response = client.PutItemAsync(request, ct) |> Async.AwaitTaskCorrect
        if response.HttpStatusCode <> HttpStatusCode.OK then
            failwithf "PutItem request returned error %O" response.HttpStatusCode

        return record.ExtractKey item
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

    member __.GetItemAsync(key : TableKey) : Async<'TRecord> = async {
        let kav = record.ToAttributeValues(key)
        let request = new GetItemRequest(tableName, kav)
        let! ct = Async.CancellationToken
        let! response = client.GetItemAsync(request, ct) |> Async.AwaitTaskCorrect
        if response.HttpStatusCode <> HttpStatusCode.OK then
            failwithf "GetItem request returned error %O" response.HttpStatusCode

        return record.OfAttributeValues response.Item
    }

    member __.GetItemsAsync(keys : seq<TableKey>, ?consistentRead : bool) : Async<'TRecord[]> = async {
        let consistentRead = defaultArg consistentRead false
        let kna = new KeysAndAttributes()
        kna.AttributesToGet.AddRange(record.Properties |> Seq.map (fun p -> p.Name))
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

        return new TableContext<'TRecord>(client, tableName, rd)
    }