namespace FSharp.DynamoDB

open System.Net

open Amazon.DynamoDBv2
open Amazon.DynamoDBv2.Model

open FSharp.DynamoDB.RecordInfo

type TableContext<'TRecord> internal (client : IAmazonDynamoDB, tableName : string, record : RecordDescriptor<'TRecord>) =

    member __.WithRecordType<'TRecord2>() =
        let rd = RecordDescriptor.Create<'TRecord2>()
        if record.KeySchema <> rd.KeySchema then
            invalidArg (string typeof<'TRecord2>) "incompatible key schema."
        
        new TableContext<'TRecord2>(client, tableName, rd)


    member __.PutItemAsync(item : 'TRecord) = async {
        let attrValues = record.ToAttributeValues(item)
        let request = new PutItemRequest(tableName, attrValues)
        request.ReturnValues <- ReturnValue.NONE
        let! ct = Async.CancellationToken
        let! response = client.PutItemAsync(request, ct) |> Async.AwaitTaskCorrect
        if response.HttpStatusCode <> HttpStatusCode.OK then
            failwithf "PutItem request returned error %O" response.HttpStatusCode
    }

    member __.GetItemAsync(hashKey : obj, ?rangeKey : obj) = async {
        let key = record.ExtractKey(hashKey, ?rangeKey = rangeKey)
        let request = new GetItemRequest(tableName, key)
        let! ct = Async.CancellationToken
        let! response = client.GetItemAsync(request, ct) |> Async.AwaitTaskCorrect
        if response.HttpStatusCode <> HttpStatusCode.OK then
            failwithf "GetItem request returned error %O" response.HttpStatusCode

        return record.OfAttributeValues response.Item
    }

type TableContext =

    static member GetTableContext<'TRecord>(client : IAmazonDynamoDB, tableName : string, ?createIfNotExists : bool, ?provisionedThroughPut : ProvisionedThroughput) = async {
        let createIfNotExists = defaultArg createIfNotExists false
        let rd = RecordDescriptor.Create<'TRecord> ()
        try
            let dtr = new DescribeTableRequest(tableName)
            let! ct = Async.CancellationToken
            let! response = client.DescribeTableAsync(dtr, ct) |> Async.AwaitTaskCorrect
            let existingSchema = ofTableDescription response.Table
            if existingSchema <> rd.KeySchema then 
                sprintf "table '%s' contains incompatible key schema %A, which is incompatible with record '%O'." 
                    tableName existingSchema typeof<'TRecord>
                |> invalidOp

        with :? ResourceNotFoundException when createIfNotExists ->
            let provisionedThroughPut = defaultArg provisionedThroughPut (new ProvisionedThroughput(10L,10L))
            let ctr = mkCreateTableRequest rd.KeySchema tableName provisionedThroughPut
            let! ct = Async.CancellationToken
            let! response = client.CreateTableAsync(ctr, ct) |> Async.AwaitTaskCorrect
            if response.HttpStatusCode <> HttpStatusCode.OK then
                failwithf "CreateTable request returned error %O" response.HttpStatusCode

        return new TableContext<'TRecord>(client, tableName, rd)
    }