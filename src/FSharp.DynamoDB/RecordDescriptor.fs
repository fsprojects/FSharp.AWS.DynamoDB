namespace FSharp.DynamoDB

open System
open System.Collections.Generic
open System.Collections.Concurrent

open Microsoft.FSharp.Quotations

open Amazon.DynamoDBv2
open Amazon.DynamoDBv2.Model

open FSharp.DynamoDB.RecordSchema
open FSharp.DynamoDB.ConditionalExpr
open FSharp.DynamoDB.UpdateExpr

type internal RecordDescriptor<'Record> internal () =
    let converter = FieldConverter.resolve<'Record>() :?> RecordConverter<'Record>
    let keyStructure = KeyStructure.FromRecordInfo converter.RecordInfo
    let keySchema = TableKeySchema.OfKeyStructure keyStructure

    member __.KeySchema = keySchema
    member __.Info = converter.RecordInfo
    member __.ToAttributeValues(key : TableKey) = KeyStructure.ExtractKey(keyStructure, key)

    member __.ExtractKey(record : 'Record) = 
        KeyStructure.ExtractKey(keyStructure, converter.RecordInfo, record)

    member __.ExtractConditional(expr : Expr<'Record -> bool>) : ConditionExpression<'Record> =
        let cexpr = ConditionalExpression.Extract converter.RecordInfo expr
        new ConditionExpression<'Record>(cexpr, expr)

    member __.ExtractRecExprUpdater(expr : Expr<'Record -> 'Record>) : UpdateExpression<'Record> =
        let aexpr = extractRecordExprUpdaters converter.RecordInfo expr
        let uexpr = extractUpdateExpression aexpr
        new UpdateExpression<'Record>(uexpr, expr)

    member __.ExtractOpExprUpdater(expr : Expr<'TRecord -> UpdateOp>) : UpdateExpression<'Record> =
        let aexpr = extractOpExprUpdaters converter.RecordInfo expr
        let uexpr = extractUpdateExpression aexpr
        new UpdateExpression<'Record>(uexpr, expr)

    member __.ToAttributeValues(record : 'Record) =
        let kv = converter.OfRecord record

        match keyStructure with
        | DefaultHashKey(name, hashKey, converter, _) ->
            let av = hashKey |> converter.OfFieldUntyped |> Option.get
            kv.Add(name, av)
        | _ -> ()

        kv

    member __.OfAttributeValues(ro : RestObject) = converter.ToRecord ro

type internal RecordDescriptor private () =
    static let descriptors = new ConcurrentDictionary<Type, Lazy<obj>>()
    static member Create<'TRecord> () =
        let mkRd _ =  lazy(new RecordDescriptor<'TRecord>() :> obj)
        let v = descriptors.GetOrAdd(typeof<'TRecord>, mkRd)
        v.Value :?> RecordDescriptor<'TRecord>