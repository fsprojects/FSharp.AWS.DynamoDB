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
    let pickler = Pickler.resolve<'Record>() :?> RecordPickler<'Record>
    let keyStructure = KeyStructure.FromRecordInfo pickler.RecordInfo
    let keySchema = TableKeySchema.OfKeyStructure keyStructure

    member __.KeySchema = keySchema
    member __.Info = pickler.RecordInfo
    member __.ToAttributeValues(key : TableKey) = KeyStructure.ExtractKey(keyStructure, key)

    member __.ExtractKey(record : 'Record) = 
        KeyStructure.ExtractKey(keyStructure, pickler.RecordInfo, record)

    member __.ExtractConditional(expr : Expr<'Record -> bool>) : ConditionExpression<'Record> =
        let cexpr = ConditionalExpression.Extract pickler.RecordInfo expr
        new ConditionExpression<'Record>(cexpr)

    member __.ExtractRecExprUpdater(expr : Expr<'Record -> 'Record>) : UpdateExpression<'Record> =
        let aexpr = extractRecordExprUpdaters pickler.RecordInfo expr
        let uexpr = UpdateExpression.Extract aexpr
        new UpdateExpression<'Record>(uexpr)

    member __.ExtractOpExprUpdater(expr : Expr<'TRecord -> UpdateOp>) : UpdateExpression<'Record> =
        let aexpr = extractOpExprUpdaters pickler.RecordInfo expr
        let uexpr = UpdateExpression.Extract aexpr
        new UpdateExpression<'Record>(uexpr)

    member __.ToAttributeValues(record : 'Record) =
        let kv = pickler.OfRecord record

        match keyStructure with
        | DefaultHashKey(name, hashKey, pickler, _) ->
            let av = hashKey |> pickler.PickleUntyped |> Option.get
            kv.Add(name, av)
        | _ -> ()

        kv

    member __.OfAttributeValues(ro : RestObject) = pickler.ToRecord ro

type internal RecordDescriptor private () =
    static let descriptors = new ConcurrentDictionary<Type, Lazy<obj>>()
    static member Create<'TRecord> () =
        let mkRd _ =  lazy(new RecordDescriptor<'TRecord>() :> obj)
        let v = descriptors.GetOrAdd(typeof<'TRecord>, mkRd)
        v.Value :?> RecordDescriptor<'TRecord>