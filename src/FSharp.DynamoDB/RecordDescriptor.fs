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

/// Provides common methods for performing table operations with record type
type internal RecordDescriptor<'Record> internal () =
    let pickler = Pickler.resolve<'Record>() :?> RecordPickler<'Record>
    let keyStructure = KeyStructure.FromRecordInfo pickler.RecordInfo
    let keySchema = TableKeySchema.OfKeyStructure keyStructure

    member __.KeySchema = keySchema
    member __.Info = pickler.RecordInfo

    /// Convert table key to attribute values
    member __.ToAttributeValues(key : TableKey) = 
        KeyStructure.ExtractKey(keyStructure, key)

    /// Extract table key from record instance
    member __.ExtractKey(record : 'Record) = 
        KeyStructure.ExtractKey(keyStructure, pickler.RecordInfo, record)

    /// Extract conditional expression from quoted F# predicate
    member __.ExtractConditional(expr : Expr<'Record -> bool>) : ConditionExpression<'Record> =
        let cexpr = ConditionalExpression.Extract pickler.RecordInfo expr
        new ConditionExpression<'Record>(cexpr)

    /// Extract update expression from quoted F# record update
    member __.ExtractRecExprUpdater(expr : Expr<'Record -> 'Record>) : UpdateExpression<'Record> =
        let aexpr = extractRecordExprUpdaters pickler.RecordInfo expr
        let uexpr = UpdateExpression.Extract aexpr
        new UpdateExpression<'Record>(uexpr)

    /// Extract update expression from quoted update operations
    member __.ExtractOpExprUpdater(expr : Expr<'TRecord -> UpdateOp>) : UpdateExpression<'Record> =
        let aexpr = extractOpExprUpdaters pickler.RecordInfo expr
        let uexpr = UpdateExpression.Extract aexpr
        new UpdateExpression<'Record>(uexpr)

    /// Converts a record instance to attribute values
    member __.ToAttributeValues(record : 'Record) =
        let kv = pickler.OfRecord record

        match keyStructure with
        | DefaultHashKey(name, hashKey, pickler, _) ->
            let av = hashKey |> pickler.PickleUntyped |> Option.get
            kv.Add(name, av)

        | DefaultRangeKey(name, rangeKey, pickler, _) ->
            let av = rangeKey |> pickler.PickleUntyped |> Option.get
            kv.Add(name, av)

        | _ -> ()

        kv

    /// Constructs a record instance from attribute values
    member __.OfAttributeValues(ro : RestObject) = pickler.ToRecord ro

type internal RecordDescriptor private () =
    static let descriptors = new ConcurrentDictionary<Type, Lazy<obj>>()
    /// Fetches a record descriptor instance for given F# record type
    static member Create<'TRecord> () =
        let mkRd _ =  lazy(new RecordDescriptor<'TRecord>() :> obj)
        let v = descriptors.GetOrAdd(typeof<'TRecord>, mkRd)
        v.Value :?> RecordDescriptor<'TRecord>