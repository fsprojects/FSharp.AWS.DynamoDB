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

/// DynamoDB table template defined by provided F# record type
[<Sealed; AutoSerializable(false)>]
type RecordTemplate<'Record> internal () =
    let pickler = Pickler.resolve<'Record>() :?> RecordPickler<'Record>
    let keyStructure = KeyStructure.FromRecordInfo pickler.RecordInfo
    let keySchema = TableKeySchema.OfKeyStructure keyStructure

    /// Key schema used by the current record
    member __.KeySchema = keySchema

    /// Record property info
    member internal __.Info = pickler.RecordInfo

    /// <summary>
    ///     Extracts the key that corresponds to supplied record instance.
    /// </summary>
    /// <param name="item">Input record instance.</param>
    member __.ExtractKey(record : 'Record) = 
        KeyStructure.ExtractKey(keyStructure, pickler.RecordInfo, record)

    /// <summary>
    ///     Precomputes a DynamoDB conditional expression using
    ///     supplied quoted record predicate.
    /// </summary>
    /// <param name="expr">Quoted record predicate.</param>
    member __.PrecomputeConditionalExpr(expr : Expr<'Record -> bool>) : ConditionExpression<'Record> =
        let cexpr = ConditionalExpression.Extract0 pickler.RecordInfo expr
        new ConditionExpression<'Record>(cexpr)

    /// <summary>
    ///     Precomputes a DynamoDB conditional expression using
    ///     supplied quoted record predicate.
    /// </summary>
    /// <param name="expr">Quoted record predicate.</param>
    member __.PrecomputeConditionalExpr(expr : Expr<'I1 -> 'Record -> bool>) =
        let f = ConditionalExpression.Extract1 pickler.RecordInfo expr
        fun i1 -> new ConditionExpression<'Record>(f i1)

    /// <summary>
    ///     Precomputes a DynamoDB conditional expression using
    ///     supplied quoted record predicate.
    /// </summary>
    /// <param name="expr">Quoted record predicate.</param>
    member __.PrecomputeConditionalExpr(expr : Expr<'I1 -> 'I2 -> 'Record -> bool>) =
        let f = ConditionalExpression.Extract2 pickler.RecordInfo expr
        fun i1 i2 -> new ConditionExpression<'Record>(f i1 i2)

    /// <summary>
    ///     Precomputes a DynamoDB conditional expression using
    ///     supplied quoted record predicate.
    /// </summary>
    /// <param name="expr">Quoted record predicate.</param>
    member __.PrecomputeConditionalExpr(expr : Expr<'I1 -> 'I2 -> 'I3 -> 'Record -> bool>) =
        let f = ConditionalExpression.Extract3 pickler.RecordInfo expr
        fun i1 i2 i3 -> new ConditionExpression<'Record>(f i1 i2 i3)

    /// <summary>
    ///     Precomputes a DynamoDB update expression using
    ///     supplied quoted record update expression.
    /// </summary>
    /// <param name="expr">Quoted record update expression.</param>
    member __.PrecomputeUpdateExpr(expr : Expr<'Record -> 'Record>) : UpdateExpression<'Record> =
        let uexpr = UpdateExpression.ExtractUpdateExpr0 pickler.RecordInfo expr
        new UpdateExpression<'Record>(uexpr)

    /// <summary>
    ///     Precomputes a DynamoDB update expression using
    ///     supplied quoted record update expression.
    /// </summary>
    /// <param name="expr">Quoted record update expression.</param>
    member __.PrecomputeUpdateExpr(expr : Expr<'I1 -> 'Record -> 'Record>) =
        let uexpr = UpdateExpression.ExtractUpdateExpr1 pickler.RecordInfo expr
        fun i1 -> new UpdateExpression<'Record>(uexpr i1)

    /// <summary>
    ///     Precomputes a DynamoDB update expression using
    ///     supplied quoted record update expression.
    /// </summary>
    /// <param name="expr">Quoted record update expression.</param>
    member __.PrecomputeUpdateExpr(expr : Expr<'I1 -> 'I2 -> 'Record -> 'Record>) =
        let uexpr = UpdateExpression.ExtractUpdateExpr2 pickler.RecordInfo expr
        fun i1 i2 -> new UpdateExpression<'Record>(uexpr i1 i2)

    /// <summary>
    ///     Precomputes a DynamoDB update expression using
    ///     supplied quoted record update expression.
    /// </summary>
    /// <param name="expr">Quoted record update expression.</param>
    member __.PrecomputeUpdateExpr(expr : Expr<'I1 -> 'I2 -> 'I3 -> 'Record -> 'Record>) =
        let uexpr = UpdateExpression.ExtractUpdateExpr3 pickler.RecordInfo expr
        fun i1 i2 i3 -> new UpdateExpression<'Record>(uexpr i1 i2 i3)

    /// <summary>
    ///     Precomputes a DynamoDB update expression using
    ///     supplied quoted record update operations.
    /// </summary>
    /// <param name="expr">Quoted record update operations.</param>
    member __.PrecomputeUpdateExpr(expr : Expr<'TRecord -> UpdateOp>) : UpdateExpression<'Record> =
        let uexpr = UpdateExpression.ExtractOpExpr0 pickler.RecordInfo expr
        new UpdateExpression<'Record>(uexpr)

    /// <summary>
    ///     Precomputes a DynamoDB update expression using
    ///     supplied quoted record update operations.
    /// </summary>
    /// <param name="expr">Quoted record update operations.</param>
    member __.PrecomputeUpdateExpr(expr : Expr<'I1 -> 'TRecord -> UpdateOp>) =
        let uexpr = UpdateExpression.ExtractOpExpr1 pickler.RecordInfo expr
        fun i1 -> new UpdateExpression<'Record>(uexpr i1)

    /// <summary>
    ///     Precomputes a DynamoDB update expression using
    ///     supplied quoted record update operations.
    /// </summary>
    /// <param name="expr">Quoted record update operations.</param>
    member __.PrecomputeUpdateExpr(expr : Expr<'I1 -> 'I2 -> 'TRecord -> UpdateOp>) =
        let uexpr = UpdateExpression.ExtractOpExpr2 pickler.RecordInfo expr
        fun i1 i2 -> new UpdateExpression<'Record>(uexpr i1 i2)

    /// <summary>
    ///     Precomputes a DynamoDB update expression using
    ///     supplied quoted record update operations.
    /// </summary>
    /// <param name="expr">Quoted record update operations.</param>
    member __.PrecomputeUpdateExpr(expr : Expr<'I1 -> 'I2 -> 'I3 -> 'TRecord -> UpdateOp>) =
        let uexpr = UpdateExpression.ExtractOpExpr3 pickler.RecordInfo expr
        fun i1 i2 i3 -> new UpdateExpression<'Record>(uexpr i1 i2 i3)

    /// Convert table key to attribute values
    member internal __.ToAttributeValues(key : TableKey) = 
        KeyStructure.ExtractKey(keyStructure, key)

    /// Converts a record instance to attribute values
    member internal __.ToAttributeValues(record : 'Record) =
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
    member internal __.OfAttributeValues(ro : RestObject) = pickler.ToRecord ro

/// Record template factory methods
[<Sealed; AbstractClass>]
type RecordTemplate private () =

    static let descriptors = new ConcurrentDictionary<Type, Lazy<obj>>()

    /// Defines a DynamoDB tempalte instance using given F# record type
    static member Define<'TRecord> () : RecordTemplate<'TRecord> =
        let mkRd _ =  lazy(new RecordTemplate<'TRecord>() :> obj)
        let v = descriptors.GetOrAdd(typeof<'TRecord>, mkRd)
        v.Value :?> RecordTemplate<'TRecord>