namespace FSharp.AWS.DynamoDB

open System
open System.Collections.Generic
open System.Collections.Concurrent

open Microsoft.FSharp.Quotations
open Microsoft.FSharp.Reflection

open Amazon.DynamoDBv2
open Amazon.DynamoDBv2.Model

open FSharp.AWS.DynamoDB.KeySchema
open FSharp.AWS.DynamoDB.ConditionalExpr
open FSharp.AWS.DynamoDB.UpdateExpr
open FSharp.AWS.DynamoDB.ProjectionExpr

/// DynamoDB table template defined by provided F# record type
[<Sealed; AutoSerializable(false)>]
type RecordTemplate<'TRecord> internal () =
    do if not <| FSharpType.IsRecord(typeof<'TRecord>, true) then
        invalidArg (string typeof<'TRecord>) "not a valid F# record type."

    let pickler = Pickler.resolve<'TRecord>() :?> RecordPickler<'TRecord>
    let recordInfo = RecordTableInfo.FromRecordPickler pickler
    let hkeyCond = 
        ConditionalExpression.TryExtractHashKeyCondition recordInfo
        |> Option.map (fun cond -> new ConditionExpression<'TRecord>(cond))

    /// Key schema used by the current record
    member __.PrimaryKey = recordInfo.PrimaryKeySchema
    /// Global Secondary index key schemata
    member __.GlobalSecondaryIndices = recordInfo.GlobalSecondaryIndices
    /// Local Secondary index key schemata
    member __.LocalSecondaryIndices = recordInfo.LocalSecondaryIndices

    /// Gets the constant HashKey if specified by the record implementation
    member __.ConstantHashKey : obj option =
        match recordInfo.PrimaryKeyStructure with
        | DefaultHashKey(_, value, _, _) -> Some value
        | _ -> None

    /// Gets the constant RangeKey if specified by the record implementation
    member __.ConstantRangeKey : obj option =
        match recordInfo.PrimaryKeyStructure with
        | DefaultRangeKey(_, value, _, _) -> Some value
        | _ -> None

    /// Gets a condition expression that matches the constant HashKey,
    /// if so specified.
    member __.ConstantHashKeyCondition = hkeyCond

    /// Record property info
    member internal __.Info = recordInfo

    /// <summary>
    ///     Extracts the key that corresponds to supplied record instance.
    /// </summary>
    /// <param name="item">Input record instance.</param>
    member __.ExtractKey(record : 'TRecord) = 
        PrimaryKeyStructure.ExtractKey(recordInfo.PrimaryKeyStructure, record)

    /// Generates a conditional which verifies whether an item already exists.
    member __.ItemExists =
        let cond = mkItemExistsCondition recordInfo.PrimaryKeySchema
        new ConditionExpression<'TRecord>(cond)

    /// Generates a conditional which verifies whether an item does not already exist.
    member __.ItemDoesNotExist =
        let cond = mkItemNotExistsCondition recordInfo.PrimaryKeySchema
        new ConditionExpression<'TRecord>(cond)

    /// <summary>
    ///     Precomputes a DynamoDB conditional expression using
    ///     supplied quoted record predicate.
    /// </summary>
    /// <param name="expr">Quoted record predicate.</param>
    member __.PrecomputeConditionalExpr(expr : Expr<'TRecord -> bool>) : ConditionExpression<'TRecord> =
        let cexpr = ConditionalExpression.Extract recordInfo expr
        new ConditionExpression<'TRecord>(cexpr)

    /// <summary>
    ///     Precomputes a DynamoDB conditional expression using
    ///     supplied quoted record predicate.
    /// </summary>
    /// <param name="expr">Quoted record predicate.</param>
    member __.PrecomputeConditionalExpr(expr : Expr<'I1 -> 'TRecord -> bool>) : 'I1 -> ConditionExpression<'TRecord> =
        let f = ConditionalExpression.Extract recordInfo expr
        fun i1 -> new ConditionExpression<'TRecord>(f.Apply(i1))

    /// <summary>
    ///     Precomputes a DynamoDB conditional expression using
    ///     supplied quoted record predicate.
    /// </summary>
    /// <param name="expr">Quoted record predicate.</param>
    member __.PrecomputeConditionalExpr(expr : Expr<'I1 -> 'I2 -> 'TRecord -> bool>) : 'I1 -> 'I2 -> ConditionExpression<'TRecord> =
        let f = ConditionalExpression.Extract recordInfo expr
        fun i1 i2 -> new ConditionExpression<'TRecord>(f.Apply(i1, i2))

    /// <summary>
    ///     Precomputes a DynamoDB conditional expression using
    ///     supplied quoted record predicate.
    /// </summary>
    /// <param name="expr">Quoted record predicate.</param>
    member __.PrecomputeConditionalExpr(expr : Expr<'I1 -> 'I2 -> 'I3 -> 'TRecord -> bool>) : 'I1 -> 'I2 -> 'I3 -> ConditionExpression<'TRecord> =
        let f = ConditionalExpression.Extract recordInfo expr
        fun i1 i2 i3 -> new ConditionExpression<'TRecord>(f.Apply(i1, i2, i3))

    /// <summary>
    ///     Precomputes a DynamoDB conditional expression using
    ///     supplied quoted record predicate.
    /// </summary>
    /// <param name="expr">Quoted record predicate.</param>
    member __.PrecomputeConditionalExpr(expr : Expr<'I1 -> 'I2 -> 'I3 -> 'I4 -> 'TRecord -> bool>) : 'I1 -> 'I2 -> 'I3 -> 'I4 -> ConditionExpression<'TRecord> =
        let f = ConditionalExpression.Extract recordInfo expr
        fun i1 i2 i3 i4 -> new ConditionExpression<'TRecord>(f.Apply(i1, i2, i3, i4))

    /// <summary>
    ///     Precomputes a DynamoDB conditional expression using
    ///     supplied quoted record predicate.
    /// </summary>
    /// <param name="expr">Quoted record predicate.</param>
    member __.PrecomputeConditionalExpr(expr : Expr<'I1 -> 'I2 -> 'I3 -> 'I4 -> 'I5 -> 'TRecord -> bool>) : 'I1 -> 'I2 -> 'I3 -> 'I4 -> 'I5 -> ConditionExpression<'TRecord> =
        let f = ConditionalExpression.Extract recordInfo expr
        fun i1 i2 i3 i4 i5 -> new ConditionExpression<'TRecord>(f.Apply(i1, i2, i3, i4, i5))

    /// <summary>
    ///     Precomputes a DynamoDB update expression using
    ///     supplied quoted record update expression.
    /// </summary>
    /// <param name="expr">Quoted record update expression.</param>
    member __.PrecomputeUpdateExpr(expr : Expr<'TRecord -> 'TRecord>) : UpdateExpression<'TRecord> =
        let uops = UpdateOperations.ExtractUpdateExpr recordInfo expr
        new UpdateExpression<'TRecord>(uops)

    /// <summary>
    ///     Precomputes a DynamoDB update expression using
    ///     supplied quoted record update expression.
    /// </summary>
    /// <param name="expr">Quoted record update expression.</param>
    member __.PrecomputeUpdateExpr(expr : Expr<'I1 -> 'TRecord -> 'TRecord>) : 'I1 -> UpdateExpression<'TRecord> =
        let uops = UpdateOperations.ExtractUpdateExpr recordInfo expr
        fun i1 -> new UpdateExpression<'TRecord>(uops.Apply(i1))

    /// <summary>
    ///     Precomputes a DynamoDB update expression using
    ///     supplied quoted record update expression.
    /// </summary>
    /// <param name="expr">Quoted record update expression.</param>
    member __.PrecomputeUpdateExpr(expr : Expr<'I1 -> 'I2 -> 'TRecord -> 'TRecord>) : 'I1 -> 'I2 -> UpdateExpression<'TRecord> =
        let uops = UpdateOperations.ExtractUpdateExpr recordInfo expr
        fun i1 i2 -> new UpdateExpression<'TRecord>(uops.Apply(i1, i2))

    /// <summary>
    ///     Precomputes a DynamoDB update expression using
    ///     supplied quoted record update expression.
    /// </summary>
    /// <param name="expr">Quoted record update expression.</param>
    member __.PrecomputeUpdateExpr(expr : Expr<'I1 -> 'I2 -> 'I3 -> 'TRecord -> 'TRecord>) : 'I1 -> 'I2 -> 'I3 -> UpdateExpression<'TRecord> =
        let uops = UpdateOperations.ExtractUpdateExpr recordInfo expr
        fun i1 i2 i3 -> new UpdateExpression<'TRecord>(uops.Apply(i1, i2, i3))

    /// <summary>
    ///     Precomputes a DynamoDB update expression using
    ///     supplied quoted record update expression.
    /// </summary>
    /// <param name="expr">Quoted record update expression.</param>
    member __.PrecomputeUpdateExpr(expr : Expr<'I1 -> 'I2 -> 'I3 -> 'I4 -> 'TRecord -> 'TRecord>) : 'I1 -> 'I2 -> 'I3 -> 'I4 -> UpdateExpression<'TRecord> =
        let uops = UpdateOperations.ExtractUpdateExpr recordInfo expr
        fun i1 i2 i3 i4 -> new UpdateExpression<'TRecord>(uops.Apply(i1, i2, i3, i4))

    /// <summary>
    ///     Precomputes a DynamoDB update expression using
    ///     supplied quoted record update expression.
    /// </summary>
    /// <param name="expr">Quoted record update expression.</param>
    member __.PrecomputeUpdateExpr(expr : Expr<'I1 -> 'I2 -> 'I3 -> 'I4 -> 'I5 -> 'TRecord -> 'TRecord>) : 'I1 -> 'I2 -> 'I3 -> 'I4 -> 'I5 -> UpdateExpression<'TRecord> =
        let uops = UpdateOperations.ExtractUpdateExpr recordInfo expr
        fun i1 i2 i3 i4 i5 -> new UpdateExpression<'TRecord>(uops.Apply(i1, i2, i3, i4, i5))

    /// <summary>
    ///     Precomputes a DynamoDB update expression using
    ///     supplied quoted record update operations.
    /// </summary>
    /// <param name="expr">Quoted record update operations.</param>
    member __.PrecomputeUpdateExpr(expr : Expr<'TRecord -> UpdateOp>) : UpdateExpression<'TRecord> =
        let uops = UpdateOperations.ExtractOpExpr recordInfo expr
        new UpdateExpression<'TRecord>(uops)

    /// <summary>
    ///     Precomputes a DynamoDB update expression using
    ///     supplied quoted record update operations.
    /// </summary>
    /// <param name="expr">Quoted record update operations.</param>
    member __.PrecomputeUpdateExpr(expr : Expr<'I1 -> 'TRecord -> UpdateOp>) : 'I1 -> UpdateExpression<'TRecord> =
        let uops = UpdateOperations.ExtractOpExpr recordInfo expr
        fun i1 -> new UpdateExpression<'TRecord>(uops.Apply i1)

    /// <summary>
    ///     Precomputes a DynamoDB update expression using
    ///     supplied quoted record update operations.
    /// </summary>
    /// <param name="expr">Quoted record update operations.</param>
    member __.PrecomputeUpdateExpr(expr : Expr<'I1 -> 'I2 -> 'TRecord -> UpdateOp>) : 'I1 -> 'I2 -> UpdateExpression<'TRecord> =
        let uops = UpdateOperations.ExtractOpExpr recordInfo expr
        fun i1 i2 -> new UpdateExpression<'TRecord>(uops.Apply(i1, i2))

    /// <summary>
    ///     Precomputes a DynamoDB update expression using
    ///     supplied quoted record update operations.
    /// </summary>
    /// <param name="expr">Quoted record update operations.</param>
    member __.PrecomputeUpdateExpr(expr : Expr<'I1 -> 'I2 -> 'I3 -> 'TRecord -> UpdateOp>) : 'I1 -> 'I2 -> 'I3 -> UpdateExpression<'TRecord> =
        let uops = UpdateOperations.ExtractOpExpr recordInfo expr
        fun i1 i2 i3 -> new UpdateExpression<'TRecord>(uops.Apply(i1, i2, i3))

    /// <summary>
    ///     Precomputes a DynamoDB update expression using
    ///     supplied quoted record update operations.
    /// </summary>
    /// <param name="expr">Quoted record update operations.</param>
    member __.PrecomputeUpdateExpr(expr : Expr<'I1 -> 'I2 -> 'I3 -> 'I4 -> 'TRecord -> UpdateOp>) : 'I1 -> 'I2 -> 'I3 -> 'I4 -> UpdateExpression<'TRecord> =
        let uops = UpdateOperations.ExtractOpExpr recordInfo expr
        fun i1 i2 i3 i4 -> new UpdateExpression<'TRecord>(uops.Apply(i1, i2, i3, i4))

    /// <summary>
    ///     Precomputes a DynamoDB update expression using
    ///     supplied quoted record update operations.
    /// </summary>
    /// <param name="expr">Quoted record update operations.</param>
    member __.PrecomputeUpdateExpr(expr : Expr<'I1 -> 'I2 -> 'I3 -> 'I4 -> 'I5 -> 'TRecord -> UpdateOp>) : 'I1 -> 'I2 -> 'I3 -> 'I4 -> 'I5 -> UpdateExpression<'TRecord> =
        let uops = UpdateOperations.ExtractOpExpr recordInfo expr
        fun i1 i2 i3 i4 i5 -> new UpdateExpression<'TRecord>(uops.Apply(i1, i2, i3, i4, i5))

    /// <summary>
    ///     Precomputes a DynamoDB projection expression using
    ///     supplied quoted projection expression.
    /// </summary>
    /// <param name="expr">Quoted record projection expression.</param>
    member __.PrecomputeProjectionExpr(expr : Expr<'TRecord -> 'TProjection>) : ProjectionExpression<'TRecord, 'TProjection> =
        let pexpr = ProjectionExpr.Extract recordInfo expr
        new ProjectionExpression<'TRecord, 'TProjection>(pexpr)

    /// Convert table key to attribute values
    member internal __.ToAttributeValues(key : TableKey) = 
        PrimaryKeyStructure.ExtractKey(recordInfo.PrimaryKeyStructure, key)

    /// Converts a record instance to attribute values
    member internal __.ToAttributeValues(record : 'TRecord) =
        let kv = pickler.OfRecord record

        match recordInfo.PrimaryKeyStructure with
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