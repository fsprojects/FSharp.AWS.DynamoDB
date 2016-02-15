namespace FSharp.DynamoDB

open System
open System.Collections.Generic
open System.Collections.Concurrent

open Microsoft.FSharp.Quotations

open Amazon.DynamoDBv2
open Amazon.DynamoDBv2.Model

open FSharp.DynamoDB.FieldConverter
open FSharp.DynamoDB.Common
open FSharp.DynamoDB.RecordInfoBuilder
open FSharp.DynamoDB.ConditionalExprs

type internal RecordDescriptor<'Record> internal () =
    let recordInfo = mkRecordInfo typeof<'Record>
    let exprCmp = new ExprEqualityComparer()
    let conditionals = new ConcurrentDictionary<Expr, ConditionalExpression>(exprCmp)

    member __.KeySchema = recordInfo.KeySchema
    member __.Properties = recordInfo.Properties
    member __.ToAttributeValues(key : TableKey) =
        let dict = new Dictionary<string, AttributeValue> ()
        let extractKey name (conv : FieldConverter) (value:obj) =
            if obj.ReferenceEquals(value, null) then invalidArg name "Key value was not specified."
            let fav = conv.OfFieldUntyped value
            let av = FsAttributeValue.ToAttributeValue fav
            dict.Add(name, av)

        match recordInfo.KeyStructure with
        | HashKeyOnly hkp -> extractKey hkp.Name hkp.Converter key.HashKey
        | Combined(hkp,rkp) ->
            extractKey hkp.Name hkp.Converter key.HashKey
            extractKey rkp.Name rkp.Converter key.RangeKey
        | DefaultHashKey(name, value, conv, rkp) ->
            if key.IsHashKeySpecified then
                extractKey name conv key.HashKey
            else
                let av = value |> conv.OfFieldUntyped |> FsAttributeValue.ToAttributeValue
                dict.Add(name, av)

            extractKey rkp.Name rkp.Converter key.RangeKey

        dict

    member __.ExtractKey(record : 'Record) =
        let dict = new Dictionary<string, AttributeValue> ()
        let inline getValue (rp : RecordProperty) = rp.PropertyInfo.GetValue(record)
        match recordInfo.KeyStructure with
        | HashKeyOnly hkp -> let hashKey = getValue hkp in TableKey.Hash hashKey
        | Combined(hkp,rkp) ->
            let hashKey = getValue hkp
            let rangeKey = getValue rkp
            TableKey.Combined(hashKey, rangeKey)
        | DefaultHashKey(_, hashKey, _, rkp) ->
            let rangeKey = getValue rkp
            TableKey.Combined(hashKey, rangeKey)

    member __.ExtractConditional(expr : Expr<'Record -> bool>) : ConditionalExpression =
        conditionals.GetOrAdd(expr, fun _ -> extractQueryExpr recordInfo expr)

    member __.ToAttributeValues(record : 'Record) =
        let dict = new Dictionary<string, AttributeValue> ()
        for rp in recordInfo.Properties do
            let value = rp.PropertyInfo.GetValue(record)
            match rp.Converter.OfFieldUntyped value with
            | Undefined -> ()
            | repr -> 
                let av = FsAttributeValue.ToAttributeValue(repr) 
                dict.Add(rp.Name, av)

        match recordInfo.KeyStructure with
        | DefaultHashKey(name, hashKey, converter, _) ->
            let av = hashKey |> converter.OfFieldUntyped |> FsAttributeValue.ToAttributeValue
            dict.Add(name, av)

        | _ -> ()

        dict

    member __.OfAttributeValues(dict : Dictionary<string, AttributeValue>) =
        let readValue(rp : RecordProperty) =
            let ok, found = dict.TryGetValue rp.Name
            if ok then rp.Converter.ToFieldUntyped (FsAttributeValue.FromAttributeValue found)
            elif rp.AllowDefaultValue then rp.Converter.DefaultValueUntyped
            else invalidOp <| sprintf "Could not locate attribute value for field '%s'." rp.Name

        let values = Array.map readValue recordInfo.Properties
        recordInfo.ConstructorInfo.Invoke values :?> 'Record

type internal RecordDescriptor private () =
    static let descriptors = new ConcurrentDictionary<Type, Lazy<obj>>()
    static member Create<'TRecord> () =
        let rd = lazy(new RecordDescriptor<'TRecord>() :> obj)
        descriptors.GetOrAdd(typeof<'TRecord>, rd).Value :?> RecordDescriptor<'TRecord>