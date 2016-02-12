namespace FSharp.DynamoDB

open System
open System.Collections.Generic
open System.Collections.Concurrent

open Amazon.DynamoDBv2
open Amazon.DynamoDBv2.Model

open FSharp.DynamoDB.FieldConverter
open FSharp.DynamoDB.RecordInfo

type internal RecordDescriptor<'Record> internal () =
    let recordInfo = mkRecordInfo typeof<'Record>

    member __.KeySchema = recordInfo.KeySchema
    member __.Properties = recordInfo.Properties
    member __.ToAttributeValues(key : TableKey) =
        let dict = new Dictionary<string, AttributeValue> ()
        let extractKey (rp : RecordProperty) (value:obj) =
            let fav = rp.Converter.OfFieldUntyped value
            let av = FsAttributeValue.ToAttributeValue fav
            dict.Add(rp.Name, av)

        extractKey recordInfo.HashKeyProperty key.HashKey
        match recordInfo.RangeKeyProperty with
        | None when key.IsRangeKeySpecified -> invalidArg "rangeKey" "RangeKey parameters not supported."
        | None -> ()
        | Some _ when not key.IsRangeKeySpecified -> invalidArg "rangeKey" "A RangeKey value must be specified."
        | Some rkp -> extractKey rkp key.RangeKeyInternal

        dict

    member __.ExtractKey(record : 'Record) =
        let dict = new Dictionary<string, AttributeValue> ()
        let inline getValue (rp : RecordProperty) = rp.PropertyInfo.GetValue(record)
        let hashKey = getValue recordInfo.HashKeyProperty
        match recordInfo.RangeKeyProperty with
        | None -> TableKey.Hash hashKey
        | Some rk -> TableKey.Combined(hashKey, getValue rk)

    member __.ToAttributeValues(record : 'Record) =
        let dict = new Dictionary<string, AttributeValue> ()
        for rp in recordInfo.Properties do
            let value = rp.PropertyInfo.GetValue(record)
            match rp.Converter.OfFieldUntyped value with
            | Undefined -> ()
            | repr -> 
                let av = FsAttributeValue.ToAttributeValue(repr) 
                dict.Add(rp.Name, av)

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