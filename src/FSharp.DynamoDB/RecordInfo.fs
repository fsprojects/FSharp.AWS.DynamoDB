module internal FSharp.DynamoDB.RecordInfo

open System
open System.Collections.Generic
open System.Reflection

open Microsoft.FSharp.Reflection

open Amazon.DynamoDBv2
open Amazon.DynamoDBv2.Model

open FSharp.DynamoDB.TypeShape
open FSharp.DynamoDB.Common
open FSharp.DynamoDB.TableOps
open FSharp.DynamoDB.FieldConverter

let mkRecordProperty (propertyInfo : PropertyInfo) =
    let attributes = propertyInfo.GetAttributes()
    let converter = 
        match tryGetAttribute<PropertySerializerAttribute> attributes with
        | Some serializer -> new SerializerConverter(serializer, propertyInfo.PropertyType) :> FieldConverter
        | None -> resolveConvUntyped propertyInfo.PropertyType

    let name =
        match attributes |> tryGetAttribute<CustomNameAttribute> with
        | Some cn -> cn.Name
        | None -> propertyInfo.Name

    let allowDefaultValue = containsAttribute<AllowDefaultValueAttribute> attributes

    {
        Name = name
        PropertyInfo = propertyInfo
        Converter = converter
        Attributes = attributes
        AllowDefaultValue = allowDefaultValue
    }

let mkRecordInfo (recordType : Type) =
    let ctorInfo = FSharpValue.PreComputeRecordConstructorInfo (recordType, true)
    let properties = FSharpType.GetRecordFields(recordType, true) |> Array.map mkRecordProperty
    let hashKeyP, rangeKeyP, keySchema = extractKeySchema recordType properties
    { 
        RecordType = recordType
        ConstructorInfo = ctorInfo 
        KeySchema = keySchema
        HashKeyProperty = hashKeyP
        RangeKeyProperty = rangeKeyP
        Properties = properties 
    }