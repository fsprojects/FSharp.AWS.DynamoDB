module internal FSharp.DynamoDB.RecordInfoBuilder

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

let extractKeyStructure (recordType : Type) (properties : RecordProperty[]) =
    let hkcaOpt = recordType.TryGetAttribute<HashKeyConstantAttribute> ()

    match properties |> Array.filter (fun p -> p.ContainsAttribute<HashKeyAttribute>()) with
    | [|hashKeyP|] when Option.isSome hkcaOpt ->
        invalidArg (string recordType) "Cannot attach HashKey attribute to records containing RangeKeyConstant attribute."

    | [|hashKeyP|] ->
        match properties |> Array.filter (fun p -> p.ContainsAttribute<RangeKeyAttribute>()) with
        | [||] -> HashKeyOnly hashKeyP
        | [|rangeKeyP|] -> Combined(hashKeyP, rangeKeyP)
        | _ -> invalidArg (string recordType) "Found more than one record fields carrying the RangeKey attribute."

    | [||] when Option.isSome hkcaOpt ->
        match properties |> Array.filter (fun p -> p.ContainsAttribute<RangeKeyAttribute>()) with
        | [||] -> invalidArg (string recordType) "Records carrying the RangeKeyConstant attribute must specify a RangeKey property."
        | [|rangeKeyP|] -> 
            let hkca = Option.get hkcaOpt
            let converter = resolveConvUntyped hkca.HashKeyType
            DefaultHashKey(hkca.Name, hkca.HashKey, converter, rangeKeyP)

        | _ -> invalidArg (string recordType) "Found more than one record fields carrying the RangeKey attribute."

    | [||] -> invalidArg (string recordType) "Found no record fields carrying the HashKey attribute."
    | _ -> invalidArg (string recordType) "Found more than one record fields carrying the HashKey attribute."


let mkRecordProperty (propertyInfo : PropertyInfo) =
    let attributes = propertyInfo.GetAttributes()
    let converter = 
        match tryGetAttribute<PropertySerializerAttribute> attributes with
        | Some serializer -> new SerializerConverter(propertyInfo, serializer) :> FieldConverter
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
    let keyStructure = extractKeyStructure recordType properties
    let keySchema = TableKeySchema.OfKeyStructure keyStructure
    { 
        RecordType = recordType
        ConstructorInfo = ctorInfo 
        KeySchema = keySchema
        KeyStructure = keyStructure
        Properties = properties 
    }