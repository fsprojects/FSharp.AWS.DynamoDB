module internal FSharp.DynamoDB.RecordInfo

open System
open System.Collections.Generic
open System.Reflection

open Microsoft.FSharp.Reflection

open Amazon.DynamoDBv2
open Amazon.DynamoDBv2.Model

open FSharp.DynamoDB.TypeShape
open FSharp.DynamoDB.FieldConverter

[<NoEquality; NoComparison>]
type RecordProperty =
    {
        Name : string
        PropertyInfo : PropertyInfo
        Converter : FieldConverter
        AllowDefaultValue : bool
        Attributes : Attribute []
    }
with
    member rp.TryGetAttribute<'Attribute when 'Attribute :> Attribute> () = tryGetAttribute<'Attribute> rp.Attributes
    member rp.GetAttributes<'Attribute when 'Attribute :> Attribute> () = getAttributes<'Attribute> rp.Attributes
    member rp.ContainsAttribute<'Attribute when 'Attribute :> Attribute> () = containsAttribute<'Attribute> rp.Attributes

[<NoEquality; NoComparison>]
type RecordInfo =
    {
        RecordType : Type
        KeySchema : TableKeySchema
        Properties : RecordProperty []
        HashKeyProperty : RecordProperty
        RangeKeyProperty : RecordProperty option
        ConstructorInfo : ConstructorInfo
    }

let extractKeySchema (recordType : Type) (properties : RecordProperty[]) =
    let mkKeySchema (p : RecordProperty) =
        let keyType =
            match p.Converter.Representation with
            | FieldRepresentation.String -> ScalarAttributeType.S
            | FieldRepresentation.Number -> ScalarAttributeType.N
            | FieldRepresentation.Bytes -> ScalarAttributeType.B
            | FieldRepresentation.Serializer -> invalidArg p.Name <| "DynamoDB Key attributes do not support serialization attributes."
            | _ -> invalidArg p.Name <| sprintf "Unsupported type '%O' for DynamoDB Key attribute." p.Converter.Type

        { AttributeName = p.Name ; KeyType = keyType }

    match properties |> Array.filter (fun p -> p.ContainsAttribute<HashKeyAttribute>()) with
    | [|hashKeyP|] ->
        let hkey = mkKeySchema hashKeyP
        match properties |> Array.filter (fun p -> p.ContainsAttribute<RangeKeyAttribute>()) with
        | [||] -> hashKeyP, None, { HashKey = hkey ; RangeKey = None }
        | [|rangeKeyP|] -> let rkey = mkKeySchema rangeKeyP in hashKeyP, Some rangeKeyP, { HashKey = hkey ; RangeKey = Some rkey }
        | _ -> invalidArg (string recordType) "Found more than one record fields carrying the RangeKey attribute."

    | [||] -> invalidArg (string recordType) "Found no record fields carrying the HashKey attribute."
    | _ -> invalidArg (string recordType) "Found more than one record fields carrying the HashKey attribute."

let ofTableDescription (td : TableDescription) =
    let mkKeySchema (kse : KeySchemaElement) =
        let ad = td.AttributeDefinitions |> Seq.find (fun ad -> ad.AttributeName = kse.AttributeName)
        { AttributeName = kse.AttributeName ; KeyType = ad.AttributeType }
            
    let hk = td.KeySchema |> Seq.find (fun ks -> ks.KeyType = KeyType.HASH) |> mkKeySchema
    let rk = td.KeySchema |> Seq.tryPick (fun ks -> if ks.KeyType = KeyType.RANGE then Some(mkKeySchema ks) else None)
    { HashKey = hk ; RangeKey = rk }

let mkCreateTableRequest (schema : TableKeySchema) (tableName : string) (provisionedThroughput : ProvisionedThroughput) =
    let ctr = new CreateTableRequest(TableName = tableName)
    let addKey kt (ks : KeySchema) =
        ctr.KeySchema.Add <| new KeySchemaElement(ks.AttributeName, kt)
        ctr.AttributeDefinitions.Add <| new AttributeDefinition(ks.AttributeName, ks.KeyType)

    addKey KeyType.HASH schema.HashKey
    schema.RangeKey |> Option.iter (fun rk -> addKey KeyType.RANGE rk)
    ctr.ProvisionedThroughput <- provisionedThroughput
    ctr

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