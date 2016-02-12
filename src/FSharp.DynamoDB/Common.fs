module internal FSharp.DynamoDB.Common

open System
open System.Collections.Generic
open System.IO
open System.Reflection

open Amazon.DynamoDBv2
open Amazon.DynamoDBv2.Model

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


type KeyStructure =
    | HashKeyOnly of hashKeyProperty:RecordProperty
    | Combined of hashKeyProperty:RecordProperty * rangeKeyProperty:RecordProperty
    | DefaultHashKey of hkName:string * hkValue:obj * hkConverter:FieldConverter * rangeKeyProperty:RecordProperty

[<NoEquality; NoComparison>]
type RecordInfo =
    {
        RecordType : Type
        KeySchema : TableKeySchema
        KeyStructure : KeyStructure
        Properties : RecordProperty []
        ConstructorInfo : ConstructorInfo
    }