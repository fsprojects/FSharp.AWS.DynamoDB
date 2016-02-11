module internal FSharp.DynamoDB.RecordInfo

open System
open System.Reflection

open FSharp.DynamoDB.TypeShape
open FSharp.DynamoDB.FieldConverter

[<NoEquality; NoComparison>]
type RecordProperty =
    {
        Name : string
        PropertyInfo : PropertyInfo
        Converter : FieldConverter
        Shape : TypeShape
        AllowDefaultValue : bool
        Serializer : PropertySerializerAttribute option
        Attributes : Attribute []
    }
with
    member rp.TryGetAttribute<'Attribute when 'Attribute :> Attribute> () = tryGetAttribute<'Attribute> rp.Attributes
    member rp.GetAttributes<'Attribute when 'Attribute :> Attribute> () = getAttributes<'Attribute> rp.Attributes
    member rp.ContainsAttribute<'Attribute when 'Attribute :> Attribute> () = containsAttribute<'Attribute> rp.Attributes