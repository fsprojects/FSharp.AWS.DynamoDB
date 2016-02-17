module internal FSharp.DynamoDB.FieldConverter.RecordConverter

open System
open System.Collections
open System.Collections.Generic
open System.IO
open System.Reflection

open Microsoft.FSharp.Reflection

open Amazon.Util
open Amazon.DynamoDBv2.Model

open FSharp.DynamoDB
open FSharp.DynamoDB.FieldConverter
open FSharp.DynamoDB.FieldConverter.ValueConverters

type RecordProperty =
    {
        Name : string
        PropertyInfo : PropertyInfo
        Converter : FieldConverter
        NoDefaultValue : bool
    }
with
    static member FromPropertyInfo (resolver : IFieldConverterResolver) (prop : PropertyInfo) =
        let attributes = prop.GetAttributes()
        let converter = 
            match tryGetAttribute<PropertySerializerAttribute> attributes with
            | Some serializer -> new SerializerConverter(prop, serializer, resolver) :> FieldConverter
            | None -> resolver.Resolve prop.PropertyType

        let name =
            match attributes |> tryGetAttribute<CustomNameAttribute> with
            | Some cn -> cn.Name
            | None -> prop.Name

        let noDefaultValue = containsAttribute<NoDefaultValueAttribute> attributes

        {
            Name = name
            PropertyInfo = prop
            Converter = converter
            NoDefaultValue = noDefaultValue
        }

type RecordConverter<'T>(ctor : obj[] -> 'T, properties : RecordProperty []) =
    inherit FieldConverter<'T> ()

    member __.Properties = properties
    member __.OfRecord (value : 'T) : RestObject =
        let values = new RestObject()
        for prop in properties do
            let field = prop.PropertyInfo.GetValue value
            let av = prop.Converter.OfFieldUntyped field
            values.Add(prop.Name, av)

        values

    member __.ToRecord (ro : RestObject) : 'T =
        let values = Array.zeroCreate<obj> properties.Length
        for i = 0 to properties.Length - 1 do
            let prop = properties.[i]
            let notFound() = raise <| new KeyNotFoundException(sprintf "attribute %A not found." prop.Name)
            let ok, av = ro.TryGetValue prop.Name
            if ok then values.[i] <- prop.Converter.ToFieldUntyped av
            elif prop.NoDefaultValue then notFound()
            else values.[i] <- prop.Converter.DefaultValueUntyped

        ctor values

    override __.ConverterType = ConverterType.Record
    override __.Representation = FieldRepresentation.Map
    override __.DefaultValue = invalidOp <| sprintf "default values not supported for records."

    override __.OfField (record : 'T) =
        let ro = __.OfRecord record in AttributeValue(M = ro)

    override __.ToField a =
        if a.IsMSet then __.ToRecord a.M
        else invalidCast a

let mkTupleConverter<'T> (resolver : IFieldConverterResolver) =
    let ctor, rest = FSharpValue.PreComputeTupleConstructorInfo(typeof<'T>)
    if Option.isSome rest then invalidArg (string typeof<'T>) "Tuples of arity > 7 not supported"
    let properties = typeof<'T>.GetProperties() |> Array.map (RecordProperty.FromPropertyInfo resolver)
    let mkRecord values = ctor.Invoke values :?> 'T
    new RecordConverter<'T>(mkRecord, properties)

let mkFSharpRecordConverter<'T> (resolver : IFieldConverterResolver) =
    let ctor = FSharpValue.PreComputeRecordConstructorInfo(typeof<'T>, true)
    let properties = FSharpType.GetRecordFields(typeof<'T>, true) |> Array.map (RecordProperty.FromPropertyInfo resolver)
    let mkRecord values = ctor.Invoke values :?> 'T
    new RecordConverter<'T>(mkRecord, properties)