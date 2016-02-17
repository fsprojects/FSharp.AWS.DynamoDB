module internal FSharp.DynamoDB.FieldConverter.RecordConverter

open System
open System.Collections
open System.Collections.Generic
open System.IO
open System.Reflection

open Microsoft.FSharp.Reflection

open Amazon.Util

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
    static member FromPropertyInfo(prop : PropertyInfo, resolver : IFieldConverterResolver) =
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

type RecordConverter<'T>(ctor : obj[] -> 'T) =
    inherit FieldConverter<'T> ()

    abstract Properties : RecordProperty []

    member __.OfRecord (value : 'T) : KeyValuePair<string, FsAttributeValue>[] =
        let props = __.Properties
        let values = new ResizeArray<_>()
        for prop in props do
            let field = prop.PropertyInfo.GetValue value
            match prop.Converter.OfFieldUntyped field with
            | Undefined -> ()
            | av -> values.Add <| KeyValuePair(prop.Name, av)

        values.ToArray()

    member __.ToRecord (attrs : KeyValuePair<string, FsAttributeValue>[]) =
        let dict = cdict attrs
        let props = __.Properties
        let values = Array.zeroCreate<obj> props.Length
        for i = 0 to props.Length - 1 do
            let prop = props.[i]
            let notFound() = raise <| new KeyNotFoundException(sprintf "attribute %A not found." prop.Name)
            let ok, av = dict.TryGetValue prop.Name
            if ok then
                match av with
                | Undefined when prop.NoDefaultValue -> notFound()
                | Undefined -> values.[i] <- prop.Converter.DefaultValueUntyped
                | av -> values.[i] <- prop.Converter.ToFieldUntyped av


//    let ctor = FSharpValue.PreComputeRecordConstructorInfo(typeof<'T>, true)
//    let properties = 
//        FSharpType.GetRecordFields(typeof<'T>, true) 
//        |> Array.map (fun p -> RecordProperty.Define(p, resolver))
//
//    override __.WriteRecord value =
//        let dict = new Dictionary<_,_>()
//        for prop in properties do
//            let field = prop.PropertyInfo.GetValue(value)
//            let av = prop.Converter.WriteUntyped field
//            dict.Add(prop.Name, av)
//
//        { Map = dict }
//
//    override __.ReadRecord ro =
//        let dict = ro.Map
//        let values = Array.zeroCreate<obj> properties.Length
//        for i = 0 to properties.Length - 1 do
//            let prop = properties.[i]
//            let ok, av' = dict.TryGetValue prop.Name
//            if ok then
//                values.[i] <- prop.Converter.ReadUntyped av'
//            elif prop.AllowDefaultValue then
//                values.[i] <- prop.Converter.DefaultValueUntyped
//            else
//                raise <| new KeyNotFoundException(sprintf "attribute %A not found." prop.Name)
//
//        ctor.Invoke values :?> 'T