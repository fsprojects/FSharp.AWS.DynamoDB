[<AutoOpen>]
module internal FSharp.DynamoDB.FieldConverter.PrimitiveConverters

open System
open System.Collections
open System.Collections.Generic
open System.IO
open System.Reflection

open Microsoft.FSharp.Core.LanguagePrimitives

open Amazon.Util
open Amazon.DynamoDBv2.Model

open FSharp.DynamoDB

type BoolConverter() =
    inherit StringRepresentableFieldConverter<bool>()
    override __.Representation = FieldRepresentation.Bool
    override __.ConverterType = ConverterType.Value

    override __.DefaultValue = false
    override __.OfField b = AttributeValue(BOOL = b) |> Some
    override __.ToField a =
        if a.IsBOOLSet then a.BOOL
        else invalidCast a

    override __.Parse s = Boolean.Parse s
    override __.UnParse s = string s

type StringConverter() =
    inherit StringRepresentableFieldConverter<string> ()
    override __.Representation = FieldRepresentation.String
    override __.ConverterType = ConverterType.Value

    override __.DefaultValue = null
    override __.OfField s =
        if isNull s then AttributeValue(NULL = true)
        else AttributeValue(s)
        |> Some

    override __.ToField a =
        if a.NULL then null
        elif not <| isNull a.S then a.S
        else invalidCast a

    override __.Parse s = s
    override __.UnParse s = s

let inline mkNumericalConverter< ^N when ^N : (static member Parse : string -> ^N)> () =
    let inline parseNum x = ( ^N : (static member Parse : string -> ^N) x)
    { new NumRepresentableFieldConverter< ^N>() with
        member __.Representation = FieldRepresentation.Number
        member __.ConverterType = ConverterType.Value

        member __.Parse s = parseNum s
        member __.UnParse e = string e

        member __.DefaultValue = Unchecked.defaultof< ^N>
        member __.OfField num = AttributeValue(N = string num) |> Some
        member __.ToField a = 
            if not <| isNull a.N then parseNum a.N 
            else invalidCast a
    }

type BytesConverter() =
    inherit FieldConverter<byte[]> ()
    override __.Representation = FieldRepresentation.Bytes
    override __.ConverterType = ConverterType.Value

    override __.DefaultValue = [||]
    override __.OfField bs = 
        if isNull bs then Some <| AttributeValue(NULL = true)
        elif bs.Length = 0 then None 
        else Some <| AttributeValue(B = new MemoryStream(bs))

    override __.ToField a = 
        if a.NULL then null
        elif not <| isNull a.B then a.B.ToArray()
        else
            invalidCast a

type GuidConverter() =
    inherit StringRepresentableFieldConverter<Guid> ()
    override __.Representation = FieldRepresentation.String
    override __.ConverterType = ConverterType.Value

    override __.DefaultValue = Guid.Empty
    override __.OfField g = AttributeValue(string g) |> Some
    override __.ToField a =
        if not <| isNull a.S then Guid.Parse a.S
        else invalidCast a

    override __.Parse s = Guid.Parse s
    override __.UnParse g = string g

type DateTimeOffsetConverter() =
    inherit StringRepresentableFieldConverter<DateTimeOffset> ()
    let parse s = DateTimeOffset.Parse(s).ToLocalTime()
    let unparse (d:DateTimeOffset) = d.ToUniversalTime().ToString(AWSSDKUtils.ISO8601DateFormat)

    override __.Representation = FieldRepresentation.String
    override __.ConverterType = ConverterType.Value

    override __.DefaultValue = DateTimeOffset()
    override __.Parse s = parse s
    override __.UnParse d = unparse d

    override __.OfField d = AttributeValue(unparse d) |> Some
    override __.ToField a = 
        if not <| isNull a.S then parse a.S 
        else invalidCast a

type TimeSpanConverter() =
    inherit NumRepresentableFieldConverter<TimeSpan> ()
    override __.Representation = FieldRepresentation.String
    override __.ConverterType = ConverterType.Value

    override __.Parse s = TimeSpan.FromTicks(int64 s)
    override __.UnParse t = string t.Ticks
    override __.DefaultValue = TimeSpan.Zero
    override __.OfField t = AttributeValue(N = string t.Ticks) |> Some
    override __.ToField a = 
        if not <| isNull a.N then TimeSpan.FromTicks(int64 a.N) 
        else invalidCast a

type EnumerationConverter<'E, 'U when 'E : enum<'U>>(uconv : NumRepresentableFieldConverter<'U>) =
    inherit NumRepresentableFieldConverter<'E> ()
    override __.Representation = FieldRepresentation.Number
    override __.ConverterType = ConverterType.Value

    override __.DefaultValue = Unchecked.defaultof<'E>
    override __.OfField e = let u = EnumToValue<'E,'U> e in uconv.OfField u
    override __.ToField a = EnumOfValue<'U, 'E>(uconv.ToField a)
    override __.Parse s = uconv.Parse s |> EnumOfValue<'U, 'E>
    override __.UnParse e = EnumToValue<'E, 'U> e |> uconv.UnParse

type NullableConverter<'T when 'T : (new : unit -> 'T) and 'T :> ValueType and 'T : struct>(tconv : FieldConverter<'T>) =
    inherit FieldConverter<Nullable<'T>> ()
    override __.Representation = tconv.Representation
    override __.ConverterType = ConverterType.Wrapper
    override __.DefaultValue = Nullable<'T>()
    override __.OfField n = if n.HasValue then tconv.OfField n.Value else AttributeValue(NULL = true) |> Some
    override __.ToField a = if a.NULL then Nullable<'T> () else new Nullable<'T>(tconv.ToField a)

type OptionConverter<'T>(tconv : FieldConverter<'T>) =
    inherit FieldConverter<'T option> ()
    override __.Representation = tconv.Representation
    override __.ConverterType = ConverterType.Wrapper
    override __.DefaultValue = None
    override __.OfField topt = match topt with None -> None | Some t -> tconv.OfField t
    override __.ToField a = if a.NULL then None else Some(tconv.ToField a)

let mkFSharpRefConverter<'T> (tconv : FieldConverter<'T>) =
    match tconv with
    | :? NumRepresentableFieldConverter<'T> as tconv ->
        { new NumRepresentableFieldConverter<'T ref>() with
            member __.Representation = tconv.Representation
            member __.ConverterType = tconv.ConverterType
            member __.DefaultValue = ref tconv.DefaultValue
            member __.OfField tref = tconv.OfField tref.Value
            member __.ToField a = tconv.ToField a |> ref
            member __.Parse s = tconv.Parse s |> ref
            member __.UnParse tref = tconv.UnParse tref.Value
        } :> FieldConverter<'T ref>

    | :? StringRepresentableFieldConverter<'T> as tconv ->
        { new StringRepresentableFieldConverter<'T ref>() with
            member __.Representation = tconv.Representation
            member __.ConverterType = tconv.ConverterType
            member __.DefaultValue = ref tconv.DefaultValue
            member __.OfField tref = tconv.OfField tref.Value
            member __.ToField a = tconv.ToField a |> ref
            member __.Parse s = tconv.Parse s |> ref
            member __.UnParse tref = tconv.UnParse tref.Value
        } :> FieldConverter<'T ref>

    | _ ->
        { new FieldConverter<'T ref>() with
            member __.Representation = tconv.Representation
            member __.ConverterType = tconv.ConverterType
            member __.DefaultValue = ref tconv.DefaultValue
            member __.OfField tref = tconv.OfField tref.Value
            member __.ToField a = tconv.ToField a |> ref }


type SerializerConverter(propertyInfo : PropertyInfo, serializer : PropertySerializerAttribute, resolver : IFieldConverterResolver) =
    inherit FieldConverter()

    let converter = resolver.Resolve serializer.PickleType

    override __.Type = propertyInfo.PropertyType
    override __.Representation = converter.Representation
    override __.ConverterType = ConverterType.Serialized
    override __.DefaultValueUntyped = 
        raise <| NotSupportedException("Default values not supported in serialized types.")

    override __.OfFieldUntyped value = 
        let pickle = serializer.SerializeUntyped value
        converter.OfFieldUntyped pickle

    override __.ToFieldUntyped a =
        let pickle = converter.ToFieldUntyped a
        serializer.DeserializeUntyped pickle