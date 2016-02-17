module internal FSharp.DynamoDB.FieldConverter.ValueConverters

open System
open System.Collections
open System.Collections.Generic
open System.IO
open System.Reflection

open Microsoft.FSharp.Core.LanguagePrimitives

open Amazon.Util

open FSharp.DynamoDB
open FSharp.DynamoDB.FieldConverter

let inline invalidCast (fsa:FsAttributeValue) : 'T = 
    raise <| new InvalidCastException(sprintf "could not convert value %A to type '%O'" fsa typeof<'T>)

type UnSupportedField =
    static member Raise(fieldType : Type, ?reason : string) =
        let message = 
            match reason with
            | None -> sprintf "unsupported record field type '%O'" fieldType
            | Some r -> sprintf "unsupported record field type '%O': %s" fieldType r

        raise <| new ArgumentException(message)

[<AbstractClass>]
type StringRepresentableFieldConverter<'T>() =
    inherit FieldConverter<'T>()
    abstract Parse : string -> 'T
    abstract UnParse : 'T -> string

[<AbstractClass>]
type NumRepresentableFieldConverter<'T>() =
    inherit StringRepresentableFieldConverter<'T> ()

type BoolConverter() =
    inherit StringRepresentableFieldConverter<bool>()
    override __.Representation = FieldRepresentation.Bool
    override __.ConverterType = ConverterType.Value

    override __.DefaultValue = false
    override __.OfField b = Bool b
    override __.ToField a =
        match a with 
        | Bool b -> b
        | _ -> invalidCast a

    override __.Parse s = Boolean.Parse s
    override __.UnParse s = string s

type StringConverter() =
    inherit StringRepresentableFieldConverter<string> ()
    override __.Representation = FieldRepresentation.String
    override __.ConverterType = ConverterType.Value

    override __.DefaultValue = null
    override __.OfField s = String s
    override __.ToField a =
        match a with
        | Null -> null
        | String s -> s
        | _ -> invalidCast a

    override __.Parse s = s
    override __.UnParse s = s

type BytesConverter() =
    inherit FieldConverter<byte[]> ()
    override __.Representation = FieldRepresentation.Bytes
    override __.ConverterType = ConverterType.Value

    override __.DefaultValue = null
    override __.OfField bs = Bytes bs
    override __.ToField a = 
        match a with 
        | Null -> null
        | Bytes bs -> bs
        | _ -> invalidCast a

type GuidConverter() =
    inherit StringRepresentableFieldConverter<Guid> ()
    override __.Representation = FieldRepresentation.String
    override __.ConverterType = ConverterType.Value

    override __.DefaultValue = Guid.Empty
    override __.OfField g = String(string g)
    override __.ToField a =
        match a with
        | String s -> Guid.Parse s
        | _ -> invalidCast a

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

    override __.OfField d = String(unparse d)
    override __.ToField a = match a with String s -> parse s | _ -> invalidCast a

type TimeSpanConverter() =
    inherit NumRepresentableFieldConverter<TimeSpan> ()
    override __.Representation = FieldRepresentation.String
    override __.ConverterType = ConverterType.Value

    override __.Parse s = TimeSpan.FromTicks(int64 s)
    override __.UnParse t = string t.Ticks
    override __.DefaultValue = TimeSpan.Zero
    override __.OfField t = Number(string t.Ticks)
    override __.ToField a = match a with Number n -> TimeSpan.FromTicks(int64 n) | _ -> invalidCast a

let inline mkNumericalConverter< ^N when ^N : (static member Parse : string -> ^N)> () =
    let inline parseNum x = ( ^N : (static member Parse : string -> ^N) x)
    { new NumRepresentableFieldConverter< ^N>() with
        member __.Representation = FieldRepresentation.Number
        member __.ConverterType = ConverterType.Value

        member __.Parse s = parseNum s
        member __.UnParse e = string e

        member __.DefaultValue = Unchecked.defaultof< ^N>
        member __.OfField num = Number(string num)
        member __.ToField a = match a with Number n -> parseNum n | _ -> invalidCast a
    }

type EnumerationConverter<'E, 'U when 'E : enum<'U>>(uconv : StringRepresentableFieldConverter<'U>) =
    inherit NumRepresentableFieldConverter<'E> ()
    override __.Representation = FieldRepresentation.Number
    override __.ConverterType = ConverterType.Value

    override __.DefaultValue = Unchecked.defaultof<'E>
    override __.OfField e = let u = EnumToValue<'E,'U> e in Number(u.ToString())
    override __.ToField a = EnumOfValue<'U, 'E>(uconv.ToField a)
    override __.Parse s = uconv.Parse s |> EnumOfValue<'U, 'E>
    override __.UnParse e = EnumToValue<'E, 'U> e |> uconv.UnParse

type NullableConverter<'T when 'T : (new : unit -> 'T) and 'T :> ValueType and 'T : struct>(tconv : FieldConverter<'T>) =
    inherit FieldConverter<Nullable<'T>> ()
    override __.Representation = tconv.Representation
    override __.ConverterType = ConverterType.Optional
    override __.DefaultValue = Nullable<'T>()
    override __.OfField n = if n.HasValue then tconv.OfField n.Value else Undefined
    override __.ToField a =
        match a with
        | Undefined | Null -> new Nullable<'T>()
        | a -> new Nullable<'T>(tconv.ToField a)

type OptionConverter<'T>(tconv : FieldConverter<'T>) =
    inherit FieldConverter<'T option> ()
    override __.Representation = tconv.Representation
    override __.ConverterType = ConverterType.Optional
    override __.DefaultValue = None
    override __.OfField topt = match topt with None -> Undefined | Some t -> tconv.OfField t
    override __.ToField a =
        match a with
        | Undefined | Null -> None
        | _ -> Some(tconv.ToField a)

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

type ListConverter<'List, 'T when 'List :> seq<'T>>(ctor : seq<'T> -> 'List, tconv : FieldConverter<'T>) =
    inherit FieldConverter<'List>()
    override __.Representation = FieldRepresentation.List
    override __.ConverterType = ConverterType.Value
    override __.DefaultValue = ctor [||]
    override __.OfField list = list |> Seq.map tconv.OfFieldUntyped |> Seq.toArray |> List
    override __.ToField a =
        match a with
        | Null -> ctor [||]
        | List ts -> ts |> Seq.map tconv.ToField |> ctor
        | _ -> invalidCast a

type BytesSetConverter<'BSet when 'BSet :> seq<byte[]>>(ctor : seq<byte []> -> 'BSet) =
    inherit FieldConverter<'BSet>()
    override __.Representation = FieldRepresentation.BytesSet
    override __.ConverterType = ConverterType.Value
    override __.DefaultValue = ctor [||]
    override __.OfField bss = BytesSet (Seq.toArray bss)
    override __.ToField a =
        match a with
        | Null -> ctor [||]
        | BytesSet ns -> ctor ns
        | _ -> invalidCast a

type NumSetConverter<'Set, 'T when 'Set :> seq<'T>> (ctor : seq<'T> -> 'Set, tconv : NumRepresentableFieldConverter<'T>) =
    inherit FieldConverter<'Set>()
    override __.DefaultValue = ctor [||]
    override __.Representation = FieldRepresentation.NumberSet
    override __.ConverterType = ConverterType.Value
    override __.OfField set = set |> Seq.map tconv.UnParse |> Seq.toArray |> NumberSet
    override __.ToField a =
        match a with
        | Null -> ctor [||]
        | NumberSet es -> es |> Seq.map tconv.Parse |> ctor
        | _ -> invalidCast a

type StringSetConverter<'Set, 'T when 'Set :> seq<'T>> (ctor : seq<'T> -> 'Set, tconv : StringRepresentableFieldConverter<'T>) =
    inherit FieldConverter<'Set>()
    override __.DefaultValue = ctor [||]
    override __.Representation = FieldRepresentation.StringSet
    override __.ConverterType = ConverterType.Value
    override __.OfField set = set |> Seq.map tconv.UnParse |> Seq.toArray |> StringSet
    override __.ToField a =
        match a with
        | Null -> ctor [||]
        | StringSet es -> es |> Seq.map tconv.Parse |> ctor
        | _ -> invalidCast a

let mkSetConverter<'List, 'T when 'List :> seq<'T>> ctor (tconv : FieldConverter<'T>) : FieldConverter<'List> =
    if tconv.IsOptional then UnSupportedField.Raise typeof<'List>
    match tconv with
    | :? NumRepresentableFieldConverter<'T> as tc -> NumSetConverter<'List, 'T>(ctor, tc) :> _
    | :? StringRepresentableFieldConverter<'T> as tc -> StringSetConverter<'List, 'T>(ctor, tc) :> _
    | _ -> UnSupportedField.Raise typeof<'List>

type MapConverter<'Map, 'Key, 'Value when 'Map :> seq<KeyValuePair<'Key, 'Value>>>
                    (ctor : seq<KeyValuePair<'Key, 'Value>> -> 'Map, 
                        kconv : StringRepresentableFieldConverter<'Key>,
                        vconv : FieldConverter<'Value>) =

    inherit FieldConverter<'Map>()
    do if vconv.IsOptional then UnSupportedField.Raise typeof<'Map>
    override __.Representation = FieldRepresentation.Map
    override __.ConverterType = ConverterType.Value
    override __.DefaultValue = ctor [||]
    override __.OfField map = 
        map 
        |> Seq.map (fun kv -> KeyValuePair(kconv.UnParse kv.Key, vconv.OfField kv.Value)) 
        |> Seq.toArray
        |> Map

    override __.ToField a =
        match a with
        | Null -> ctor [||]
        | Map attrs -> attrs |> Seq.map (fun kv -> KeyValuePair(kconv.Parse kv.Key, vconv.ToField kv.Value)) |> ctor
        | _ -> invalidCast a

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
        converter.OfFieldUntyped value

    override __.ToFieldUntyped a =
        let pickle = converter.ToFieldUntyped a
        serializer.DeserializeUntyped pickle