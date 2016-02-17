module internal FSharp.DynamoDB.FieldConverter.ValueConverters

open System
open System.Collections
open System.Collections.Generic
open System.IO
open System.Reflection

open Microsoft.FSharp.Core.LanguagePrimitives

open Amazon.Util
open Amazon.DynamoDBv2.Model

open FSharp.DynamoDB
open FSharp.DynamoDB.FieldConverter

type AttributeValue with
    member inline av.IsSSSet = av.SS.Count > 0
    member inline av.IsNSSet = av.NS.Count > 0
    member inline av.IsBSSet = av.BS.Count > 0

    member av.Print() =
        if av.NULL then "{ NULL = true }"
        elif av.IsBOOLSet then sprintf "{ BOOL = %b }" av.BOOL
        elif av.S <> null then sprintf "{ S = %s }" av.S
        elif av.N <> null then sprintf "{ N = %s }" av.N
        elif av.B <> null then sprintf "{ N = %A }" (av.B.ToArray())
        elif av.SS.Count > 0 then sprintf "{ SS = %A }" (Seq.toArray av.SS)
        elif av.NS.Count > 0 then sprintf "{ SN = %A }" (Seq.toArray av.NS)
        elif av.BS.Count > 0 then 
            av.BS 
            |> Seq.map (fun bs -> bs.ToArray()) 
            |> Seq.toArray
            |> sprintf "{ BS = %A }"

        elif av.IsLSet then 
            av.L |> Seq.map (fun av -> av.Print()) |> Seq.toArray |> sprintf "{ L = %A }"

        elif av.IsMSet then 
            av.M 
            |> Seq.map (fun kv -> (kv.Key, kv.Value.Print())) 
            |> Seq.toArray
            |> sprintf "{ M = %A }"

        else
            "{ }"

let inline invalidCast (av:AttributeValue) : 'T = 
    raise <| new InvalidCastException(sprintf "could not convert value %A to type '%O'" (av.Print()) typeof<'T>)

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
    override __.OfField b = AttributeValue(BOOL = b)
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
        member __.OfField num = AttributeValue(N = string num)
        member __.ToField a = 
            if not <| isNull a.N then parseNum a.N 
            else invalidCast a
    }

type BytesConverter() =
    inherit FieldConverter<byte[]> ()
    override __.Representation = FieldRepresentation.Bytes
    override __.ConverterType = ConverterType.Value

    override __.DefaultValue = null
    override __.OfField bs = 
        if isNull bs then AttributeValue(NULL = true)
        else AttributeValue(B = new MemoryStream(bs))

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
    override __.OfField g = AttributeValue(string g)
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

    override __.OfField d = AttributeValue(unparse d)
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
    override __.OfField t = AttributeValue(N = string t.Ticks)
    override __.ToField a = 
        if not <| isNull a.N then TimeSpan.FromTicks(int64 a.N) 
        else invalidCast a

type EnumerationConverter<'E, 'U when 'E : enum<'U>>(uconv : StringRepresentableFieldConverter<'U>) =
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
    override __.OfField n = if n.HasValue then tconv.OfField n.Value else AttributeValue(NULL = true)
    override __.ToField a = if a.NULL then Nullable<'T> () else new Nullable<'T>(tconv.ToField a)

type OptionConverter<'T>(tconv : FieldConverter<'T>) =
    inherit FieldConverter<'T option> ()
    override __.Representation = tconv.Representation
    override __.ConverterType = ConverterType.Wrapper
    override __.DefaultValue = None
    override __.OfField topt = match topt with None -> AttributeValue(NULL = true) | Some t -> tconv.OfField t
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

type ListConverter<'List, 'T when 'List :> seq<'T>>(ctor : seq<'T> -> 'List, tconv : FieldConverter<'T>) =
    inherit FieldConverter<'List>()
    override __.Representation = FieldRepresentation.List
    override __.ConverterType = ConverterType.Value
    override __.DefaultValue = ctor [||]
    override __.OfField list = 
        if isNull list then AttributeValue(NULL = true)
        else 
            AttributeValue(L = (list |> Seq.map tconv.OfField |> rlist))

    override __.ToField a =
        if a.NULL then ctor [||]
        elif a.IsLSet then a.L |> Seq.map tconv.ToField |> ctor
        else invalidCast a

type BytesSetConverter<'BSet when 'BSet :> seq<byte[]>>(ctor : seq<byte []> -> 'BSet) =
    inherit FieldConverter<'BSet>()
    override __.Representation = FieldRepresentation.BytesSet
    override __.ConverterType = ConverterType.Value
    override __.DefaultValue = ctor [||]
    override __.OfField bss = 
        if isNull bss then AttributeValue(NULL = true)
        else AttributeValue (BS = (bss |> Seq.map (fun bs -> new MemoryStream(bs)) |> rlist))

    override __.ToField a =
        if a.NULL then ctor [||]
        elif a.IsBSSet then a.BS |> Seq.map (fun ms -> ms.ToArray()) |> ctor
        else invalidCast a

type NumSetConverter<'Set, 'T when 'Set :> seq<'T>> (ctor : seq<'T> -> 'Set, tconv : NumRepresentableFieldConverter<'T>) =
    inherit FieldConverter<'Set>()
    override __.DefaultValue = ctor [||]
    override __.Representation = FieldRepresentation.NumberSet
    override __.ConverterType = ConverterType.Value
    override __.OfField set = 
        if isNull set then AttributeValue(NULL = true)
        else
            AttributeValue(NS = (set |> Seq.map tconv.UnParse |> rlist))

    override __.ToField a =
        if a.NULL then ctor [||]
        elif a.IsNSSet then a.NS |> Seq.map tconv.Parse |> ctor
        else invalidCast a

type StringSetConverter<'Set, 'T when 'Set :> seq<'T>> (ctor : seq<'T> -> 'Set, tconv : StringRepresentableFieldConverter<'T>) =
    inherit FieldConverter<'Set>()
    override __.DefaultValue = ctor [||]
    override __.Representation = FieldRepresentation.StringSet
    override __.ConverterType = ConverterType.Value
    override __.OfField set = 
        if isNull set then AttributeValue(NULL = true)
        else AttributeValue(SS = (set |> Seq.map tconv.UnParse |> rlist))

    override __.ToField a =
        if a.NULL then ctor [||]
        elif a.IsSSSet then a.SS |> Seq.map tconv.Parse |> ctor
        else invalidCast a

let mkSetConverter<'List, 'T when 'List :> seq<'T>> ctor (tconv : FieldConverter<'T>) : FieldConverter<'List> =
    match tconv with
    | :? NumRepresentableFieldConverter<'T> as tc -> NumSetConverter<'List, 'T>(ctor, tc) :> _
    | :? StringRepresentableFieldConverter<'T> as tc -> StringSetConverter<'List, 'T>(ctor, tc) :> _
    | _ -> UnSupportedField.Raise typeof<'List>

type MapConverter<'Map, 'Key, 'Value when 'Map :> seq<KeyValuePair<'Key, 'Value>>>
                    (ctor : seq<KeyValuePair<'Key, 'Value>> -> 'Map, 
                        kconv : StringRepresentableFieldConverter<'Key>,
                        vconv : FieldConverter<'Value>) =

    inherit FieldConverter<'Map>()
    override __.Representation = FieldRepresentation.Map
    override __.ConverterType = ConverterType.Value
    override __.DefaultValue = ctor [||]
    override __.OfField map =
        if isNull map then AttributeValue(NULL = true)
        else
            let m = map |> Seq.map (fun kv -> keyVal (kconv.UnParse kv.Key) (vconv.OfField kv.Value)) |> cdict
            AttributeValue(M = m)

    override __.ToField a =
        if a.NULL then ctor [||]
        elif a.IsMSet then a.M |> Seq.map (fun kv -> keyVal (kconv.Parse kv.Key) (vconv.ToField kv.Value)) |> ctor
        else invalidCast a

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