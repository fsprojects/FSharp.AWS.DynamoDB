module internal FSharp.DynamoDB.FieldConverter

open System
open System.Collections
open System.Collections.Generic
open System.IO
open System.Reflection

open Microsoft.FSharp.Core.LanguagePrimitives

open Amazon.Util
open Amazon.DynamoDBv2.Model

open FSharp.DynamoDB.TypeShape

type UnSupportedField =
    static member Raise(fieldType : Type, ?reason : string) =
        let message = 
            match reason with
            | None -> sprintf "unsupported record field type '%O'" fieldType
            | Some r -> sprintf "unsupported record field type '%O': %s" fieldType r

        raise <| new ArgumentException(message)

type FieldRepresentation =
    | Number        = 01
    | String        = 02
    | Bool          = 03
    | Bytes         = 04
    | Strings       = 05
    | Numbers       = 06
    | Bytess        = 07
    | List          = 08
    | Map           = 09
    | Serializer    = 10

type FsAttributeValue =
    | Undefined
    | Null
    | Bool of bool
    | String of string
    | Number of string
    | Bytes of byte[]
    | Strings of string[]
    | Numbers of string[]
    | Bytess of byte[][]
    | List of FsAttributeValue[]
    | Map of KeyValuePair<string, FsAttributeValue>[]
with
    static member FromAttributeValue(av : AttributeValue) =
        if av.NULL then Null
        elif av.IsBOOLSet then Bool av.BOOL
        elif av.S <> null then String av.S
        elif av.N <> null then Number av.N
        elif av.B <> null then Bytes (av.B.ToArray())
        elif av.SS.Count > 0 then Strings (Seq.toArray av.SS)
        elif av.NS.Count > 0 then Numbers (Seq.toArray av.NS)
        elif av.BS.Count > 0 then av.BS |> Seq.map (fun bs -> bs.ToArray()) |> Seq.toArray |> Bytess
        elif av.IsLSet then av.L |> Seq.map FsAttributeValue.FromAttributeValue |> Seq.toArray |> List
        elif av.IsMSet then 
            av.M 
            |> Seq.map (fun kv -> KeyValuePair(kv.Key, FsAttributeValue.FromAttributeValue kv.Value)) 
            |> Seq.toArray
            |> Map

        else Undefined

    static member ToAttributeValue(fsav : FsAttributeValue) =
        match fsav with
        | Undefined -> invalidArg "fsav" "undefined attribute value."
        | Null -> AttributeValue(NULL = true)
        | Bool b -> AttributeValue(BOOL = b)
        | String null -> AttributeValue(NULL = true)
        | String s -> AttributeValue(s)
        | Number null -> invalidArg "fsav" "Number attribute contains null as value."
        | Number n -> AttributeValue(N = n)
        | Bytes null -> AttributeValue(NULL = true)
        | Bytes bs -> AttributeValue(B = new MemoryStream(bs))
        | Strings (null | [||]) -> AttributeValue(NULL = true)
        | Strings ss -> AttributeValue(rlist ss)
        | Numbers (null | [||]) -> AttributeValue(NULL = true)
        | Numbers ns -> AttributeValue(NS = rlist ns)
        | Bytess (null | [||]) -> AttributeValue(NULL = true)
        | Bytess bss -> AttributeValue(BS = (bss |> Seq.map (fun bs -> new MemoryStream(bs)) |> rlist))
        | List (null | [||]) -> AttributeValue(NULL = true)
        | List attrs -> AttributeValue(L = (attrs |> Seq.map FsAttributeValue.ToAttributeValue |> rlist))
        | Map (null | [||]) -> AttributeValue(NULL = true)
        | Map attrs -> 
            AttributeValue(M =
                (attrs
                |> Seq.map (fun kv -> KeyValuePair(kv.Key, FsAttributeValue.ToAttributeValue kv.Value)) 
                |> cdict))

let inline private invalidCast (fsa:FsAttributeValue) : 'T = 
    raise <| new InvalidCastException(sprintf "could not convert value %A to type '%O'" fsa typeof<'T>)

type AttributeValue with
    member inline av.IsEmpty =
        av.NULL = false &&
        av.IsBOOLSet = false &&
        av.S = null &&
        av.N = null &&
        av.B = null &&
        av.NS.Count = 0 &&
        av.BS.Count = 0 &&
        av.SS.Count = 0 &&
        av.IsLSet = false &&
        av.IsMSet = false

let isKeyRepr repr =
    match repr with
    | FieldRepresentation.Number
    | FieldRepresentation.String
    | FieldRepresentation.Bytes -> true
    | _ -> false

let isScalarRepr repr =
    match repr with
    | FieldRepresentation.Number
    | FieldRepresentation.String
    | FieldRepresentation.Bytes
    | FieldRepresentation.Bool -> true
    | _ -> false


[<AbstractClass>]
type FieldConverter() =
    abstract Type : Type
    abstract Representation : FieldRepresentation
    abstract DefaultValueUntyped : obj
    abstract IsOptionalType : bool
    default __.IsOptionalType = false
    abstract OfFieldUntyped : obj -> FsAttributeValue
    abstract ToFieldUntyped : FsAttributeValue -> obj

[<AbstractClass>]
type FieldConverter<'T>() =
    inherit FieldConverter()
    abstract DefaultValue : 'T
    abstract OfField : 'T -> FsAttributeValue
    abstract ToField : FsAttributeValue -> 'T

    override __.Type = typeof<'T>
    override __.DefaultValueUntyped = __.DefaultValue :> obj
    override __.OfFieldUntyped o = __.OfField(o :?> 'T)
    override __.ToFieldUntyped av = __.ToField av :> obj

[<AbstractClass>]
type StringRepresentableFieldConverter<'T>() =
    inherit FieldConverter<'T>()
    override __.Representation = FieldRepresentation.String
    abstract Parse : string -> 'T
    abstract UnParse : 'T -> string

[<AbstractClass>]
type NumRepresentableFieldConverter<'T>() =
    inherit StringRepresentableFieldConverter<'T> ()
    override __.Representation = FieldRepresentation.Number

type BoolConverter() =
    inherit StringRepresentableFieldConverter<bool>()
    override __.DefaultValue = false
    override __.Representation = FieldRepresentation.Bool
    override __.OfField b = Bool b
    override __.ToField a =
        match a with 
        | Bool b -> b
        | _ -> invalidCast a

    override __.Parse s = Boolean.Parse s
    override __.UnParse s = string s

type StringConverter() =
    inherit StringRepresentableFieldConverter<string> ()
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
    override __.DefaultValue = null
    override __.OfField bs = Bytes bs
    override __.ToField a = 
        match a with 
        | Null -> null
        | Bytes bs -> bs
        | _ -> invalidCast a

type GuidConverter() =
    inherit StringRepresentableFieldConverter<Guid> ()
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
    override __.DefaultValue = DateTimeOffset()
    override __.OfField d = String(d.ToUniversalTime().ToString(AWSSDKUtils.ISO8601DateFormat))
    override __.ToField a = match a with String s -> DateTimeOffset.Parse(s).ToLocalTime() | _ -> invalidCast a
    override __.Parse s = DateTimeOffset.Parse(s).ToLocalTime()
    override __.UnParse d = d.ToUniversalTime().ToString(AWSSDKUtils.ISO8601DateFormat)

type TimeSpanConverter() =
    inherit NumRepresentableFieldConverter<TimeSpan> ()
    override __.DefaultValue = TimeSpan.Zero
    override __.OfField t = Number(string t.Ticks)
    override __.ToField a = match a with Number n -> TimeSpan.FromTicks(int64 n) | _ -> invalidCast a
    override __.Parse s = TimeSpan.FromTicks(int64 s)
    override __.UnParse t = string t.Ticks

let inline mkNumericalConverter< ^N when ^N : (static member Parse : string -> ^N)> () =
    let inline parseNum x = ( ^N : (static member Parse : string -> ^N) x)
    { new NumRepresentableFieldConverter< ^N>() with
        member __.DefaultValue = Unchecked.defaultof< ^N>
        member __.OfField num = Number(string num)
        member __.ToField a = match a with Number n -> parseNum n | _ -> invalidCast a
        member __.Parse s = parseNum s
        member __.UnParse e = string e
    }

type EnumerationConverter<'E, 'U when 'E : enum<'U>>(uconv : StringRepresentableFieldConverter<'U>) =
    inherit NumRepresentableFieldConverter<'E> ()
    override __.DefaultValue = Unchecked.defaultof<'E>
    override __.OfField e = let u = EnumToValue<'E,'U> e in Number(u.ToString())
    override __.ToField a = EnumOfValue<'U, 'E>(uconv.ToField a)
    override __.Parse s = uconv.Parse s |> EnumOfValue<'U, 'E>
    override __.UnParse e = EnumToValue<'E, 'U> e |> uconv.UnParse

type NullableConverter<'T when 'T : (new : unit -> 'T) and 'T :> ValueType and 'T : struct>(tconv : FieldConverter<'T>) =
    inherit FieldConverter<Nullable<'T>> ()
    override __.Representation = tconv.Representation
    override __.IsOptionalType = true
    override __.DefaultValue = Nullable<'T>()
    override __.OfField n = if n.HasValue then tconv.OfField n.Value else Undefined
    override __.ToField a =
        match a with
        | Undefined | Null -> new Nullable<'T>()
        | a -> new Nullable<'T>(tconv.ToField a)

type OptionConverter<'T>(tconv : FieldConverter<'T>) =
    inherit FieldConverter<'T option> ()
    override __.Representation = tconv.Representation
    override __.IsOptionalType = true
    override __.DefaultValue = None
    override __.OfField topt = match topt with None -> Undefined | Some t -> tconv.OfField t
    override __.ToField a =
        match a with
        | Undefined | Null -> None
        | _ -> Some(tconv.ToField a)

let mkFSharpRefConverter (tconv : FieldConverter<'T>) =
    match tconv with
    | :? NumRepresentableFieldConverter<'T> as tconv ->
        { new NumRepresentableFieldConverter<'T ref>() with
            member __.DefaultValue = ref tconv.DefaultValue
            member __.OfField tref = tconv.OfField tref.Value
            member __.ToField a = tconv.ToField a |> ref
            member __.Parse s = tconv.Parse s |> ref
            member __.UnParse tref = tconv.UnParse tref.Value
        } :> FieldConverter<'T ref>

    | :? StringRepresentableFieldConverter<'T> as tconv ->
        { new StringRepresentableFieldConverter<'T ref>() with
            member __.DefaultValue = ref tconv.DefaultValue
            member __.OfField tref = tconv.OfField tref.Value
            member __.ToField a = tconv.ToField a |> ref
            member __.Parse s = tconv.Parse s |> ref
            member __.UnParse tref = tconv.UnParse tref.Value
        } :> FieldConverter<'T ref>

    | _ ->
        { new FieldConverter<'T ref>() with
            member __.DefaultValue = ref tconv.DefaultValue
            member __.Representation = tconv.Representation
            member __.OfField tref = tconv.OfField tref.Value
            member __.ToField a = tconv.ToField a |> ref }

type BytesListConverter<'BSeq when 'BSeq :> seq<byte[]>>(ctor : seq<byte []> -> 'BSeq) =
    inherit FieldConverter<'BSeq>()
    override __.Representation = FieldRepresentation.Bytess
    override __.DefaultValue = ctor [||]
    override __.OfField bss = Bytess (Seq.toArray bss)
    override __.ToField a =
        match a with
        | Null -> ctor [||]
        | Bytess ns -> ctor ns
        | _ -> invalidCast a

type NumListConverter<'List, 'T when 'List :> seq<'T>> (ctor : seq<'T> -> 'List, tconv : NumRepresentableFieldConverter<'T>) =
    inherit FieldConverter<'List>()
    override __.DefaultValue = ctor [||]
    override __.Representation = FieldRepresentation.Numbers
    override __.OfField set = set |> Seq.map tconv.UnParse |> Seq.toArray |> Numbers
    override __.ToField a =
        match a with
        | Null -> ctor [||]
        | Numbers es -> es |> Seq.map tconv.Parse |> ctor
        | _ -> invalidCast a

type StringListConverter<'List, 'T when 'List :> seq<'T>> (ctor : seq<'T> -> 'List, tconv : StringRepresentableFieldConverter<'T>) =
    inherit FieldConverter<'List>()
    override __.DefaultValue = ctor [||]
    override __.Representation = FieldRepresentation.Strings
    override __.OfField set = set |> Seq.map tconv.UnParse |> Seq.toArray |> Strings
    override __.ToField a =
        match a with
        | Null -> ctor [||]
        | Strings es -> es |> Seq.map tconv.Parse |> ctor
        | _ -> invalidCast a

type ListConverter<'List, 'T when 'List :> seq<'T>> (ctor : seq<'T> -> 'List, tconv : FieldConverter<'T>) =
    inherit FieldConverter<'List>()
    override __.DefaultValue = ctor [||]
    override __.Representation = FieldRepresentation.List
    override __.OfField set = 
        set |> Seq.map tconv.OfField |> Seq.toArray |> List

    override __.ToField a =
        match a with
        | Null -> ctor [||]
        | List es -> es |> Seq.map tconv.ToField |> ctor
        | _ -> invalidCast a

let mkListConverter ctor (tconv : FieldConverter<'T>) : FieldConverter<'List> =
    if tconv.IsOptionalType then UnSupportedField.Raise typeof<'List>
    match tconv with
    | :? NumRepresentableFieldConverter<'T> as tc -> NumListConverter<'List, 'T>(ctor, tc) :> _
    | :? StringRepresentableFieldConverter<'T> as tc -> StringListConverter<'List, 'T>(ctor, tc) :> _
    | _ -> new ListConverter<'List, 'T>(ctor, tconv) :> _

type MapConverter<'Map, 'Key, 'Value when 'Map :> seq<KeyValuePair<'Key, 'Value>>>
                    (ctor : seq<KeyValuePair<'Key, 'Value>> -> 'Map, 
                        kconv : StringRepresentableFieldConverter<'Key>,
                        vconv : FieldConverter<'Value>) =

    inherit FieldConverter<'Map>()
    do if vconv.IsOptionalType then UnSupportedField.Raise typeof<'Map>
    override __.Representation = FieldRepresentation.Map
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

let rec resolveConv<'T> () = resolveConvUntyped typeof<'T> :?> FieldConverter<'T>

and resolveSRConv<'T> () = 
    match resolveConv<'T> () with
    | :? StringRepresentableFieldConverter<'T> as sr -> sr
    | _ -> UnSupportedField.Raise typeof<'T>

and resolveConvUntyped (t : Type) : FieldConverter =
    match getShape t with
    | :? ShapeBool -> new BoolConverter() :> _
    | :? ShapeByte -> mkNumericalConverter<byte> () :> _
    | :? ShapeSByte -> mkNumericalConverter<sbyte> () :> _
    | :? ShapeInt16 -> mkNumericalConverter<int16> () :> _
    | :? ShapeInt32 -> mkNumericalConverter<int32> () :> _
    | :? ShapeInt64 -> mkNumericalConverter<int64> () :> _
    | :? ShapeUInt16 -> mkNumericalConverter<uint16> () :> _
    | :? ShapeUInt32 -> mkNumericalConverter<uint32> () :> _
    | :? ShapeUInt64 -> mkNumericalConverter<uint64> () :> _
    | :? ShapeSingle -> mkNumericalConverter<single> () :> _
    | :? ShapeDouble -> mkNumericalConverter<double> () :> _
    | :? ShapeDecimal -> mkNumericalConverter<decimal> () :> _
    | :? ShapeString -> new StringConverter() :> _
    | :? ShapeGuid -> new GuidConverter() :> _
    | :? ShapeTimeSpan -> new TimeSpanConverter() :> _
    | :? ShapeDateTime -> UnSupportedField.Raise(t, "please use DateTimeOffset instead.")
    | :? ShapeDateTimeOffset -> new DateTimeOffsetConverter() :> _
    | ShapeEnum s ->
        s.Accept {
            new IEnumVisitor<FieldConverter> with
                member __.VisitEnum<'E, 'U when 'E : enum<'U>> () =
                    new EnumerationConverter<'E, 'U>(resolveSRConv()) :> _ }

    | ShapeNullable s ->
        s.Accept {
            new INullableVisitor<FieldConverter> with
                member __.VisitNullable<'T when 'T : (new : unit -> 'T) and 'T :> ValueType and 'T : struct> () = 
                    new NullableConverter<'T>(resolveConv()) :> _ }

    | ShapeFSharpRef s ->
        s.Accept {
            new IFSharpRefVisitor<FieldConverter> with
                member __.VisitFSharpRef<'T> () =
                    mkFSharpRefConverter (resolveConv<'T>()) :> _ }

    | ShapeFSharpOption s ->
        s.Accept {
            new IFSharpOptionVisitor<FieldConverter> with
                member __.VisitFSharpOption<'T> () =
                    let tconv = resolveConv<'T>()
                    if tconv.IsOptionalType then UnSupportedField.Raise typeof<'T option>
                    new OptionConverter<'T>(tconv) :> _ }

    | ShapeArray s ->
        s.Accept { 
            new IArrayVisitor<FieldConverter> with
                member __.VisitArray<'T> () =
                    if typeof<'T> = typeof<byte> then
                        BytesConverter() :> _
                    elif typeof<'T> = typeof<byte []> then
                        BytesListConverter<byte [][]>(Array.ofSeq) :> _
                    else
                        mkListConverter Array.ofSeq (resolveConv<'T>()) :> _ }

    | ShapeFSharpList s ->
        s.Accept {
            new IFSharpListVisitor<FieldConverter> with
                member __.VisitFSharpList<'T> () =
                    if typeof<'T> = typeof<byte []> then
                        BytesListConverter<byte [] list>(List.ofSeq) :> _
                    else
                        mkListConverter List.ofSeq (resolveConv<'T>()) :> _ }

    | ShapeResizeArray s ->
        s.Accept {
            new IResizeArrayVisitor<FieldConverter> with
                member __.VisitResizeArray<'T> () =
                    if typeof<'T> = typeof<byte []> then
                        BytesListConverter<ResizeArray<byte []>>(rlist) :> _
                    else
                        mkListConverter rlist (resolveConv<'T>()) :> _ }

    | ShapeHashSet s ->
        s.Accept {
            new IHashSetVisitor<FieldConverter> with
                member __.VisitHashSet<'T when 'T : equality> () =
                    if typeof<'T> = typeof<byte []> then
                        BytesListConverter<HashSet<byte []>>(HashSet) :> _
                    else
                        mkListConverter HashSet (resolveConv<'T>()) :> _ }

    | ShapeFSharpSet s ->
        s.Accept {
            new IFSharpSetVisitor<FieldConverter> with
                member __.VisitFSharpSet<'T when 'T : comparison> () =
                    if typeof<'T> = typeof<byte []> then
                        BytesListConverter<Set<byte []>>(Set.ofSeq) :> _
                    else
                        mkListConverter Set.ofSeq (resolveConv<'T>()) :> _ }

    | ShapeDictionary s ->
        s.Accept {
            new IDictionaryVisitor<FieldConverter> with
                member __.VisitDictionary<'K, 'V when 'K : equality> () =
                    new MapConverter<Dictionary<'K, 'V>, 'K, 'V>(cdict, resolveSRConv(), resolveConv()) :> _ }

    | ShapeFSharpMap s ->
        s.Accept { 
            new IFSharpMapVisitor<FieldConverter> with
                member __.VisitFSharpMap<'K, 'V when 'K : comparison> () =
                    let mkMap (kvs : seq<KeyValuePair<'K,'V>>) =
                        kvs |> Seq.map (fun kv -> kv.Key, kv.Value) |> Map.ofSeq

                    new MapConverter<Map<'K,'V>, 'K, 'V>(mkMap, resolveSRConv(), resolveConv()) :> _ }

    | _ -> UnSupportedField.Raise t


type SerializerConverter(propertyInfo : PropertyInfo, serializer : PropertySerializerAttribute) =
    inherit FieldConverter()
    let converter = resolveConvUntyped serializer.PickleType

    override __.Type = propertyInfo.PropertyType
    override __.Representation = FieldRepresentation.Serializer
    override __.DefaultValueUntyped = raise <| NotSupportedException("Default values not supported in serialized types.")
    override __.OfFieldUntyped value = 
        let pickle = serializer.SerializeUntyped value
        converter.OfFieldUntyped value

    override __.ToFieldUntyped a =
        let pickle = converter.ToFieldUntyped a
        serializer.DeserializeUntyped pickle