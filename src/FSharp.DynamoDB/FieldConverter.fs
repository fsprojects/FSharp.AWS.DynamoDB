module internal FSharp.DynamoDB.FieldConverter

open System
open System.IO
open System.Collections
open System.Collections.Generic

open Microsoft.FSharp.Core.LanguagePrimitives

open Amazon.Util
open Amazon.DynamoDBv2.Model

open FSharp.DynamoDB.TypeShape

type FieldRepresentation =
    | Number        = 01
    | String        = 02
    | Bool          = 03
    | Bytes         = 04
    | Strings       = 05
    | Numbers       = 06
    | Bytess        = 07
    | Set           = 08
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
    abstract Parse : string -> 'T
    abstract UnParse : 'T -> string

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
    override __.Representation = FieldRepresentation.String
    override __.OfField s = String s
    override __.ToField a =
        match a with
        | Null -> null
        | String s -> s
        | _ -> invalidCast a

    override __.Parse s = s
    override __.UnParse s = s

type BytesConverter() =
    inherit StringRepresentableFieldConverter<byte[]> ()
    override __.Representation = FieldRepresentation.Bytes
    override __.DefaultValue = null
    override __.OfField bs = Bytes bs
    override __.ToField a = 
        match a with 
        | Null -> null
        | Bytes bs -> bs
        | _ -> invalidCast a

    override __.Parse s = Convert.FromBase64String s
    override __.UnParse s = Convert.ToBase64String s

type GuidConverter() =
    inherit StringRepresentableFieldConverter<Guid> ()
    override __.Representation = FieldRepresentation.String
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
    override __.Representation = FieldRepresentation.String
    override __.DefaultValue = DateTimeOffset()
    override __.OfField d = String(d.ToUniversalTime().ToString(AWSSDKUtils.ISO8601DateFormat))
    override __.ToField a = match a with String s -> DateTimeOffset.Parse(s).ToLocalTime() | _ -> invalidCast a
    override __.Parse s = DateTimeOffset.Parse(s).ToLocalTime()
    override __.UnParse d = d.ToUniversalTime().ToString(AWSSDKUtils.ISO8601DateFormat)

type TimeSpanConverter() =
    inherit StringRepresentableFieldConverter<TimeSpan> ()
    override __.Representation = FieldRepresentation.Number
    override __.DefaultValue = TimeSpan.Zero
    override __.OfField t = Number(string t.Ticks)
    override __.ToField a = match a with Number n -> TimeSpan.FromTicks(int64 n) | _ -> invalidCast a
    override __.Parse s = TimeSpan.FromTicks(int64 s)
    override __.UnParse t = string t.Ticks

let inline mkNumericalConverter< ^N when ^N : (static member Parse : string -> ^N)> () =
    let inline parseNum x = ( ^N : (static member Parse : string -> ^N) x)
    { new StringRepresentableFieldConverter< ^N>() with
        member __.Representation = FieldRepresentation.Number
        member __.DefaultValue = Unchecked.defaultof< ^N>
        member __.OfField num = Number(string num)
        member __.ToField a = match a with Number n -> parseNum n | _ -> invalidCast a
        member __.Parse s = parseNum s
        member __.UnParse e = string e
    }

type EnumerationConverter<'E, 'U when 'E : enum<'U>>(uconv : StringRepresentableFieldConverter<'U>) =
    inherit StringRepresentableFieldConverter<'E> ()
    override __.Representation = FieldRepresentation.Number
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
    | :? StringRepresentableFieldConverter<'T> as tconv ->
        { new StringRepresentableFieldConverter<'T ref>() with
            member __.DefaultValue = ref tconv.DefaultValue
            member __.Representation = tconv.Representation
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

type NumericalSeqConverter<'NSeq, 'N when 'NSeq :> seq<'N>>(ctor : seq<'N> -> 'NSeq, nconv : StringRepresentableFieldConverter<'N>) =
    inherit FieldConverter<'NSeq>()
    override __.Representation = FieldRepresentation.Numbers
    override __.DefaultValue = ctor [||]
    override __.OfField nums = let nums = nums |> Seq.map nconv.UnParse |> Seq.toArray in Numbers(nums)
    override __.ToField a =
        match a with
        | Null -> ctor [||]
        | Numbers ns -> ns |> Seq.map nconv.Parse |> ctor
        | _ -> invalidCast a

type StringSeqConverter<'SSeq when 'SSeq :> seq<string>>(ctor : seq<string> -> 'SSeq) =
    inherit FieldConverter<'SSeq>()
    override __.Representation = FieldRepresentation.Strings
    override __.DefaultValue = ctor [||]
    override __.OfField strings = Strings(Seq.toArray strings)
    override __.ToField a =
        match a with
        | Null -> ctor [||]
        | Numbers ns -> ctor ns
        | _ -> invalidCast a

type BytesSeqConverter<'BSeq when 'BSeq :> seq<byte[]>>(ctor : seq<byte []> -> 'BSeq) =
    inherit FieldConverter<'BSeq>()
    override __.Representation = FieldRepresentation.Bytess
    override __.DefaultValue = ctor [||]
    override __.OfField bss = Bytess (Seq.toArray bss)
    override __.ToField a =
        match a with
        | Null -> ctor [||]
        | Bytess ns -> ctor ns
        | _ -> invalidCast a

type MapConverter<'Map, 'Key, 'Value when 'Map :> seq<KeyValuePair<'Key, 'Value>>>
                    (ctor : seq<KeyValuePair<'Key, 'Value>> -> 'Map, 
                        kconv : StringRepresentableFieldConverter<'Key>,
                        vconv : FieldConverter<'Value>) =

    inherit FieldConverter<'Map>()
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

type SetConverter<'Set, 'T when 'Set :> seq<'T>> (ctor : seq<'T> -> 'Set, tconv : FieldConverter<'T>) =
    inherit FieldConverter<'Set>()
    override __.DefaultValue = ctor [||]
    override __.Representation = FieldRepresentation.Map
    override __.OfField set = set |> Seq.map tconv.OfField |> Seq.toArray |> List
    override __.ToField a =
        match a with
        | Null -> ctor [||]
        | List es -> es |> Seq.map tconv.ToField |> ctor
        | _ -> invalidCast a


//type FSharpRefConverter<'T>(tconv : FieldConverter<'T>) =
//    inherit FieldConverter<'T ref>()
//    override __.DefaultValue = ref tconv.DefaultValue
//    override __.Representation = tconv.Representation
//    override __.OfField set = set |> Seq.map

type UnSupportedField =
    static member Raise(fieldType : Type, ?reason : string) =
        let message = 
            match reason with
            | None -> sprintf "unsupported record field type '%O'" fieldType
            | Some r -> sprintf "unsupported record field type '%O': %s" fieldType r

        raise <| new ArgumentException(message)

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
                    if typeof<'T> = typeof<string> then
                        StringSeqConverter<string []>(Array.ofSeq) :> _
                    elif typeof<'T> = typeof<byte> then
                        BytesConverter() :> _
                    elif typeof<'T> = typeof<byte []> then
                        BytesSeqConverter<byte [][]>(Array.ofSeq) :> _
                    elif typeof<'T>.IsPrimitive || typeof<'T>.IsEnum then
                        new NumericalSeqConverter<'T[], 'T>(Array.ofSeq, resolveSRConv()) :> _
                    else
                        UnSupportedField.Raise t }

    | ShapeFSharpList s ->
        s.Accept {
            new IFSharpListVisitor<FieldConverter> with
                member __.VisitFSharpList<'T> () =
                    if typeof<'T> = typeof<string> then
                        StringSeqConverter<string list>(List.ofSeq) :> _
                    elif typeof<'T> = typeof<byte []> then
                        BytesSeqConverter<byte [] list>(List.ofSeq) :> _
                    elif typeof<'T>.IsPrimitive || typeof<'T>.IsEnum then
                        new NumericalSeqConverter<'T list, 'T>(List.ofSeq, resolveSRConv()) :> _
                    else
                        UnSupportedField.Raise t }

    | ShapeResizeArray s ->
        s.Accept {
            new IResizeArrayVisitor<FieldConverter> with
                member __.VisitResizeArray<'T> () =
                    if typeof<'T> = typeof<string> then
                        StringSeqConverter<ResizeArray<string>>(rlist) :> _
                    elif typeof<'T> = typeof<byte []> then
                        BytesSeqConverter<ResizeArray<byte []>>(rlist) :> _
                    elif typeof<'T>.IsPrimitive || typeof<'T>.IsEnum then
                        NumericalSeqConverter<ResizeArray<'T>, 'T>(rlist, resolveSRConv()) :> _
                    else
                        UnSupportedField.Raise t }

    | ShapeHashSet s ->
        s.Accept {
            new IHashSetVisitor<FieldConverter> with
                member __.VisitHashSet<'T when 'T : equality> () =
                    let tconv = resolveConv<'T> ()
                    if tconv.IsOptionalType then UnSupportedField.Raise typeof<HashSet<'T>>
                    new SetConverter<HashSet<'T>, 'T>(HashSet, tconv) :> _ }

    | ShapeFSharpSet s ->
        s.Accept {
            new IFSharpSetVisitor<FieldConverter> with
                member __.VisitFSharpSet<'T when 'T : comparison> () =
                    let tconv = resolveConv<'T> ()
                    if tconv.IsOptionalType then UnSupportedField.Raise typeof<Set<'T>>
                    new SetConverter<Set<'T>, 'T>(Set.ofSeq, tconv) :> _ }

    | ShapeDictionary s ->
        s.Accept {
            new IDictionaryVisitor<FieldConverter> with
                member __.VisitDictionary<'K, 'V when 'K : equality> () =
                    let kconv = resolveSRConv<'K> ()
                    let vconv = resolveConv<'V> ()
                    if vconv.IsOptionalType then UnSupportedField.Raise typeof<Dictionary<'K,'V>>

                    new MapConverter<Dictionary<'K, 'V>, 'K, 'V>(cdict, kconv, vconv) :> _ }

    | ShapeFSharpMap s ->
        s.Accept { 
            new IFSharpMapVisitor<FieldConverter> with
                member __.VisitFSharpMap<'K, 'V when 'K : comparison> () =
                    let mkMap (kvs : seq<KeyValuePair<'K,'V>>) =
                        kvs |> Seq.map (fun kv -> kv.Key, kv.Value) |> Map.ofSeq

                    let kconv = resolveSRConv<'K> ()
                    let vconv = resolveConv<'V> ()
                    if vconv.IsOptionalType then UnSupportedField.Raise typeof<Map<'K,'V>>

                    new MapConverter<Map<'K,'V>, 'K, 'V>(mkMap, kconv, vconv) :> _ }

    | _ -> UnSupportedField.Raise t


type SerializerConverter(serializer : PropertySerializerAttribute, propertyType : Type) =
    inherit FieldConverter()
    let converter = resolveConvUntyped serializer.PickleType

    override __.Type = propertyType
    override __.Representation = FieldRepresentation.Serializer
    override __.DefaultValueUntyped = raise <| NotSupportedException("Default values not supported in serialized types.")
    override __.OfFieldUntyped value = 
        let pickle = serializer.SerializeUntyped value
        converter.OfFieldUntyped value

    override __.ToFieldUntyped a =
        let pickle = converter.ToFieldUntyped a
        serializer.DeserializeUntyped pickle