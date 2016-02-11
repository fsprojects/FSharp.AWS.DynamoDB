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
    | Number    = 1
    | String    = 2
    | Bool      = 3
    | Bytes     = 4
    | Strings   = 5
    | Numbers   = 6
    | Bytess    = 7
    | Set       = 8
    | Map       = 9

[<AbstractClass>]
type FieldConverter() =
    abstract Type : Type
    abstract Representation : FieldRepresentation
    abstract OfFieldUntyped : obj -> AttributeValue
    abstract ToFieldUntyped : AttributeValue -> obj

[<AbstractClass>]
type FieldConverter<'T>() =
    inherit FieldConverter()

    abstract OfField : 'T -> AttributeValue
    abstract ToField : AttributeValue -> 'T

    override __.Type = typeof<'T>
    override __.OfFieldUntyped o = __.OfField(o :?> 'T)
    override __.ToFieldUntyped av = __.ToField av :> obj

[<AbstractClass>]
type StringRepresentableFieldConverter<'T>() =
    inherit FieldConverter<'T>()
    abstract Parse : string -> 'T
    abstract UnParse : 'T -> string

type BoolConverter() =
    inherit StringRepresentableFieldConverter<bool>()
    override __.Representation = FieldRepresentation.Bool
    override __.OfField b = new AttributeValue(BOOL = b)
    override __.ToField a = a.BOOL
    override __.Parse s = Boolean.Parse s
    override __.UnParse s = string s

type StringConverter() =
    inherit StringRepresentableFieldConverter<string> ()
    override __.Representation = FieldRepresentation.String
    override __.OfField s = new AttributeValue(S = s)
    override __.ToField a = a.S
    override __.Parse s = s
    override __.UnParse s = s

type BytesConverter() =
    inherit StringRepresentableFieldConverter<byte[]> ()
    override __.Representation = FieldRepresentation.Bytes
    override __.OfField b =
        if b = null then new AttributeValue(NULL = true)
        else new AttributeValue(B = new MemoryStream(b))

    override __.ToField a = match a.B with null -> null | m -> m.ToArray()
    override __.Parse s = Convert.FromBase64String s
    override __.UnParse s = Convert.ToBase64String s

type GuidConverter() =
    inherit StringRepresentableFieldConverter<Guid> ()
    override __.Representation = FieldRepresentation.String
    override __.OfField g = new AttributeValue(S = string g)
    override __.ToField a =
        match a.S with
        | null -> invalidOp "Unset string"
        | s -> Guid.Parse s

    override __.Parse s = Guid.Parse s
    override __.UnParse g = string g

type DateTimeConverter() =
    inherit StringRepresentableFieldConverter<DateTime> ()
    override __.Representation = FieldRepresentation.String
    override __.OfField d = new AttributeValue(S = d.ToString(AWSSDKUtils.ISO8601DateFormat))
    override __.ToField a = DateTime.Parse(a.S)
    override __.Parse s = DateTime.Parse s
    override __.UnParse g = g.ToString(AWSSDKUtils.ISO8601DateFormat)

type DateTimeOffsetConverter() =
    inherit StringRepresentableFieldConverter<DateTimeOffset> ()
    override __.Representation = FieldRepresentation.String
    override __.OfField d = new AttributeValue(S = d.ToUniversalTime().ToString(AWSSDKUtils.ISO8601DateFormat))
    override __.ToField a = DateTimeOffset.Parse(a.S)
    override __.Parse s = DateTimeOffset.Parse s
    override __.UnParse d = d.ToUniversalTime().ToString(AWSSDKUtils.ISO8601DateFormat)

type TimeSpanConverter() =
    inherit StringRepresentableFieldConverter<TimeSpan> ()
    override __.Representation = FieldRepresentation.Number
    override __.OfField t = new AttributeValue(N = string t.Ticks)
    override __.ToField a = TimeSpan.FromTicks(int64 a.N)
    override __.Parse s = TimeSpan.FromTicks(int64 s)
    override __.UnParse t = string t.Ticks

let inline mkNumericalConverter< ^N when ^N : (static member Parse : string -> ^N)> () =
    let inline parseNum x = ( ^N : (static member Parse : string -> ^N) x)
    { new StringRepresentableFieldConverter< ^N>() with
        member __.Representation = FieldRepresentation.Number
        member __.OfField num = new AttributeValue(N = string num)
        member __.ToField a = parseNum a.N
        member __.Parse s = parseNum s
        member __.UnParse e = string e
    }

type EnumerationConverter<'E, 'U when 'E : enum<'U>>(uconv : StringRepresentableFieldConverter<'U>) =
    inherit StringRepresentableFieldConverter<'E> ()
    override __.Representation = FieldRepresentation.Number
    override __.OfField e = let u = EnumToValue<'E,'U> e in new AttributeValue(N = u.ToString())
    override __.ToField a = EnumOfValue<'U, 'E>(uconv.ToField a)
    override __.Parse s = uconv.Parse s |> EnumOfValue<'U, 'E>
    override __.UnParse e = EnumToValue<'E, 'U> e |> uconv.UnParse

//type NullableConverter<'T when 'T : (new : unit -> 'T) and 'T :> ValueType and 'T : struct>(tconv : FieldConverter<'T>) =
//    inherit FieldConverter<Nullable<'T>> ()
//    override __.Representation = tconv.Representation
//    override __.OfField n = if n.HasValue then tconv.OfField n.Value else new AttributeValue()
//    override __.ToField a = if a.Has

type NumericalSeqConverter<'NSeq, 'N when 'NSeq :> seq<'N>>(ctor : seq<'N> -> 'NSeq, nconv : StringRepresentableFieldConverter<'N>) =
    inherit FieldConverter<'NSeq>()
    override __.Representation = FieldRepresentation.Numbers
    override __.OfField nums = let nums = nums |> Seq.map nconv.UnParse |> rlist in new AttributeValue(NS = nums)
    override __.ToField a =
        match a.NS with
        | null -> ctor [||]
        | ra -> ra |> Seq.map nconv.Parse |> ctor

type StringSeqConverter<'SSeq when 'SSeq :> seq<string>>(ctor : seq<string> -> 'SSeq) =
    inherit FieldConverter<'SSeq>()
    override __.Representation = FieldRepresentation.Strings
    override __.OfField strings = new AttributeValue(rlist strings)
    override __.ToField a =
        match a.NS with
        | null -> ctor [||]
        | ra -> ctor ra

type BytesSeqConverter<'BSeq when 'BSeq :> seq<byte[]>>(ctor : seq<byte []> -> 'BSeq) =
    inherit FieldConverter<'BSeq>()
    override __.Representation = FieldRepresentation.Bytess
    override __.OfField bss = 
        let bss = bss |> Seq.map (fun bs -> new MemoryStream(bs)) |> rlist
        new AttributeValue(BS = bss)

    override __.ToField a =
        match a.BS with
        | null -> ctor [||]
        | ra -> ra |> Seq.map (fun m -> m.ToArray()) |> ctor

type MapConverter<'Map, 'Key, 'Value when 'Map :> seq<KeyValuePair<'Key, 'Value>>>

                    (ctor : seq<KeyValuePair<'Key, 'Value>> -> 'Map, 
                        kconv : StringRepresentableFieldConverter<'Key>,
                        vconv : FieldConverter<'Value>) =

    inherit FieldConverter<'Map>()
    override __.Representation = FieldRepresentation.Map
    override __.OfField map =
        let attr = new AttributeValue()
        for kv in map do attr.M.Add(kconv.UnParse kv.Key, vconv.OfField kv.Value)
        attr

    override __.ToField attr =
        attr.M |> Seq.map (fun kv -> KeyValuePair(kconv.Parse kv.Key, vconv.ToField kv.Value)) |> ctor

type SetConverter<'Set, 'T when 'Set :> seq<'T>> (ctor : seq<'T> -> 'Set, tconv : FieldConverter<'T>) =
    inherit FieldConverter<'Set>()
    override __.Representation = FieldRepresentation.Map
    override __.OfField set =
        let attr = new AttributeValue()
        set |> Seq.map tconv.OfField |> attr.L.AddRange
        attr

    override __.ToField attr =
        attr.L |> Seq.map tconv.ToField |> ctor


//type IConverterResolver =
//    abstract Resolve : Type -> FieldConverter
//    abstract Resolve<'T> : unit -> FieldConverter<'T>



exception UnSupportedFieldType of Type:Type
 with
    override e.Message = sprintf "unsupported record field type '%O'" e.Type


let rec resolveConv<'T> () = resolveConvUntyped typeof<'T> :?> FieldConverter<'T>

and resolveSRConv<'T> () = 
    match resolveConv<'T> () with
    | :? StringRepresentableFieldConverter<'T> as sr -> sr
    | _ -> raise <| UnSupportedFieldType typeof<'T>

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
    | :? ShapeDecimal -> mkNumericalConverter<decimal> () :> _
    | :? ShapeString -> new StringConverter() :> _
    | :? ShapeGuid -> new GuidConverter() :> _
    | :? ShapeTimeSpan -> new TimeSpanConverter() :> _
    | :? ShapeDateTime -> new DateTimeConverter() :> _
    | :? ShapeDateTimeOffset -> new DateTimeOffsetConverter() :> _
    | ShapeEnum s ->
        s.Accept {
            new IEnumVisitor<FieldConverter> with
                member __.VisitEnum<'E, 'U when 'E : enum<'U>> () =
                    new EnumerationConverter<'E, 'U>(resolveSRConv()) :> _ }

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
                        raise <| UnSupportedFieldType t }

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
                        raise <| UnSupportedFieldType t }

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
                        raise <| UnSupportedFieldType t }

    | ShapeHashSet s ->
        s.Accept {
            new IHashSetVisitor<FieldConverter> with
                member __.VisitHashSet<'T when 'T : equality> () =
                    new SetConverter<HashSet<'T>, 'T>(HashSet, resolveConv()) :> _ }

    | ShapeFSharpSet s ->
        s.Accept {
            new IFSharpSetVisitor<FieldConverter> with
                member __.VisitFSharpSet<'T when 'T : comparison> () =
                    new SetConverter<Set<'T>, 'T>(Set.ofSeq, resolveConv()) :> _ }

    | ShapeDictionary s ->
        s.Accept {
            new IDictionaryVisitor<FieldConverter> with
                member __.VisitDictionary<'K, 'V when 'K : equality> () =
                    let mkDict (kvs : seq<KeyValuePair<_,_>>) = 
                        let d = new Dictionary<'K,'V>()
                        for kv in kvs do d.Add(kv.Key, kv.Value)
                        d

                    new MapConverter<Dictionary<'K, 'V>, 'K, 'V>(mkDict, resolveSRConv(), resolveConv()) :> _ }

    | ShapeFSharpMap s ->
        s.Accept { 
            new IFSharpMapVisitor<FieldConverter> with
                member __.VisitFSharpMap<'K, 'V when 'K : comparison> () =
                    let mkMap (kvs : seq<KeyValuePair<'K,'V>>) =
                        kvs |> Seq.map (fun kv -> kv.Key, kv.Value) |> Map.ofSeq

                    new MapConverter<Map<'K,'V>, 'K, 'V>(mkMap, resolveSRConv(), resolveConv()) :> _ }

    | _ -> raise <| UnSupportedFieldType t
