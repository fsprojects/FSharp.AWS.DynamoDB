module internal FSharp.DynamoDB.TypeConverter

open System
open System.Collections
open System.Collections.Generic
open System.IO
open System.Reflection

open Microsoft.FSharp.Core.LanguagePrimitives
open Microsoft.FSharp.Reflection

open Amazon.Util
open Amazon.DynamoDBv2.Model

open FSharp.DynamoDB.TypeShape

//type Representation =
//    | Number        = 01
//    | String        = 02
//    | Bool          = 03
//    | Bytes         = 04
//    | StringSet     = 05
//    | NumberSet     = 06
//    | BytesSet      = 07
//    | List          = 08
//    | Map           = 09
//    | Serializer    = 10

type FsAttributeValue =
    | Null
    | Bool of bool
    | String of string
    | Number of string
    | Bytes of byte[]
    | StringSet of string[]
    | NumberSet of string[]
    | BytesSet of byte[][]
    | List of FsAttributeValue[]
    | Map of Map<string, FsAttributeValue>
with
    static member FromAttributeValue(av : AttributeValue) =
        if av.NULL then Null
        elif av.IsBOOLSet then Bool av.BOOL
        elif av.S <> null then String av.S
        elif av.N <> null then Number av.N
        elif av.B <> null then Bytes (av.B.ToArray())
        elif av.SS.Count > 0 then StringSet (Seq.toArray av.SS)
        elif av.NS.Count > 0 then NumberSet (Seq.toArray av.NS)
        elif av.BS.Count > 0 then av.BS |> Seq.map (fun bs -> bs.ToArray()) |> Seq.toArray |> BytesSet
        elif av.IsLSet then av.L |> Seq.map FsAttributeValue.FromAttributeValue |> Seq.toArray |> List
        elif av.IsMSet then 
            av.M 
            |> Seq.map (fun kv -> kv.Key, FsAttributeValue.FromAttributeValue kv.Value)
            |> Map.ofSeq
            |> Map

        else
            invalidArg "av" "undefined attribute value."

    static member ToAttributeValue(fsav : FsAttributeValue) =
        match fsav with
        | Null -> AttributeValue(NULL = true)
        | Bool b -> AttributeValue(BOOL = b)
        | String null -> AttributeValue(NULL = true)
        | String s -> AttributeValue(s)
        | Number null -> invalidArg "fsav" "Number attribute contains null as value."
        | Number n -> AttributeValue(N = n)
        | Bytes null -> AttributeValue(NULL = true)
        | Bytes bs -> AttributeValue(B = new MemoryStream(bs))
        | StringSet (null | [||]) -> AttributeValue(NULL = true)
        | StringSet ss -> AttributeValue(rlist ss)
        | NumberSet (null | [||]) -> AttributeValue(NULL = true)
        | NumberSet ns -> AttributeValue(NS = rlist ns)
        | BytesSet (null | [||]) -> AttributeValue(NULL = true)
        | BytesSet bss -> AttributeValue(BS = (bss |> Seq.map (fun bs -> new MemoryStream(bs)) |> rlist))
        | List (null | [||]) -> AttributeValue(NULL = true)
        | List attrs -> AttributeValue(L = (attrs |> Seq.map FsAttributeValue.ToAttributeValue |> rlist))
        | Map m when m.IsEmpty -> AttributeValue(NULL = true)
        | Map m -> 
            AttributeValue(M =
                (m
                |> Seq.map (fun kv -> KeyValuePair(kv.Key, FsAttributeValue.ToAttributeValue kv.Value)) 
                |> cdict))
    
type RestObject = Dictionary<string, AttributeValue>

[<AutoOpen>]
module Utils =

    type AttributeValue with
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
        let msg = sprintf "could not convert value %A to type '%O'" (av.Print()) typeof<'T>
        raise <| new InvalidCastException(msg)



type Representation =
    | Primitive  = 1L
    | Serialized = 2L
    | Optional   = 3L
    | Record     = 4L

[<AbstractClass>]
type TypeConverter() =
    abstract Type : Type
    abstract Representation : Representation

    abstract DefaultValueUntyped : obj
    abstract ToAttrUntyped : obj -> AttributeValue option
    abstract OfAttrUntyped : AttributeValue -> obj

[<AbstractClass>]
type TypeConverter<'T>() =
    inherit TypeConverter()

    abstract DefaultValue : 'T
    abstract ToAttr : 'T -> AttributeValue option
    abstract OfAttr : AttributeValue -> 'T

    override __.Type = typeof<'T>
    override __.DefaultValueUntyped = __.DefaultValue :> obj
    override __.ToAttrUntyped o = __.ToAttr (o :?> 'T)
    override __.OfAttrUntyped a = __.OfAttr a :> obj

type BoolConverter() =
    inherit TypeConverter<bool>()
    override __.DefaultValue = false
    override __.Representation = Representation.Primitive
    override __.ToAttr b = new AttributeValue(BOOL = b) |> Some
    override __.OfAttr av =
        if av.IsBOOLSet then av.BOOL
        else invalidCast av

type StringConverter() =
    inherit TypeConverter<string> ()
    override __.DefaultValue = null
    override __.ToAttr s =
        if s = null then AttributeValue(NULL = true)
        else AttributeValue(s)
        |> Some

    override __.OfAttr av =
        if av.NULL then null
        elif av.S <> null then av.S
        else invalidCast av

let inline mkNumericalConverter< ^N when ^N : (static member Parse : string -> ^N)> () =
    { new TypeConverter< ^N>() with
        member __.Representation = Representation.Primitive
        member __.DefaultValue = Unchecked.defaultof< ^N>
        member __.ToAttr num = AttributeValue(string num) |> Some
        member __.OfAttr av = 
            if av.N <> null then
                ( ^N : (static member Parse : string -> ^N) av.N)
            else
                invalidCast av
    }

type BytesConverter() =
    inherit TypeConverter<byte[]> ()
    override __.Representation = Representation.Primitive
    override __.DefaultValue = null
    override __.ToAttr bs = 
        if bs = null then AttributeValue(NULL = true)
        else
            AttributeValue(B = new MemoryStream(bs))

        |> Some

    override __.OfAttr av = 
        if av.NULL then null
        elif av.B <> null then av.B.ToArray()
        else invalidCast av

type GuidConverter() =
    inherit TypeConverter<Guid> ()
    override __.Representation = Representation.Primitive
    override __.DefaultValue = Guid.Empty
    override __.ToAttr g = AttributeValue(string g) |> Some
    override __.OfAttr av =
        if av.S <> null then Guid.Parse av.S
        else invalidCast av

type DateTimeOffsetConverter() =
    inherit TypeConverter<DateTimeOffset> ()
    override __.Representation = Representation.Primitive
    override __.DefaultValue = DateTimeOffset()
    override __.ToAttr d = 
        let fmt = d.ToUniversalTime().ToString(AWSSDKUtils.ISO8601DateFormat)
        AttributeValue(fmt) |> Some

    override __.OfAttr av =
        if av.S <> null then DateTimeOffset.Parse(av.S).ToLocalTime()
        else invalidCast av

type TimeSpanConverter() =
    inherit TypeConverter<TimeSpan> ()
    override __.DefaultValue = TimeSpan.Zero
    override __.Representation = Representation.Primitive
    override __.ToAttr t = AttributeValue (string t.Ticks) |> Some
    override __.OfAttr av =
        if av.I
        match av with
        | Number n -> TimeSpan.FromTicks(int64 n)
        | _ -> invalidCast av

    override __.Parse s = TimeSpan.FromTicks(int64 s)
    override __.UnParse t = string t.Ticks

type EnumerationConverter<'E, 'U when 'E : enum<'U>>(uconv : NumRepresentableConverter<'U>) =
    inherit NumRepresentableConverter<'E> ()
    override __.DefaultValue = Unchecked.defaultof<'E>
    override __.Write e = let u = EnumToValue<'E,'U> e in uconv.Write u
    override __.Read av = EnumOfValue<'U, 'E>(uconv.Read av)
    override __.Parse s = uconv.Parse s |> EnumOfValue<'U, 'E>
    override __.UnParse e = EnumToValue<'E, 'U> e |> uconv.UnParse

type NullableConverter<'T when 'T : (new : unit -> 'T) and 'T :> ValueType and 'T : struct>(tconv : TypeConverter<'T>) =
    inherit TypeConverter<Nullable<'T>> ()
    override __.Representation = tconv.Representation
    override __.IsOptionalType = true
    override __.DefaultValue = Nullable<'T>()
    override __.Write n = if n.HasValue then tconv.Write n.Value else Undefined
    override __.Read av =
        match av with
        | Undefined | Null -> new Nullable<'T>()
        | av -> new Nullable<'T>(tconv.Read av)

type OptionConverter<'T>(tconv : TypeConverter<'T>) =
    inherit TypeConverter<'T option> ()
    override __.Representation = tconv.Representation
    override __.IsOptionalType = true
    override __.DefaultValue = None
    override __.Write topt = match topt with None -> Undefined | Some t -> tconv.Write t
    override __.Read a =
        match a with
        | Undefined | Null -> None
        | _ -> Some(tconv.Read a)


let mkFSharpRefConverter<'T> (tconv : TypeConverter<'T>) =
    match tconv with
    | :? NumRepresentableConverter<'T> as tconv ->
        { new NumRepresentableConverter<'T ref>() with
            member __.DefaultValue = ref tconv.DefaultValue
            member __.Write tref = tconv.Write tref.Value
            member __.Read a = tconv.Read a |> ref
            member __.Parse s = tconv.Parse s |> ref
            member __.UnParse tref = tconv.UnParse tref.Value
        } :> TypeConverter<'T ref>

    | :? StringRepresentableConverter<'T> as tconv ->
        { new StringRepresentableConverter<'T ref>() with
            member __.DefaultValue = ref tconv.DefaultValue
            member __.Write tref = tconv.Write tref.Value
            member __.Read a = tconv.Read a |> ref
            member __.Parse s = tconv.Parse s |> ref
            member __.UnParse tref = tconv.UnParse tref.Value
        } :> TypeConverter<'T ref>

    | _ ->
        { new TypeConverter<'T ref>() with
            member __.DefaultValue = ref tconv.DefaultValue
            member __.Representation = tconv.Representation
            member __.IsOptionalType = tconv.IsOptionalType
            member __.Write tref = tconv.Write tref.Value
            member __.Read a = tconv.Read a |> ref }

type ListConverter<'List, 'T when 'List :> seq<'T>>(ctor : seq<'T> -> 'List, tconv : TypeConverter<'T>) =
    inherit TypeConverter<'List>()
    override __.Representation = Representation.List
    override __.DefaultValue = ctor [||]
    override __.Write list = list |> Seq.map tconv.OfFieldUntyped |> Seq.toArray |> List
    override __.Read a =
        match a with
        | Null -> ctor [||]
        | List ts -> ts |> Seq.map tconv.ToField |> ctor
        | _ -> invalidCast a

[<AbstractClass>]
type RecordConverter<'T> () =
    inherit TypeConverter<'T>()

    abstract WriteRecord : value:'T -> FsRestObject
    abstract ReadRecord  : FsRestObject -> 'T

    override __.Representation = Representation.Map
    override __.DefaultValue = invalidOp "Default values not supported in records."

    override __.Write (value:'T) =
        Map (__.WriteRecord value)

    override __.Read (av : FsAttributeValue) =
        match av with
        | Map fsr -> __.ReadRecord fsr
        | _ -> invalidCast av

type ITypeConverterResolver =
    abstract Resolve<'T> : unit -> TypeConverter<'T>
    abstract Resolve : Type -> TypeConverter
//    abstract Register : TypeConverter<'T> -> TypeConverter<'T>

type SerializerConverter(resolver : ITypeConverterResolver, prop : PropertyInfo, serializer : PropertySerializerAttribute) =
    inherit TypeConverter()

    let pickleConverter = resolver.Resolve serializer.PickleType

    override __.Representation = Representation.Serializer
    override __.Type = prop.PropertyType
    override __.DefaultValueUntyped = raise <| NotSupportedException("Default values not supported in serialized types.")
    override __.WriteUntyped value =
        let pickle = serializer.SerializeUntyped(value)
        pickleConverter.WriteUntyped pickle

    override __.ReadUntyped attr =
        let pickle = pickleConverter.ReadUntyped attr
        serializer.DeserializeUntyped pickle


type RecordProperty =
    {
        Name : string
        PropertyInfo : PropertyInfo
        Converter : TypeConverter
        AllowDefaultValue : bool
        Attributes : Attribute []
    }
with
    static member Define(prop : PropertyInfo, resolver : ITypeConverterResolver) =
        let attributes = prop.GetAttributes()
        let converter = 
            match tryGetAttribute<PropertySerializerAttribute> attributes with
            | Some serializer -> new SerializerConverter(resolver, prop, serializer) :> TypeConverter
            | None -> resolver.Resolve prop.PropertyType

        let name =
            match attributes |> tryGetAttribute<CustomNameAttribute> with
            | Some cn -> cn.Name
            | None -> prop.Name

        let allowDefaultValue = containsAttribute<AllowDefaultValueAttribute> attributes

        {
            Name = name
            PropertyInfo = prop
            Converter = converter
            Attributes = attributes
            AllowDefaultValue = allowDefaultValue
        }

type FsharpRecordConverter<'T>(resolver : ITypeConverterResolver) =
    inherit RecordConverter<'T> ()

    let ctor = FSharpValue.PreComputeRecordConstructorInfo(typeof<'T>, true)
    let properties = 
        FSharpType.GetRecordFields(typeof<'T>, true) 
        |> Array.map (fun p -> RecordProperty.Define(p, resolver))

    override __.WriteRecord value =
        let dict = new Dictionary<_,_>()
        for prop in properties do
            let field = prop.PropertyInfo.GetValue(value)
            let av = prop.Converter.WriteUntyped field
            dict.Add(prop.Name, av)

        { Map = dict }

    override __.ReadRecord ro =
        let dict = ro.Map
        let values = Array.zeroCreate<obj> properties.Length
        for i = 0 to properties.Length - 1 do
            let prop = properties.[i]
            let ok, av' = dict.TryGetValue prop.Name
            if ok then
                values.[i] <- prop.Converter.ReadUntyped av'
            elif prop.AllowDefaultValue then
                values.[i] <- prop.Converter.DefaultValueUntyped
            else
                raise <| new KeyNotFoundException(sprintf "attribute %A not found." prop.Name)

        ctor.Invoke values :?> 'T