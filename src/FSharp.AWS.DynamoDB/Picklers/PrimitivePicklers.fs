[<AutoOpen>]
module internal FSharp.AWS.DynamoDB.PrimitivePicklers

open System
open System.Globalization
open System.IO
open System.Reflection

open TypeShape
open Amazon.DynamoDBv2.Model

//
//  Pickler implementations for primitive types
//

type BoolPickler() =
    inherit StringRepresentablePickler<bool>()
    override _.PickleType = PickleType.Bool
    override _.PicklerType = PicklerType.Value
    override _.IsComparable = true

    override _.DefaultValue = false
    override _.Pickle b = AttributeValue(BOOL = b) |> Some
    override _.UnPickle a = if a.IsBOOLSet then a.BOOL.GetValueOrDefault false else invalidCast a

    override _.Parse s = Boolean.Parse s
    override _.UnParse s = string s

type StringPickler() =
    inherit StringRepresentablePickler<string>()
    override _.PickleType = PickleType.String
    override _.PicklerType = PicklerType.Value
    override _.IsComparable = true

    override _.DefaultValue = null
    override _.Pickle s =
        if isNull s then
            AttributeValue(NULL = true)
        else
            AttributeValue(s)
        |> Some

    override _.UnPickle a =
        if a.NULL.GetValueOrDefault false then null
        elif not <| isNull a.S then a.S
        else invalidCast a

    override _.Parse s = s
    override _.UnParse s = s

type CharPickler() =
    inherit StringRepresentablePickler<char>()
    override _.PickleType = PickleType.String
    override _.PicklerType = PicklerType.Value
    override _.IsComparable = true

    override _.DefaultValue = char 0
    override _.Pickle c = AttributeValue(string c) |> Some
    override _.UnPickle a = if not <| isNull a.S then Char.Parse(a.S) else invalidCast a

    override _.Parse s = Char.Parse s
    override _.UnParse c = string c

let inline mkNumericalPickler< ^N
    when ^N: (static member Parse: string * IFormatProvider -> ^N) and ^N: (member ToString: IFormatProvider -> string)>
    ()
    =
    let inline parseNum s = (^N: (static member Parse: string * IFormatProvider -> ^N) (s, CultureInfo.InvariantCulture))

    let inline toString n = (^N: (member ToString: IFormatProvider -> string) (n, CultureInfo.InvariantCulture))

    { new NumRepresentablePickler< ^N >() with
        member _.PickleType = PickleType.Number
        member _.PicklerType = PicklerType.Value
        member _.IsComparable = true

        member _.Parse s = parseNum s
        member _.UnParse e = toString e

        member _.DefaultValue = Unchecked.defaultof< ^N>
        member _.Pickle num = AttributeValue(N = toString num) |> Some
        member _.UnPickle a = if not <| isNull a.N then parseNum a.N else invalidCast a

        member x.PickleCoerced o =
            let n =
                match o with
                | :? ^N as n -> n
                | other -> string other |> parseNum
            x.Pickle n }

type DoublePickler() =
    inherit NumRepresentablePickler<Double>()
    let parse s = Double.Parse(s, CultureInfo.InvariantCulture)
    let unparse (d: double) = d.ToString("G17", CultureInfo.InvariantCulture)

    override _.PickleType = PickleType.Number
    override _.PicklerType = PicklerType.Value
    override _.IsComparable = true

    override _.Parse s = parse s
    override _.UnParse e = unparse e

    override _.DefaultValue = Unchecked.defaultof<double>
    override _.Pickle num = AttributeValue(N = unparse num) |> Some
    override _.UnPickle a = if not <| isNull a.N then parse a.N else invalidCast a

    override x.PickleCoerced o =
        let n =
            match o with
            | :? double as n -> n
            | other -> string other |> parse
        x.Pickle n

type ByteArrayPickler() =
    inherit StringRepresentablePickler<byte[]>()
    override _.PickleType = PickleType.Bytes
    override _.PicklerType = PicklerType.Value
    override _.IsComparable = true

    override _.Parse s = Convert.FromBase64String s
    override _.UnParse b = Convert.ToBase64String b

    override _.DefaultValue = [||]
    override _.Pickle bs =
        if isNull bs then Some <| AttributeValue(NULL = true)
        elif bs.Length = 0 then None
        else Some <| AttributeValue(B = new MemoryStream(bs))

    override _.UnPickle a =
        if a.NULL.GetValueOrDefault false then null
        elif not <| isNull a.B then a.B.ToArray()
        else invalidCast a


type MemoryStreamPickler() =
    inherit Pickler<MemoryStream>()
    override _.PickleType = PickleType.Bytes
    override _.PicklerType = PicklerType.Value

    override _.DefaultValue = null
    override _.Pickle m =
        if isNull m then Some <| AttributeValue(NULL = true)
        elif m.Length = 0L then None
        else Some <| AttributeValue(B = m)

    override _.UnPickle a =
        if a.NULL.GetValueOrDefault false then null
        elif notNull a.B then a.B
        else invalidCast a

type GuidPickler() =
    inherit StringRepresentablePickler<Guid>()
    override _.PickleType = PickleType.String
    override _.PicklerType = PicklerType.Value
    override _.IsComparable = true

    override _.DefaultValue = Guid.Empty
    override _.Pickle g = AttributeValue(string g) |> Some
    override _.UnPickle a = if not <| isNull a.S then Guid.Parse a.S else invalidCast a

    override _.Parse s = Guid.Parse s
    override _.UnParse g = string g

type DateTimeOffsetPickler() =
    inherit StringRepresentablePickler<DateTimeOffset>()
    static let isoFormat = "yyyy-MM-dd\THH:mm:ss.fffffffzzz"
    static let parse s = DateTimeOffset.Parse(s)
    static let unparse (d: DateTimeOffset) = d.ToString(isoFormat)

    override _.PickleType = PickleType.String
    override _.PicklerType = PicklerType.Value
    override _.IsComparable = true

    override _.DefaultValue = DateTimeOffset()
    override _.Parse s = parse s
    override _.UnParse d = unparse d

    override _.Pickle d = AttributeValue(unparse d) |> Some
    override _.UnPickle a = if not <| isNull a.S then parse a.S else invalidCast a


type TimeSpanPickler() =
    inherit NumRepresentablePickler<TimeSpan>()
    override _.PickleType = PickleType.String
    override _.PicklerType = PicklerType.Value
    override _.IsComparable = true

    override _.Parse s = TimeSpan.FromTicks(int64 s)
    override _.UnParse t = string t.Ticks
    override _.DefaultValue = TimeSpan.Zero
    override _.Pickle t = AttributeValue(N = string t.Ticks) |> Some
    override _.UnPickle a =
        if not <| isNull a.N then
            TimeSpan.FromTicks(int64 a.N)
        else
            invalidCast a


type EnumerationPickler<'E, 'U when 'E: enum<'U> and 'E: struct and 'E :> ValueType and 'E: (new: unit -> 'E)>() =
    inherit StringRepresentablePickler<'E>()
    override _.PickleType = PickleType.String
    override _.PicklerType = PicklerType.Enum

    override _.DefaultValue = Unchecked.defaultof<'E>
    override _.Pickle e = AttributeValue(S = e.ToString()) |> Some
    override _.UnPickle a =
        if notNull a.S then
            Enum.Parse(typeof<'E>, a.S) :?> 'E
        else
            invalidCast a

    override _.Parse s = Enum.Parse(typeof<'E>, s) :?> 'E
    override _.UnParse e = e.ToString()

type NullablePickler<'T when 'T: (new: unit -> 'T) and 'T :> ValueType and 'T: struct>(tp: Pickler<'T>) =
    inherit Pickler<Nullable<'T>>()
    override _.PickleType = tp.PickleType
    override _.PicklerType = PicklerType.Wrapper
    override _.IsComparable = tp.IsComparable
    override _.DefaultValue = Nullable<'T>()
    override _.Pickle n =
        if n.HasValue then
            tp.Pickle n.Value
        else
            AttributeValue(NULL = true) |> Some
    override _.UnPickle a =
        if a.NULL.GetValueOrDefault false then
            Nullable<'T>()
        else
            new Nullable<'T>(tp.UnPickle a)

type OptionPickler<'T>(tp: Pickler<'T>) =
    inherit Pickler<'T option>()
    override _.PickleType = tp.PickleType
    override _.PicklerType = PicklerType.Wrapper
    override _.IsComparable = tp.IsComparable
    override _.DefaultValue = None
    override _.Pickle topt =
        match topt with
        | None -> None
        | Some t -> tp.Pickle t
    override _.UnPickle a = if a.NULL.GetValueOrDefault false then None else Some(tp.UnPickle a)
    override x.PickleCoerced obj =
        match obj with
        | :? 'T as t -> tp.Pickle t
        | :? ('T option) as topt -> x.Pickle topt
        | _ -> raise <| InvalidCastException()

type StringRepresentationPickler<'T>(ep: StringRepresentablePickler<'T>) =
    inherit Pickler<'T>()
    override _.PickleType = PickleType.String
    override _.PicklerType = ep.PicklerType
    override _.DefaultValue = ep.DefaultValue
    override _.Pickle t = AttributeValue(S = ep.UnParse t) |> Some
    override _.UnPickle a = if notNull a.S then ep.Parse a.S else invalidCast a

let mkStringRepresentationPickler (resolver: IPicklerResolver) (prop: PropertyInfo) =
    TypeShape.Create(prop.PropertyType).Accept
        { new ITypeVisitor<Pickler> with
            member _.Visit<'T>() =
                match resolver.Resolve<'T>() with
                | :? StringRepresentablePickler<'T> as tp ->
                    if tp.PickleType = PickleType.String then
                        tp :> Pickler
                    else
                        new StringRepresentationPickler<'T>(tp) :> Pickler
                | _ -> invalidArg prop.Name "property type cannot be represented as string." }

type SerializerAttributePickler<'T>(serializer: IPropertySerializer, resolver: IPicklerResolver) =
    inherit Pickler<'T>()

    let picklePickler = resolver.Resolve serializer.PickleType

    override _.PickleType = picklePickler.PickleType
    override _.PicklerType = PicklerType.Serialized
    override _.DefaultValue = raise <| NotSupportedException("Default values not supported in serialized types.")

    override _.Pickle value =
        let pickle = serializer.Serialize value
        picklePickler.PickleUntyped pickle

    override _.UnPickle a =
        let pickle = picklePickler.UnPickleUntyped a
        serializer.Deserialize pickle

let mkSerializerAttributePickler (resolver: IPicklerResolver) (serializer: IPropertySerializer) (t: Type) =
    TypeShape.Create(t).Accept
        { new ITypeVisitor<Pickler> with
            member _.Visit<'T>() = new SerializerAttributePickler<'T>(serializer, resolver) :> _ }
