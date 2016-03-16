[<AutoOpen>]
module internal FSharp.AWS.DynamoDB.PrimitivePicklers

open System
open System.Collections
open System.Collections.Generic
open System.Globalization
open System.IO
open System.Reflection

open Microsoft.FSharp.Core.LanguagePrimitives

open Amazon.DynamoDBv2.Model

open FSharp.AWS.DynamoDB
open FSharp.AWS.DynamoDB.TypeShape

//
//  Pickler implementations for primitive types
//

type BoolPickler() =
    inherit StringRepresentablePickler<bool>()
    override __.PickleType = PickleType.Bool
    override __.PicklerType = PicklerType.Value
    override __.IsComparable = true

    override __.DefaultValue = false
    override __.Pickle b = AttributeValue(BOOL = b) |> Some
    override __.UnPickle a =
        if a.IsBOOLSet then a.BOOL
        else invalidCast a

    override __.Parse s = Boolean.Parse s
    override __.UnParse s = string s

type StringPickler() =
    inherit StringRepresentablePickler<string> ()
    override __.PickleType = PickleType.String
    override __.PicklerType = PicklerType.Value
    override __.IsComparable = true

    override __.DefaultValue = null
    override __.Pickle s =
        if isNull s then AttributeValue(NULL = true)
        elif s = "" then invalidOp "empty strings not supported by DynamoDB."
        else AttributeValue(s)
        |> Some

    override __.UnPickle a =
        if a.NULL then null
        elif not <| isNull a.S then a.S
        else invalidCast a

    override __.Parse s = s
    override __.UnParse s = s

type CharPickler() =
    inherit StringRepresentablePickler<char> ()
    override __.PickleType = PickleType.String
    override __.PicklerType = PicklerType.Value
    override __.IsComparable = true

    override __.DefaultValue = char 0
    override __.Pickle c = AttributeValue(string c) |> Some

    override __.UnPickle a =
        if not <| isNull a.S then System.Char.Parse(a.S)
        else invalidCast a

    override __.Parse s = System.Char.Parse s
    override __.UnParse c = string c

let inline mkNumericalPickler< ^N when ^N : (static member Parse : string * IFormatProvider -> ^N)
                                   and ^N : (member ToString : IFormatProvider -> string)> () =
    let inline parseNum s = 
        ( ^N : (static member Parse : string * IFormatProvider -> ^N) (s, CultureInfo.InvariantCulture))

    let inline toString n =
        ( ^N : (member ToString : IFormatProvider -> string) (n, CultureInfo.InvariantCulture))

    { new NumRepresentablePickler< ^N>() with
        member __.PickleType = PickleType.Number
        member __.PicklerType = PicklerType.Value
        member __.IsComparable = true

        member __.Parse s = parseNum s
        member __.UnParse e = toString e

        member __.DefaultValue = Unchecked.defaultof< ^N>
        member __.Pickle num = AttributeValue(N = toString num) |> Some
        member __.UnPickle a = 
            if not <| isNull a.N then parseNum a.N 
            else invalidCast a

        member __.PickleCoerced o =
            let n = match o with :? ^N as n -> n | other -> string other |> parseNum
            __.Pickle n
    }

type ByteArrayPickler() =
    inherit StringRepresentablePickler<byte[]> ()
    override __.PickleType = PickleType.Bytes
    override __.PicklerType = PicklerType.Value
    override __.IsComparable = true

    override __.Parse s = Convert.FromBase64String s
    override __.UnParse b = Convert.ToBase64String b

    override __.DefaultValue = [||]
    override __.Pickle bs = 
        if isNull bs then Some <| AttributeValue(NULL = true)
        elif bs.Length = 0 then None 
        else Some <| AttributeValue(B = new MemoryStream(bs))

    override __.UnPickle a = 
        if a.NULL then null
        elif not <| isNull a.B then a.B.ToArray()
        else
            invalidCast a


type MemoryStreamPickler() =
    inherit Pickler<MemoryStream> ()
    override __.PickleType = PickleType.Bytes
    override __.PicklerType = PicklerType.Value

    override __.DefaultValue = null
    override __.Pickle m = 
        if isNull m then Some <| AttributeValue(NULL = true)
        elif m.Length = 0L then None 
        else Some <| AttributeValue(B = m)

    override __.UnPickle a = 
        if a.NULL then null
        elif notNull a.B then a.B
        else
            invalidCast a

type GuidPickler() =
    inherit StringRepresentablePickler<Guid> ()
    override __.PickleType = PickleType.String
    override __.PicklerType = PicklerType.Value
    override __.IsComparable = true

    override __.DefaultValue = Guid.Empty
    override __.Pickle g = AttributeValue(string g) |> Some
    override __.UnPickle a =
        if not <| isNull a.S then Guid.Parse a.S
        else invalidCast a

    override __.Parse s = Guid.Parse s
    override __.UnParse g = string g

type DateTimeOffsetPickler() =
    inherit StringRepresentablePickler<DateTimeOffset> ()
    static let isoFormat = "yyyy-MM-dd\THH:mm:ss.fffffff\Z"
    let parse s = DateTimeOffset.Parse(s).ToLocalTime()
    let unparse (d:DateTimeOffset) = d.ToUniversalTime().ToString(isoFormat)

    override __.PickleType = PickleType.String
    override __.PicklerType = PicklerType.Value
    override __.IsComparable = true

    override __.DefaultValue = DateTimeOffset()
    override __.Parse s = parse s
    override __.UnParse d = unparse d

    override __.Pickle d = AttributeValue(unparse d) |> Some
    override __.UnPickle a = 
        if not <| isNull a.S then parse a.S 
        else invalidCast a


type TimeSpanPickler() =
    inherit NumRepresentablePickler<TimeSpan> ()
    override __.PickleType = PickleType.String
    override __.PicklerType = PicklerType.Value
    override __.IsComparable = true

    override __.Parse s = TimeSpan.FromTicks(int64 s)
    override __.UnParse t = string t.Ticks
    override __.DefaultValue = TimeSpan.Zero
    override __.Pickle t = AttributeValue(N = string t.Ticks) |> Some
    override __.UnPickle a = 
        if not <| isNull a.N then TimeSpan.FromTicks(int64 a.N) 
        else invalidCast a


type EnumerationPickler<'E, 'U when 'E : enum<'U>>() =
    inherit StringRepresentablePickler<'E> ()
    override __.PickleType = PickleType.String
    override __.PicklerType = PicklerType.Enum

    override __.DefaultValue = Unchecked.defaultof<'E>
    override __.Pickle e = AttributeValue(S = e.ToString()) |> Some
    override __.UnPickle a = 
        if notNull a.S then Enum.Parse(typeof<'E>, a.S) :?> 'E
        else invalidCast a

    override __.Parse s = Enum.Parse(typeof<'E>, s) :?> 'E
    override __.UnParse e = e.ToString()

type NullablePickler<'T when 'T : (new : unit -> 'T) and 'T :> ValueType and 'T : struct>(tp : Pickler<'T>) =
    inherit Pickler<Nullable<'T>> ()
    override __.PickleType = tp.PickleType
    override __.PicklerType = PicklerType.Wrapper
    override __.IsComparable = tp.IsComparable
    override __.DefaultValue = Nullable<'T>()
    override __.Pickle n = if n.HasValue then tp.Pickle n.Value else AttributeValue(NULL = true) |> Some
    override __.UnPickle a = if a.NULL then Nullable<'T> () else new Nullable<'T>(tp.UnPickle a)

type OptionPickler<'T>(tp : Pickler<'T>) =
    inherit Pickler<'T option> ()
    override __.PickleType = tp.PickleType
    override __.PicklerType = PicklerType.Wrapper
    override __.IsComparable = tp.IsComparable
    override __.DefaultValue = None
    override __.Pickle topt = match topt with None -> None | Some t -> tp.Pickle t
    override __.UnPickle a = if a.NULL then None else Some(tp.UnPickle a)
    override __.PickleCoerced obj =
        match obj with
        | :? 'T as t -> tp.Pickle t
        | :? ('T option) as topt -> __.Pickle topt
        | _ -> raise <| new InvalidCastException()


type StringRepresentationPickler<'T>(ep : StringRepresentablePickler<'T>) =
    inherit Pickler<'T> ()
    override __.PickleType = PickleType.String
    override __.PicklerType = ep.PicklerType
    override __.DefaultValue = ep.DefaultValue
    override __.Pickle t = AttributeValue(S = ep.UnParse t) |> Some
    override __.UnPickle a = 
        if notNull a.S then ep.Parse a.S
        else invalidCast a

let mkStringRepresentationPickler (resolver : IPicklerResolver) (prop : PropertyInfo) =
    getShape(prop.PropertyType).Accept {
        new IFunc<Pickler> with
            member __.Invoke<'T>() =
                match resolver.Resolve<'T>() with
                | :? StringRepresentablePickler<'T> as tp ->
                    if tp.PickleType = PickleType.String then tp :> Pickler
                    else new StringRepresentationPickler<'T>(tp) :> Pickler
                | _ ->
                    invalidArg prop.Name "property type cannot be represented as string."
    }

type SerializerAttributePickler<'T>(serializer : IPropertySerializer, resolver : IPicklerResolver) =
    inherit Pickler<'T>()

    let picklePickler = resolver.Resolve serializer.PickleType

    override __.PickleType = picklePickler.PickleType
    override __.PicklerType = PicklerType.Serialized
    override __.DefaultValue = 
        raise <| NotSupportedException("Default values not supported in serialized types.")

    override __.Pickle value = 
        let pickle = serializer.Serialize value
        picklePickler.PickleUntyped pickle

    override __.UnPickle a =
        let pickle = picklePickler.UnPickleUntyped a
        serializer.Deserialize pickle

let mkSerializerAttributePickler (resolver : IPicklerResolver) (serializer : IPropertySerializer) (t : Type) =
    getShape(t).Accept { 
        new IFunc<Pickler> with 
            member __.Invoke<'T> () = 
                new SerializerAttributePickler<'T>(serializer, resolver) :> _ }