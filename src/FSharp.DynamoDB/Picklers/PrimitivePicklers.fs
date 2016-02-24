[<AutoOpen>]
module internal FSharp.DynamoDB.PrimitivePicklers

open System
open System.Collections
open System.Collections.Generic
open System.IO
open System.Reflection

open Microsoft.FSharp.Core.LanguagePrimitives

open Amazon.DynamoDBv2.Model

open FSharp.DynamoDB
open FSharp.DynamoDB.TypeShape

//
//  Pickler implementations for primitive types
//

type BoolPickler() =
    inherit StringRepresentablePickler<bool>()
    override __.PickleType = PickleType.Bool
    override __.PicklerType = PicklerType.Value

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

    override __.DefaultValue = null
    override __.Pickle s =
        if isNull s then AttributeValue(NULL = true)
        else AttributeValue(s)
        |> Some

    override __.UnPickle a =
        if a.NULL then null
        elif not <| isNull a.S then a.S
        else invalidCast a

    override __.Parse s = s
    override __.UnParse s = s

let inline mkNumericalPickler< ^N when ^N : (static member Parse : string -> ^N)> () =
    let inline parseNum x = ( ^N : (static member Parse : string -> ^N) x)
    { new NumRepresentablePickler< ^N>() with
        member __.PickleType = PickleType.Number
        member __.PicklerType = PicklerType.Value

        member __.Parse s = parseNum s
        member __.UnParse e = string e

        member __.DefaultValue = Unchecked.defaultof< ^N>
        member __.Pickle num = AttributeValue(N = string num) |> Some
        member __.UnPickle a = 
            if not <| isNull a.N then parseNum a.N 
            else invalidCast a
    }

type ByteArrayPickler() =
    inherit Pickler<byte[]> ()
    override __.PickleType = PickleType.Bytes
    override __.PicklerType = PicklerType.Value

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

    override __.Parse s = TimeSpan.FromTicks(int64 s)
    override __.UnParse t = string t.Ticks
    override __.DefaultValue = TimeSpan.Zero
    override __.Pickle t = AttributeValue(N = string t.Ticks) |> Some
    override __.UnPickle a = 
        if not <| isNull a.N then TimeSpan.FromTicks(int64 a.N) 
        else invalidCast a

type EnumerationPickler<'E, 'U when 'E : enum<'U>>(up : NumRepresentablePickler<'U>) =
    inherit NumRepresentablePickler<'E> ()
    override __.PickleType = PickleType.Number
    override __.PicklerType = PicklerType.Value

    override __.DefaultValue = Unchecked.defaultof<'E>
    override __.Pickle e = let u = EnumToValue<'E,'U> e in up.Pickle u
    override __.UnPickle a = EnumOfValue<'U, 'E>(up.UnPickle a)
    override __.Parse s = up.Parse s |> EnumOfValue<'U, 'E>
    override __.UnParse e = EnumToValue<'E, 'U> e |> up.UnParse

type NullablePickler<'T when 'T : (new : unit -> 'T) and 'T :> ValueType and 'T : struct>(tp : Pickler<'T>) =
    inherit Pickler<Nullable<'T>> ()
    override __.PickleType = tp.PickleType
    override __.PicklerType = PicklerType.Wrapper
    override __.DefaultValue = Nullable<'T>()
    override __.Pickle n = if n.HasValue then tp.Pickle n.Value else AttributeValue(NULL = true) |> Some
    override __.UnPickle a = if a.NULL then Nullable<'T> () else new Nullable<'T>(tp.UnPickle a)

type OptionPickler<'T>(tp : Pickler<'T>) =
    inherit Pickler<'T option> ()
    override __.PickleType = tp.PickleType
    override __.PicklerType = PicklerType.Wrapper
    override __.DefaultValue = None
    override __.Pickle topt = match topt with None -> None | Some t -> tp.Pickle t
    override __.UnPickle a = if a.NULL then None else Some(tp.UnPickle a)

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