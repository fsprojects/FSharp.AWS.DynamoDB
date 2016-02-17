namespace FSharp.DynamoDB.FieldConverter

open System
open System.IO
open System.Collections.Generic

open Amazon.DynamoDBv2.Model

open FSharp.DynamoDB

type FieldRepresentation =
    | Number        = 01
    | String        = 02
    | Bool          = 03
    | Bytes         = 04
    | StringSet     = 05
    | NumberSet     = 06
    | BytesSet      = 07
    | List          = 08
    | Map           = 09

type ConverterType =
    | Value         = 01
    | Wrapper       = 02
    | Record        = 03
    | Serialized    = 04

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
    | Map of KeyValuePair<string, FsAttributeValue>[]
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
            |> Seq.map (fun kv -> KeyValuePair(kv.Key, FsAttributeValue.FromAttributeValue kv.Value)) 
            |> Seq.toArray
            |> Map

        else invalidArg "av" "undefined attribute value"

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
        | Map (null | [||]) -> AttributeValue(NULL = true)
        | Map attrs -> 
            AttributeValue(M =
                (attrs
                |> Seq.map (fun kv -> KeyValuePair(kv.Key, FsAttributeValue.ToAttributeValue kv.Value)) 
                |> cdict))

type RestObject = Dictionary<string, AttributeValue>

[<AbstractClass>]
type FieldConverter() =
    abstract Type : Type
    abstract Representation : FieldRepresentation
    abstract ConverterType  : ConverterType

    abstract DefaultValueUntyped : obj
    abstract OfFieldUntyped : obj -> AttributeValue
    abstract ToFieldUntyped : AttributeValue -> obj

    member __.IsScalar = 
        match __.Representation with
        | FieldRepresentation.Number
        | FieldRepresentation.String
        | FieldRepresentation.Bytes
        | FieldRepresentation.Bool -> true
        | _ -> false

[<AbstractClass>]
type FieldConverter<'T>() =
    inherit FieldConverter()

    abstract DefaultValue : 'T
    abstract OfField : 'T -> AttributeValue
    abstract ToField : AttributeValue -> 'T

    override __.Type = typeof<'T>
    override __.DefaultValueUntyped = __.DefaultValue :> obj
    override __.OfFieldUntyped o = __.OfField(o :?> 'T)
    override __.ToFieldUntyped av = __.ToField av :> obj

type IFieldConverterResolver =
    abstract Resolve : Type -> FieldConverter
    abstract Resolve<'T> : unit -> FieldConverter<'T>