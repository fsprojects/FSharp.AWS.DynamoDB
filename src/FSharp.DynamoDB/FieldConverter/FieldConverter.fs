namespace FSharp.DynamoDB.FieldConverter

open System
open System.IO
open System.Collections.Generic

open Amazon.DynamoDBv2.Model

open FSharp.DynamoDB

type RestObject = Dictionary<string, AttributeValue>

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

[<AbstractClass>]
type FieldConverter() =
    abstract Type : Type
    abstract Representation : FieldRepresentation
    abstract ConverterType  : ConverterType

    abstract DefaultValueUntyped : obj
    abstract OfFieldUntyped : obj -> AttributeValue option
    abstract ToFieldUntyped : AttributeValue -> obj

    abstract Coerce : obj -> AttributeValue option
    default __.Coerce obj = __.OfFieldUntyped obj

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
    abstract OfField : 'T -> AttributeValue option
    abstract ToField : AttributeValue -> 'T

    override __.Type = typeof<'T>
    override __.DefaultValueUntyped = __.DefaultValue :> obj
    override __.OfFieldUntyped o = __.OfField(o :?> 'T)
    override __.ToFieldUntyped av = __.ToField av :> obj

[<AbstractClass>]
type StringRepresentableFieldConverter<'T>() =
    inherit FieldConverter<'T>()
    abstract Parse : string -> 'T
    abstract UnParse : 'T -> string

[<AbstractClass>]
type NumRepresentableFieldConverter<'T>() =
    inherit StringRepresentableFieldConverter<'T> ()

type ICollectionConverter =
    abstract ElementConverter : FieldConverter

type IFieldConverterResolver =
    abstract Resolve : Type -> FieldConverter
    abstract Resolve<'T> : unit -> FieldConverter<'T>

[<AutoOpen>]
module internal FieldConveterUtils =

    let inline invalidCast (av:AttributeValue) : 'T = 
        let msg = sprintf "could not convert value %A to type '%O'" (av.Print()) typeof<'T>
        raise <| new InvalidCastException(msg)

    let getEconv (conv : FieldConverter) = (unbox<ICollectionConverter> conv).ElementConverter

    type UnSupportedField =
        static member Raise(fieldType : Type, ?reason : string) =
            let message = 
                match reason with
                | None -> sprintf "unsupported record field type '%O'" fieldType
                | Some r -> sprintf "unsupported record field type '%O': %s" fieldType r

            raise <| new ArgumentException(message)