[<AutoOpen>]
module internal FSharp.DynamoDB.Pickler

open System
open System.IO
open System.Collections.Generic

open Amazon.DynamoDBv2.Model

open FSharp.DynamoDB

type RestObject = Dictionary<string, AttributeValue>

type PickleType =
    | Number        = 01
    | String        = 02
    | Bool          = 03
    | Bytes         = 04
    | StringSet     = 05
    | NumberSet     = 06
    | BytesSet      = 07
    | List          = 08
    | Map           = 09

type PicklerType =
    | Value         = 01
    | Wrapper       = 02
    | Record        = 03
    | Union         = 04
    | Serialized    = 05

[<AbstractClass>]
type Pickler() =
    abstract Type : Type
    abstract PickleType : PickleType
    abstract PicklerType  : PicklerType

    abstract DefaultValueUntyped : obj
    abstract PickleUntyped   : obj -> AttributeValue option
    abstract UnPickleUntyped : AttributeValue -> obj

    abstract Coerce : obj -> AttributeValue option
    default __.Coerce obj = __.PickleUntyped obj

    member __.IsScalar = 
        match __.PickleType with
        | PickleType.Number
        | PickleType.String
        | PickleType.Bytes
        | PickleType.Bool -> true
        | _ -> false

[<AbstractClass>]
type Pickler<'T>() =
    inherit Pickler()

    abstract DefaultValue : 'T
    abstract Pickle   : 'T -> AttributeValue option
    abstract UnPickle : AttributeValue -> 'T

    override __.Type = typeof<'T>
    override __.DefaultValueUntyped = __.DefaultValue :> obj
    override __.PickleUntyped o = __.Pickle(o :?> 'T)
    override __.UnPickleUntyped av = __.UnPickle av :> obj

[<AbstractClass>]
type StringRepresentablePickler<'T>() =
    inherit Pickler<'T>()
    abstract Parse : string -> 'T
    abstract UnParse : 'T -> string

[<AbstractClass>]
type NumRepresentablePickler<'T>() =
    inherit StringRepresentablePickler<'T> ()

type ICollectionPickler =
    abstract ElementConverter : Pickler

type IPicklerResolver =
    abstract Resolve : Type -> Pickler
    abstract Resolve<'T> : unit -> Pickler<'T>

[<AutoOpen>]
module internal PicklerUtils =

    let inline invalidCast (av:AttributeValue) : 'T = 
        let msg = sprintf "could not convert value %A to type '%O'" (av.Print()) typeof<'T>
        raise <| new InvalidCastException(msg)

    let getElemPickler (pickler : Pickler) = (unbox<ICollectionPickler> pickler).ElementConverter

    type UnSupportedType =
        static member Raise(fieldType : Type, ?reason : string) =
            let message = 
                match reason with
                | None -> sprintf "unsupported record field type '%O'" fieldType
                | Some r -> sprintf "unsupported record field type '%O': %s" fieldType r

            raise <| new ArgumentException(message)