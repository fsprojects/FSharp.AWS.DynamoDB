[<AutoOpen>]
module internal FSharp.AWS.DynamoDB.Pickler

open System
open System.Collections.Generic

open Amazon.DynamoDBv2.Model

open FSharp.AWS.DynamoDB

type RestObject = Dictionary<string, AttributeValue>

/// Pickle representation type in an AttributeValue instance
type PickleType =
    | Number = 01
    | String = 02
    | Bool = 03
    | Bytes = 04
    | StringSet = 05
    | NumberSet = 06
    | BytesSet = 07
    | List = 08
    | Map = 09

/// Pickler implementation type
type PicklerType =
    | Value = 01
    | Wrapper = 02
    | Record = 03
    | Union = 04
    | Serialized = 05
    | Enum = 06

/// Untyped pickler base class
[<AbstractClass>]
type Pickler() =
    /// Type that the given instance is responsible for pickling
    abstract Type: Type
    /// Type used for pickling value in DynamoDB
    abstract PickleType: PickleType
    /// Type of pickler that was generated
    abstract PicklerType: PicklerType

    /// Default value to be used in case of missing attribute in table
    abstract DefaultValueUntyped: obj
    /// Pickle value to AttributeValue instance, if applicable
    abstract PickleUntyped: obj -> AttributeValue option
    /// UnPickle value from AttributeValue instance
    abstract UnPickleUntyped: AttributeValue -> obj

    /// Pickle any object, making an effort to coerce it to current pickler type
    abstract PickleCoerced: obj -> AttributeValue option
    default __.PickleCoerced obj = __.PickleUntyped obj

    /// True if DynamoDB representation preserves
    /// comparison semantics for query expressions
    abstract IsComparable: bool
    default __.IsComparable = false

    /// True if scalar DynamoDB instance
    member __.IsScalar =
        match __.PickleType with
        | PickleType.Number
        | PickleType.String
        | PickleType.Bytes
        | PickleType.Bool -> true
        | _ -> false

    override p.Equals o =
        match o with
        | :? Pickler as p' -> p.Type = p'.Type && p.PicklerType = p'.PicklerType
        | _ -> false

    override p.GetHashCode() = hash2 p.Type p.PicklerType

/// Typed pickler base class
[<AbstractClass>]
type Pickler<'T>() =
    inherit Pickler()

    /// Default value to be used in case of missing attribute in table
    abstract DefaultValue: 'T
    /// Pickle value to AttributeValue instance, if applicable
    abstract Pickle: 'T -> AttributeValue option
    /// UnPickle value from AttributeValue instance
    abstract UnPickle: AttributeValue -> 'T

    override __.Type = typeof<'T>
    override __.DefaultValueUntyped = __.DefaultValue :> obj
    override __.PickleUntyped o = __.Pickle (o :?> 'T)
    override __.UnPickleUntyped av = __.UnPickle av :> obj

/// Represent a pickler instance that can naturally represent
/// its values as strings. E.g. Guid, DateTimeOffset, number types.
[<AbstractClass>]
type StringRepresentablePickler<'T>() =
    inherit Pickler<'T>()
    abstract Parse: string -> 'T
    abstract UnParse: 'T -> string

/// Represent a pickler instance that can naturally represent
/// its values as numbers. E.g. numbers & enumerations.
[<AbstractClass>]
type NumRepresentablePickler<'T>() =
    inherit StringRepresentablePickler<'T>()

/// Picklers of collections should implement this interface
type ICollectionPickler =
    abstract ElementPickler: Pickler

/// Interface used for generating combined picklers
type IPicklerResolver =
    abstract Resolve: Type -> Pickler
    abstract Resolve<'T> : unit -> Pickler<'T>


//
//  Common pickler utilities
//

let inline invalidCast (av: AttributeValue) : 'T =
    let msg = sprintf "could not convert value %A to type '%O'" (av.Print ()) typeof<'T>
    raise <| new InvalidCastException (msg)

let getElemPickler (pickler: Pickler) = (unbox<ICollectionPickler> pickler).ElementPickler

type UnSupportedType =
    static member Raise(fieldType: Type, ?reason: string) =
        let message =
            match reason with
            | None -> sprintf "unsupported record field type '%O'" fieldType
            | Some r -> sprintf "unsupported record field type '%O': %s" fieldType r

        raise <| new ArgumentException (message)
