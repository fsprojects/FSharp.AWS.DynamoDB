namespace Rql.Core

open System
open System.IO
open System.Reflection
open System.Collections.Generic
open System.Runtime.Serialization.Formatters.Binary

type RecordFields = IDictionary<string, obj>

[<AbstractClass; AttributeUsage(AttributeTargets.Class ||| AttributeTargets.Property)>]
type PropertySerializerAttribute() =
    inherit System.Attribute()
    abstract PickleType : Type
    abstract Serialize   : value:obj -> obj
    abstract Deserialize : pickle:obj -> obj

type BinaryFormatterAttribute() =
    inherit PropertySerializerAttribute()

    override __.PickleType = typeof<byte []>
    override __.Serialize(value:obj) =
        let bfs = new BinaryFormatter()
        use m = new MemoryStream()
        bfs.Serialize(m, value)
        m.ToArray() :> obj

    override __.Deserialize(pickle:obj) =
        let bfs = new BinaryFormatter()
        let stream =
            match pickle with
            | :? Stream as s -> s
            | :? (byte []) as bytes -> new MemoryStream(bytes) :> Stream
            | _ -> raise <| new InvalidCastException()

        bfs.Deserialize(stream)

[<AttributeUsage(AttributeTargets.Property)>]
type CustomNameAttribute(name : string) =
    inherit System.Attribute()
    member __.Name = name

[<AttributeUsage(AttributeTargets.Property ||| AttributeTargets.Class)>]
type AllowDefaultValueAttribute() =
    inherit System.Attribute()

[<NoEquality; NoComparison>]
type RecordProperty =
    {
        Name : string
        PropertyInfo : PropertyInfo
        UnderlyingType : Type
        Shape : TypeShape
        AllowDefaultValue : bool
        Serializer : PropertySerializerAttribute option
        Attributes : Attribute []
    }
with
    member rp.TryGetAttribute<'Attribute when 'Attribute :> Attribute> () = tryGetAttribute<'Attribute> rp.Attributes
    member rp.GetAttributes<'Attribute when 'Attribute :> Attribute> () = getAttributes<'Attribute> rp.Attributes
    member rp.ContainsAttribute<'Attribute when 'Attribute :> Attribute> () = containsAttribute<'Attribute> rp.Attributes

[<NoEquality; NoComparison>]
type internal RecordInfo =
    {
        Properties : RecordProperty []
        ConstructorInfo : ConstructorInfo
    }

[<NoEquality; NoComparison>]
type Comparand =
    | Property of RecordProperty
    | Value of obj

[<NoEquality; NoComparison>]
type QueryExpr =
    | False
    | True
    | Not of QueryExpr
    | And of QueryExpr * QueryExpr
    | Or of QueryExpr * QueryExpr
    | BooleanProperty of RecordProperty
    | Predicate of MethodInfo * Comparand list