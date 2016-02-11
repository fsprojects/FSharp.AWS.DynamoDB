namespace FSharp.DynamoDB

open System
open System.IO
open System.Runtime.Serialization.Formatters.Binary

[<Sealed>]
type HashKeyAttribute() =
    inherit Attribute()

[<Sealed>]
type RangeKeyAttribute() =
    inherit Attribute()

type KeyAttributeType = 
    | String 
    | Bytes 
    | Number

type KeySchema = 
    { 
        AttributeName : string
        KeyType : KeyAttributeType 
    }

type TableKeySchema = 
    { 
        HashKey : KeySchema
        RangeKey : KeySchema option 
    }

[<AttributeUsage(AttributeTargets.Property)>]
type CustomNameAttribute(name : string) =
    inherit System.Attribute()
    member __.Name = name

[<AttributeUsage(AttributeTargets.Property ||| AttributeTargets.Class)>]
type AllowDefaultValueAttribute() =
    inherit System.Attribute()

[<AbstractClass; AttributeUsage(AttributeTargets.Class ||| AttributeTargets.Property)>]
type PropertySerializerAttribute() =
    inherit Attribute()
    abstract PickleType : Type
    abstract SerializeUntyped : value:obj -> obj
    abstract DeserializeUntyped : pickle:obj -> obj

[<AbstractClass; AttributeUsage(AttributeTargets.Class ||| AttributeTargets.Property)>]
type PropertySerializerAttribute<'PickleType>() =
    inherit PropertySerializerAttribute()
    override __.PickleType = typeof<'PickleType>
    override __.SerializeUntyped value = __.Serialize value :> obj
    override __.DeserializeUntyped pickle = __.Deserialize (pickle :?> 'PickleType)

    abstract Serialize   : value:obj -> 'PickleType
    abstract Deserialize : pickle:'PickleType -> obj

[<AttributeUsage(AttributeTargets.Class ||| AttributeTargets.Property)>]
type BinaryFormatterAttribute() =
    inherit PropertySerializerAttribute<byte[]>()

    override __.Serialize(value:obj) =
        let bfs = new BinaryFormatter()
        use m = new MemoryStream()
        bfs.Serialize(m, value)
        m.ToArray()

    override __.Deserialize(pickle : byte[]) =
        let bfs = new BinaryFormatter()
        use m = new MemoryStream(pickle)
        bfs.Deserialize(m)