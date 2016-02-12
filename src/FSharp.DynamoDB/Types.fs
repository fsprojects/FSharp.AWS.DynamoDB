namespace FSharp.DynamoDB

open System
open System.IO
open System.Runtime.Serialization.Formatters.Binary

open Amazon.DynamoDBv2
open Amazon.DynamoDBv2.Model

[<Sealed; AttributeUsage(AttributeTargets.Property)>]
type HashKeyAttribute() =
    inherit Attribute()

[<Sealed; AttributeUsage(AttributeTargets.Property)>]
type RangeKeyAttribute() =
    inherit Attribute()

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

type KeySchema = 
    {
        AttributeName : string
        KeyType : ScalarAttributeType 
    }

type TableKeySchema = 
    { 
        HashKey : KeySchema
        RangeKey : KeySchema option 
    }

[<Struct; StructuredFormatDisplay("{Format}")>]
type TableKey private (hashKey : obj, rangeKey : obj) =
    member __.HashKey = hashKey
    member __.IsRangeKeySpecified = not <| obj.ReferenceEquals(rangeKey, null)
    member __.RangeKey = if obj.ReferenceEquals(rangeKey, null) then None else Some rangeKey
    member internal __.RangeKeyInternal = rangeKey
    member private __.Format =
        match rangeKey with
        | null -> sprintf "{ HashKey = %O }" hashKey
        | rk -> sprintf "{ HashKey = %O ; RangeKey = %O }" hashKey rangeKey

    override __.ToString() = __.Format

    static member Hash<'HashKey>(hashKey : 'HashKey) = 
        if obj.ReferenceEquals(hashKey, null) then raise <| new ArgumentNullException("HashKey must not be null")
        TableKey(hashKey, null)

    static member Combined<'HashKey, 'RangeKey>(hashKey : 'HashKey, rangeKey : 'RangeKey) = 
        if obj.ReferenceEquals(hashKey, null) then raise <| new ArgumentNullException("HashKey must not be null")
        new TableKey(hashKey, rangeKey)