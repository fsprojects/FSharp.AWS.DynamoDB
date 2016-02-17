namespace FSharp.DynamoDB

open System
open System.IO
open System.Runtime.Serialization.Formatters.Binary

open Amazon.DynamoDBv2
open Amazon.DynamoDBv2.Model

[<Sealed; AttributeUsage(AttributeTargets.Property)>]
type HashKeyAttribute() =
    inherit Attribute()

[<Sealed; AttributeUsage(AttributeTargets.Class)>]
type HashKeyConstantAttribute(name : string, hashkey : obj) =
    inherit Attribute()
    do 
        if name = null then raise <| ArgumentNullException("'Name' parameter cannot be null.")
        if hashkey = null then raise <| ArgumentNullException("'HashKey' parameter cannot be null.")

    member __.Name = name
    member __.HashKey = hashkey
    member __.HashKeyType = hashkey.GetType()

[<Sealed; AttributeUsage(AttributeTargets.Property)>]
type RangeKeyAttribute() =
    inherit Attribute()

[<AttributeUsage(AttributeTargets.Property)>]
type CustomNameAttribute(name : string) =
    inherit System.Attribute()
    do if name = null then raise <| ArgumentNullException("'Name' parameter cannot be null.")
    member __.Name = name

[<AttributeUsage(AttributeTargets.Property ||| AttributeTargets.Class)>]
type NoDefaultValueAttribute() =
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
    member __.RangeKey = rangeKey
    member __.IsRangeKeySpecified = not <| obj.ReferenceEquals(rangeKey, null)
    member __.IsHashKeySpecified = not <| obj.ReferenceEquals(hashKey, null)
    member private __.Format =
        match rangeKey with
        | null -> sprintf "{ HashKey = %A }" hashKey
        | rk -> 
            match hashKey with
            | null -> sprintf "{ RangeKey = %A }" rk
            | hk -> sprintf "{ HashKey = %A ; RangeKey = %A }" hk rk

    override __.ToString() = __.Format

    static member Hash<'HashKey>(hashKey : 'HashKey) = 
        if obj.ReferenceEquals(hashKey, null) then raise <| new ArgumentNullException("HashKey must not be null")
        TableKey(hashKey, null)

    static member Range<'RangeKey>(rangeKey : 'RangeKey) =
        if obj.ReferenceEquals(rangeKey, null) then raise <| new ArgumentNullException("RangeKey must not be null")
        TableKey(null, rangeKey)

    static member Combined<'HashKey, 'RangeKey>(hashKey : 'HashKey, rangeKey : 'RangeKey) = 
        if obj.ReferenceEquals(hashKey, null) then raise <| new ArgumentNullException("HashKey must not be null")
        new TableKey(hashKey, rangeKey)

[<AutoOpen>]
module QueryExtensions =

    type Set<'T when 'T : comparison> with
        member s.Add(ts : seq<'T>) =
            Seq.append s ts |> Set.ofSeq

        member s.Remove(ts : seq<'T>) = s - set ts

    [<RequireQualifiedAccess>]
    module Set =
        let addSeq (ts : seq<'T>) (s : Set<'T>) = s.Add ts
        let removeSeq (ts : seq<'T>) (s : Set<'T>) = s.Remove ts