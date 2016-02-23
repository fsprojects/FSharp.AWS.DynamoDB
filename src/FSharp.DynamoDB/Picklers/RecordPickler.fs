[<AutoOpen>]
module internal FSharp.DynamoDB.RecordPickler

open System
open System.Text.RegularExpressions
open System.Collections
open System.Collections.Generic
open System.IO
open System.Reflection

open Microsoft.FSharp.Reflection

open Amazon.Util
open Amazon.DynamoDBv2.Model

open FSharp.DynamoDB

let private fieldNameRegex = new Regex("^[0-9a-zA-Z]+", RegexOptions.Compiled)
let isValidFieldName (name : string) =
    fieldNameRegex.IsMatch name && not <| Char.IsDigit name.[0]   
    

[<CustomEquality; NoComparison>]
type RecordInfo =
    {
        Type : Type
        ConstructorInfo : ConstructorInfo
        Properties : RecordPropertyInfo []
    }
with
    override r.Equals o =
        match o with :? RecordInfo as r' -> r.Type = r'.Type | _ -> false

    override r.GetHashCode() = hash r.Type

and [<CustomEquality; NoComparison>] 
  RecordPropertyInfo =
    {
        Name : string
        Index : int
        PropertyInfo : PropertyInfo
        Pickler : Pickler
        NoDefaultValue : bool
        IsHashKey : bool
        IsRangeKey : bool
        NestedRecord : RecordInfo option
        Attributes : Attribute[]
    }
with
    member rp.TryGetAttribute<'Attribute when 'Attribute :> Attribute> () = tryGetAttribute<'Attribute> rp.Attributes
    member rp.GetAttributes<'Attribute when 'Attribute :> Attribute> () = getAttributes<'Attribute> rp.Attributes
    member rp.ContainsAttribute<'Attribute when 'Attribute :> Attribute> () = containsAttribute<'Attribute> rp.Attributes
    member rp.IsNestedRecord = Option.isSome rp.NestedRecord

    override r.Equals o =
        match o with :? RecordPropertyInfo as r' -> r.PropertyInfo = r'.PropertyInfo | _ -> false

    override r.GetHashCode() = hash r.PropertyInfo

    static member FromPropertyInfo (resolver : IPicklerResolver) (attrId : int) (prop : PropertyInfo) =
        let attributes = prop.GetAttributes()
        let pickler = 
            match tryGetAttribute<PropertySerializerAttribute> attributes with
            | Some serializer -> new SerializerAttributePickler(prop, serializer, resolver) :> Pickler
            | None -> resolver.Resolve prop.PropertyType

        let name =
            match attributes |> tryGetAttribute<CustomNameAttribute> with
            | Some cn -> cn.Name
            | None -> prop.Name

        if not <| isValidFieldName name then
            invalidArg name "invalid record field name; must be alphanumeric and should not begin with a number."

        {
            Name = name
            Index = attrId
            PropertyInfo = prop
            Pickler = pickler
            IsHashKey = containsAttribute<HashKeyAttribute> attributes
            IsRangeKey = containsAttribute<RangeKeyAttribute> attributes
            NoDefaultValue = containsAttribute<NoDefaultValueAttribute> attributes
            NestedRecord = match box pickler with :? IRecordPickler as rc -> Some rc.RecordInfo | _ -> None
            Attributes = attributes
        }

and IRecordPickler =
    abstract RecordInfo : RecordInfo

type RecordPickler<'T>(ctorInfo : ConstructorInfo, properties : RecordPropertyInfo []) =
    inherit Pickler<'T> ()

    let recordInfo = { Type = typeof<'T> ; Properties = properties ; ConstructorInfo = ctorInfo }

    member __.RecordInfo = recordInfo
    member __.OfRecord (value : 'T) : RestObject =
        let values = new RestObject()
        for prop in properties do
            let field = prop.PropertyInfo.GetValue value
            match prop.Pickler.PickleUntyped field with
            | None -> ()
            | Some av -> values.Add(prop.Name, av)

        values

    member __.ToRecord (ro : RestObject) : 'T =
        let values = Array.zeroCreate<obj> properties.Length
        for i = 0 to properties.Length - 1 do
            let prop = properties.[i]
            let notFound() = raise <| new KeyNotFoundException(sprintf "attribute %A not found." prop.Name)
            let ok, av = ro.TryGetValue prop.Name
            if ok then values.[i] <- prop.Pickler.UnPickleUntyped av
            elif prop.NoDefaultValue then notFound()
            else values.[i] <- prop.Pickler.DefaultValueUntyped

        ctorInfo.Invoke values :?> 'T

    interface IRecordPickler with
        member __.RecordInfo = recordInfo

    override __.PicklerType = PicklerType.Record
    override __.PickleType = PickleType.Map
    override __.DefaultValue = invalidOp <| sprintf "default values not supported for records."

    override __.Pickle (record : 'T) =
        let ro = __.OfRecord record 
        if ro.Count = 0 then None
        else Some <| AttributeValue(M = ro)

    override __.UnPickle a =
        if a.IsMSet then __.ToRecord a.M
        else invalidCast a

let mkTuplePickler<'T> (resolver : IPicklerResolver) =
    let ctor, rest = FSharpValue.PreComputeTupleConstructorInfo(typeof<'T>)
    if Option.isSome rest then invalidArg (string typeof<'T>) "Tuples of arity > 7 not supported"
    let properties = typeof<'T>.GetProperties() |> Array.mapi (RecordPropertyInfo.FromPropertyInfo resolver)
    new RecordPickler<'T>(ctor, properties)

let mkFSharpRecordPickler<'T> (resolver : IPicklerResolver) =
    let ctor = FSharpValue.PreComputeRecordConstructorInfo(typeof<'T>, true)
    let properties = FSharpType.GetRecordFields(typeof<'T>, true) |> Array.mapi (RecordPropertyInfo.FromPropertyInfo resolver)
    let mkRecord values = ctor.Invoke values :?> 'T
    new RecordPickler<'T>(ctor, properties)