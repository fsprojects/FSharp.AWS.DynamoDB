[<AutoOpen>]
module internal FSharp.AWS.DynamoDB.PropertyMetadata

open System
open System.Reflection

//
//  Pickler metadata for F# type properties
//

[<CustomEquality; NoComparison>]
type PropertyMetadata =
    {
        Name : string
        Index : int
        PropertyInfo : PropertyInfo
        Pickler : Pickler
        NoDefaultValue : bool
        Attributes : Attribute[]
    }
with
    member rp.TryGetAttribute<'Attribute when 'Attribute :> Attribute> () = tryGetAttribute<'Attribute> rp.Attributes
    member rp.GetAttributes<'Attribute when 'Attribute :> Attribute> () = getAttributes<'Attribute> rp.Attributes
    member rp.ContainsAttribute<'Attribute when 'Attribute :> Attribute> () = containsAttribute<'Attribute> rp.Attributes

    override r.Equals o =
        match o with :? PropertyMetadata as r' -> r.PropertyInfo = r'.PropertyInfo | _ -> false

    override r.GetHashCode() = hash r.PropertyInfo

    static member FromPropertyInfo (resolver : IPicklerResolver) (attrId : int) (prop : PropertyInfo) =
        let attributes = prop.GetAttributes()
        let pickler =
            match attributes |> Seq.tryPick (fun a -> match box a with :? IPropertySerializer as ps -> Some ps | _ -> None) with
            | Some serializer -> mkSerializerAttributePickler resolver serializer prop.PropertyType
            | None when attributes |> containsAttribute<StringRepresentationAttribute> ->
                mkStringRepresentationPickler resolver prop
            | None -> resolver.Resolve prop.PropertyType

        let name =
            match attributes |> tryGetAttribute<CustomNameAttribute> with
            | Some cn -> cn.Name
            | None -> prop.Name

        if not <| isValidFieldName name then
            invalidArg name "invalid record field name; must be 1 to 64k long (as utf8)."

        {
            Name = name
            Index = attrId
            PropertyInfo = prop
            Pickler = pickler
            NoDefaultValue = containsAttribute<NoDefaultValueAttribute> attributes
            Attributes = attributes
        }