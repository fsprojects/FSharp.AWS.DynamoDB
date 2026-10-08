[<AutoOpen>]
module internal FSharp.AWS.DynamoDB.RecordPickler

open System.Collections.Generic

open Microsoft.FSharp.Reflection

open Amazon.DynamoDBv2.Model

open FSharp.AWS.DynamoDB

//
//  Pickler implementation for F# record types
//

type IRecordPickler =
    abstract Properties: PropertyMetadata[]

type RecordPickler<'T>(ctor: obj[] -> obj, properties: PropertyMetadata[]) =
    inherit Pickler<'T>()

    member __.Properties = properties
    member __.OfRecord(value: 'T) : RestObject =
        let values = new RestObject()
        for prop in properties do
            let field = prop.PropertyInfo.GetValue value
            match prop.Pickler.PickleUntyped field with
            | None -> ()
            | Some av when not (values.ContainsKey prop.Name) -> values.Add(prop.Name, av)
            | Some _ -> ()

        values

    member __.ToRecord(ro: RestObject) : 'T =
        let values = Array.zeroCreate<obj> properties.Length
        for i = 0 to properties.Length - 1 do
            let prop = properties.[i]
            let notFound () = raise <| new KeyNotFoundException(sprintf "attribute %A not found." prop.Name)
            let ok, av = ro.TryGetValue prop.Name
            if ok then values.[i] <- prop.Pickler.UnPickleUntyped av
            elif prop.NoDefaultValue then notFound ()
            else values.[i] <- prop.Pickler.DefaultValueUntyped

        ctor values :?> 'T

    interface IRecordPickler with
        member __.Properties = properties

    override __.PicklerType = PicklerType.Record
    override __.PickleType = PickleType.Map
    override __.DefaultValue =
        let defaultFields = properties |> Array.map (fun p -> p.Pickler.DefaultValueUntyped)
        ctor defaultFields :?> 'T

    override __.Pickle(record: 'T) =
        let ro = __.OfRecord record
        if ro.Count = 0 then
            None
        else
            Some <| AttributeValue(M = ro)

    override __.UnPickle a = if a.IsMSet then __.ToRecord a.M else invalidCast a

type PropertyMetadata with

    member rp.NestedRecord =
        match box rp.Pickler with
        | :? IRecordPickler as rp -> Some rp.Properties
        | _ -> None


let mkTuplePickler<'T> (resolver: IPicklerResolver) =
    let ctor = FSharpValue.PreComputeTupleConstructor typeof<'T>
    let properties = typeof<'T>.GetProperties() |> Array.mapi (PropertyMetadata.FromPropertyInfo resolver)
    new RecordPickler<'T>(ctor, properties)

let mkFSharpRecordPickler<'T> (resolver: IPicklerResolver) =
    let ctor = FSharpValue.PreComputeRecordConstructor(typeof<'T>, true)
    let properties =
        FSharpType.GetRecordFields(typeof<'T>, true)
        |> Array.mapi (PropertyMetadata.FromPropertyInfo resolver)
    new RecordPickler<'T>(ctor, properties)
