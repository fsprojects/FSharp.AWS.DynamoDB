namespace Rql.Core

open System
open System.Collections.Generic
open System.Reflection

open Microsoft.FSharp.Reflection

module internal RecordBuilder =

    let getUnderlyingType (serializer : PropertySerializerAttribute option) (shape : TypeShape) =
        match serializer with
        | Some s -> s.PickleType
        | None ->

        shape.Accept
            { new ITypeShapeVisitor<Type> with
                  member x.VisitType<'T>(): Type = typeof<'T>
                  member x.VisitNullable<'T when 'T : (new : unit -> 'T) and 'T :> ValueType and 'T : struct>(): Type = typeof<'T>
                  member x.VisitFSharpOption<'T>(): Type = typeof<'T>
                  member x.VisitFSharpRef<'T>(): Type = typeof<'T>
                  member x.VisitFSharpList<'T>(): Type = typeof<'T []>
                  member x.VisitFSharpSet<'T when 'T : comparison> (): Type = typeof<'T []>
                  member x.VisitFSharpMap<'K, 'V when 'K : comparison> () : Type = typeof<KeyValuePair<'K, 'V> []>
            }

    let mkRecordProperty (propertyInfo : PropertyInfo) =
        let shape = TypeShape.infer propertyInfo.PropertyType
        let attributes = propertyInfo.GetAttributes()
        let serializer = tryGetAttribute<PropertySerializerAttribute> attributes 
        let name =
            match attributes |> tryGetAttribute<CustomNameAttribute> with
            | Some cn -> cn.Name
            | None -> propertyInfo.Name

        let allowDefaultValue = containsAttribute<AllowDefaultValueAttribute> attributes
        let underlyingType = getUnderlyingType serializer shape

        {
            Name = name
            PropertyInfo = propertyInfo
            Shape = shape
            UnderlyingType = underlyingType
            Serializer = serializer
            Attributes = attributes
            AllowDefaultValue = allowDefaultValue
        }

    let mkRecordInfo (recordType : Type) =
        let properties = FSharpType.GetRecordFields(recordType, true) |> Array.map mkRecordProperty
        let ctorInfo = FSharpValue.PreComputeRecordConstructorInfo (recordType, true)
        { Properties = properties ; ConstructorInfo = ctorInfo }

    let writeFieldData (writeCont : obj -> unit) (rp : RecordProperty) (value:obj) =
        match rp.Serializer with
        | Some s -> writeCont <| s.Serialize(value)
        | None ->
            rp.Shape.Accept 
                { new ITypeShapeVisitor<bool> with
                    member __.VisitNullable<'T when 'T : (new : unit -> 'T) and 'T :> ValueType and 'T : struct>() = 
                        if not <| obj.ReferenceEquals(value, null) then writeCont value
                        true
                    member __.VisitFSharpOption<'T>() =
                        match value :?> 'T option with Some v -> writeCont v | None -> () 
                        true
                    member __.VisitFSharpRef<'T>() = 
                        !(value :?> 'T ref) |> writeCont ; true
                    member __.VisitFSharpList<'T>() = 
                        value :?> 'T list |> Seq.toArray |> writeCont ; true
                    member __.VisitFSharpMap<'K, 'V when 'K : comparison>() = 
                        value :?> Map<'K,'V> |> Seq.toArray |> writeCont ; true
                    member __.VisitFSharpSet<'T when 'T : comparison>() = 
                        value :?> Set<'T> |> Set.toArray |> writeCont ; true
                    member __.VisitType<'T>() = 
                        writeCont value ; true
                }

            |> ignore

    let extractRecordFields (info : RecordInfo) (record : 'TRecord) : RecordFields =
        let o = box record
        let d = new Dictionary<string, obj>()
        for p in info.Properties do
            let value = p.PropertyInfo.GetValue(o)
            writeFieldData (fun o -> d.Add(p.Name, o)) p value

        d :> RecordFields

    let rebuildRecord (info : RecordInfo) (fields : RecordFields) =
        let ctorParams = new ResizeArray<obj>()
        for p in info.Properties do
            let append x = ctorParams.Add x
            match p.Serializer with
            | Some s -> fields.[p.Name] |> s.Deserialize |> append
            | None ->
                let ok, found = fields.TryGetValue p.Name
                ignore(
                    p.Shape.Accept 
                        { new ITypeShapeVisitor<bool> with
                            member __.VisitFSharpOption<'T>() =
                                if ok then
                                    match found with
                                    | :? 'T as t -> Some t |> append
                                    | :? ('T option) as topt -> topt |> append
                                    | _ -> found :?> 'T |> ignore // simply force exception here
                                else
                                    append None
                                true

                            member __.VisitFSharpRef<'T>() =
                                if ok then
                                    match found with
                                    | :? 'T as t -> ref t |> append
                                    | :? ('T ref) as tref -> tref |> append
                                    | _ -> found :?> 'T |> ignore // simply force exception here
                                elif p.AllowDefaultValue then
                                    ref Unchecked.defaultof<'T> |> append
                                else
                                    raise <| KeyNotFoundException(p.Name)

                                false

                            member __.VisitNullable<'T when 'T : (new : unit -> 'T) and 'T :> ValueType and 'T : struct>() = 
                                if ok then
                                    match found with
                                    | null -> append null
                                    | _ -> append (found :?> 'T)
                                else
                                    append null

                                true

                            member __.VisitFSharpList<'T>() =
                                if ok then Seq.toList (found :?> seq<'T>) |> append
                                else append List.empty<'T>
                                true

                            member __.VisitFSharpMap<'K, 'V when 'K : comparison>() = 
                                if ok then 
                                    match found with
                                    | :? seq<'K * 'V> as kv -> Map.ofSeq kv |> append
                                    | :? seq<KeyValuePair<'K, 'V>> as kv -> kv |> Seq.map(fun kv -> kv.Key, kv.Value) |> Map.ofSeq |> append
                                    | _ -> found :?> seq<'K * 'V> |> ignore // simply force exception
                                else
                                    append Map.empty<'K,'V>
                                true


                            member __.VisitFSharpSet<'T when 'T : comparison>() = 
                                if ok then
                                    Set.ofSeq (found :?> seq<'T>) |> append
                                else
                                    append Set.empty<'T>
                                false

                            member __.VisitType<'T>() = 
                                if ok then append (found :?> 'T)
                                elif p.AllowDefaultValue then ref Unchecked.defaultof<'T> |> append
                                else raise <| new KeyNotFoundException(p.Name)
                                true
                        })

        info.ConstructorInfo.Invoke (ctorParams.ToArray()) :?> 'TRecord