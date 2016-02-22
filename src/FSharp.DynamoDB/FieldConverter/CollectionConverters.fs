[<AutoOpen>]
module internal FSharp.DynamoDB.FieldConverter.CollectionConverters

open System
open System.Collections
open System.Collections.Generic
open System.IO
open System.Reflection

open Amazon.Util
open Amazon.DynamoDBv2.Model

open FSharp.DynamoDB

type ListConverter<'List, 'T when 'List :> seq<'T>>(ctor : seq<'T> -> 'List, nullV : 'List, tconv : FieldConverter<'T>) =
    inherit FieldConverter<'List>()
    override __.Representation = FieldRepresentation.List
    override __.ConverterType = ConverterType.Value
    override __.DefaultValue = ctor [||]
    override __.Coerce obj =
        match obj with
        | null -> Some <| AttributeValue(NULL = true)
        | :? 'T as t -> 
            match tconv.OfField t with
            | None -> None
            | Some av -> Some <| AttributeValue(L = rlist [|av|])
        | _ ->
            let rl = unbox<seq<'T>> obj |> Seq.choose tconv.OfField |> rlist
            if rl.Count = 0 then None
            else
                Some <| AttributeValue(L = rl)

    override __.OfField list = __.Coerce list

    override __.ToField a =
        if a.NULL then nullV
        elif a.IsLSet then a.L |> Seq.map tconv.ToField |> ctor
        else invalidCast a

    interface ICollectionConverter with
        member __.ElementConverter = tconv :> _

type BytesSetConverter() =
    inherit FieldConverter<Set<byte[]>>()
    override __.Representation = FieldRepresentation.BytesSet
    override __.ConverterType = ConverterType.Value
    override __.DefaultValue = Set.empty
    override __.Coerce obj =
        match obj with
        | null -> Some <| AttributeValue(NULL = true)
        | :? (byte[]) as bs -> 
            if bs.Length = 0 then None
            else
                Some <| AttributeValue(BS = rlist [|new MemoryStream(bs)|])

        | _ ->
            let rl = 
                unbox<seq<byte[]>> obj 
                |> Seq.choose (fun bs -> if bs.Length = 0 then None else Some(new MemoryStream(bs)))
                |> rlist

            if rl.Count = 0 then None
            else
                Some <| AttributeValue(BS = rl)

    override __.OfField bss = __.Coerce bss

    override __.ToField a =
        if a.NULL then Set.empty
        elif a.IsBSSet then a.BS |> Seq.map (fun ms -> ms.ToArray()) |> set
        else invalidCast a

    interface ICollectionConverter with
        member __.ElementConverter = new BytesConverter() :> _

type NumSetConverter<'T when 'T : comparison> (tconv : NumRepresentableFieldConverter<'T>) =
    inherit FieldConverter<Set<'T>>()
    override __.DefaultValue = Set.empty
    override __.Representation = FieldRepresentation.NumberSet
    override __.ConverterType = ConverterType.Value
    override __.Coerce obj =
        match obj with
        | null -> Some <| AttributeValue(NULL = true)
        | :? 'T as t -> Some <| AttributeValue(NS = rlist[|tconv.UnParse t|])
        | _ ->
            let rl = obj |> unbox<seq<'T>> |> Seq.map tconv.UnParse |> rlist
            if rl.Count = 0 then None
            else Some <| AttributeValue(NS = rl)

    override __.OfField set = __.Coerce set

    override __.ToField a =
        if a.NULL then Set.empty
        elif a.IsNSSet then a.NS |> Seq.map tconv.Parse |> set
        else invalidCast a

    interface ICollectionConverter with
        member __.ElementConverter = tconv :> _

type StringSetConverter<'T when 'T : comparison> (tconv : StringRepresentableFieldConverter<'T>) =
    inherit FieldConverter<Set<'T>>()
    override __.DefaultValue = Set.empty
    override __.Representation = FieldRepresentation.StringSet
    override __.ConverterType = ConverterType.Value
    override __.Coerce obj =
        match obj with
        | null -> AttributeValue(NULL = true) |> Some
        | :? 'T as t -> AttributeValue(SS = rlist[|tconv.UnParse t|]) |> Some
        | _ ->
            let rl = obj |> unbox<seq<'T>> |> Seq.map tconv.UnParse |> rlist
            if rl.Count = 0 then AttributeValue(NULL = true) |> Some
            else AttributeValue(SS = rl) |> Some

    override __.OfField set = __.Coerce set

    override __.ToField a =
        if a.NULL then Set.empty
        elif a.IsSSSet then a.SS |> Seq.map tconv.Parse |> set
        else invalidCast a

    interface ICollectionConverter with
        member __.ElementConverter = tconv :> _

let mkSetConverter<'T when 'T : comparison>(tconv : FieldConverter<'T>) : FieldConverter<Set<'T>> =
    if typeof<'T> = typeof<byte[]> then BytesSetConverter() |> unbox else
    match tconv with
    | :? NumRepresentableFieldConverter<'T> as tc -> NumSetConverter<'T>(tc) :> _
    | :? StringRepresentableFieldConverter<'T> as tc -> StringSetConverter<'T>(tc) :> _
    | _ -> UnSupportedField.Raise typeof<Set<'T>>

type MapConverter<'Value>(vconv : FieldConverter<'Value>) =
    inherit FieldConverter<Map<string,'Value>>()
    override __.Representation = FieldRepresentation.Map
    override __.ConverterType = ConverterType.Value
    override __.DefaultValue = Map.empty
    override __.OfField map =
        if isNull map then AttributeValue(NULL = true) |> Some
        elif map.Count = 0 then None
        else
            let m = 
                map 
                |> Seq.choose (fun kv -> 
                    match vconv.OfField kv.Value with 
                    | None -> None 
                    | Some av -> Some (keyVal kv.Key av)) 
                |> cdict

            AttributeValue(M = m) |> Some
            

    override __.ToField a =
        if a.NULL then Map.empty
        elif a.IsMSet then a.M |> Seq.map (fun kv -> kv.Key, vconv.ToField kv.Value) |> Map.ofSeq
        else invalidCast a

    interface ICollectionConverter with
        member __.ElementConverter = vconv :> _