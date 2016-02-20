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

type BytesSetConverter<'BSet when 'BSet :> seq<byte[]>>(ctor : seq<byte []> -> 'BSet, nullV) =
    inherit FieldConverter<'BSet>()
    override __.Representation = FieldRepresentation.BytesSet
    override __.ConverterType = ConverterType.Value
    override __.DefaultValue = ctor [||]
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
        if a.NULL then nullV
        elif a.IsBSSet then a.BS |> Seq.map (fun ms -> ms.ToArray()) |> ctor
        else invalidCast a

    interface ICollectionConverter with
        member __.ElementConverter = new BytesConverter() :> _

type NumSetConverter<'Set, 'T when 'Set :> seq<'T>> (ctor : seq<'T> -> 'Set, nullV, tconv : NumRepresentableFieldConverter<'T>) =
    inherit FieldConverter<'Set>()
    override __.DefaultValue = ctor [||]
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
        if a.NULL then nullV
        elif a.IsNSSet then a.NS |> Seq.map tconv.Parse |> ctor
        else invalidCast a

    interface ICollectionConverter with
        member __.ElementConverter = tconv :> _

type StringSetConverter<'Set, 'T when 'Set :> seq<'T>> (ctor : seq<'T> -> 'Set, nullV, tconv : StringRepresentableFieldConverter<'T>) =
    inherit FieldConverter<'Set>()
    override __.DefaultValue = ctor [||]
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
        if a.NULL then nullV
        elif a.IsSSSet then a.SS |> Seq.map tconv.Parse |> ctor
        else invalidCast a

    interface ICollectionConverter with
        member __.ElementConverter = tconv :> _

let mkSetConverter<'Set, 'T when 'Set :> seq<'T>> ctor nullV (tconv : FieldConverter<'T>) : FieldConverter<'Set> =
    match tconv with
    | :? NumRepresentableFieldConverter<'T> as tc -> NumSetConverter<'Set, 'T>(ctor, nullV, tc) :> _
    | :? StringRepresentableFieldConverter<'T> as tc -> StringSetConverter<'Set, 'T>(ctor, nullV, tc) :> _
    | _ -> UnSupportedField.Raise typeof<'Set>

type MapConverter<'Map, 'Value when 'Map :> seq<KeyValuePair<string, 'Value>>>
                    (ctor : seq<KeyValuePair<string, 'Value>> -> 'Map, nullV,
                        vconv : FieldConverter<'Value>) =

    inherit FieldConverter<'Map>()
    override __.Representation = FieldRepresentation.Map
    override __.ConverterType = ConverterType.Value
    override __.DefaultValue = ctor [||]
    override __.Coerce obj =
        match obj with
        | null -> Some <| AttributeValue(NULL = true)
        | :? KeyValuePair<string, 'Value> as kv ->
            match vconv.OfField kv.Value with
            | None -> None
            | Some av -> Some <| AttributeValue(M = cdict [|keyVal kv.Key av|])

        | :? (string * 'Value) as kv ->
            match vconv.OfField (snd kv) with
            | None -> None
            | Some av -> Some <| AttributeValue(M = cdict [|keyVal (fst kv) av|])

        | :? seq<KeyValuePair<string, 'Value>> as kvs ->
            let m = 
                kvs |> Seq.choose (fun kv -> 
                    match vconv.OfField kv.Value with 
                    | None -> None 
                    | Some av -> Some (keyVal kv.Key av)) 
                |> cdict

            if m.Count = 0 then None
            else Some <| AttributeValue(M = m)

        | :? seq<string * 'Value> as kvs ->
            let m = 
                kvs |> Seq.choose (fun (k,v) -> 
                    match vconv.OfField v with 
                    | None -> None 
                    | Some av -> Some (keyVal k av)) 
                |> cdict

            if m.Count = 0 then None
            else Some <| AttributeValue(M = m)

        | _ -> raise <| InvalidCastException()

    override __.OfField map = __.Coerce map

    override __.ToField a =
        if a.NULL then nullV
        elif a.IsMSet then a.M |> Seq.map (fun kv -> keyVal kv.Key (vconv.ToField kv.Value)) |> ctor
        else invalidCast a

    interface ICollectionConverter with
        member __.ElementConverter = vconv :> _