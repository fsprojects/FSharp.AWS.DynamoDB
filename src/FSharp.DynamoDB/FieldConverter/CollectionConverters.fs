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

type ListConverter<'List, 'T when 'List :> seq<'T>>(ctor : seq<'T> -> 'List, tconv : FieldConverter<'T>) =
    inherit FieldConverter<'List>()
    override __.Representation = FieldRepresentation.List
    override __.ConverterType = ConverterType.Value
    override __.DefaultValue = ctor [||]
    override __.Coerce obj =
        match obj with
        | null -> AttributeValue(NULL = true)
        | :? 'T as t -> AttributeValue(L = rlist [|tconv.OfField t|])
        | _ ->
            let rl = unbox<seq<'T>> obj |> Seq.map tconv.OfField |> rlist
            if rl.Count = 0 then AttributeValue(NULL = true)
            else
                AttributeValue(L = rl)

    override __.OfField list = __.Coerce list

    override __.ToField a =
        if a.NULL then ctor [||]
        elif a.IsLSet then a.L |> Seq.map tconv.ToField |> ctor
        else invalidCast a

    interface ICollectionConverter with
        member __.ElementConverter = tconv :> _

type BytesSetConverter<'BSet when 'BSet :> seq<byte[]>>(ctor : seq<byte []> -> 'BSet) =
    inherit FieldConverter<'BSet>()
    override __.Representation = FieldRepresentation.BytesSet
    override __.ConverterType = ConverterType.Value
    override __.DefaultValue = ctor [||]
    override __.Coerce obj =
        match obj with
        | null -> AttributeValue(NULL = true)
        | :? (byte[]) as bs -> AttributeValue(BS = rlist [|new MemoryStream(bs)|])
        | _ ->
            let rl = unbox<seq<byte[]>> obj |> Seq.map (fun bs -> new MemoryStream(bs)) |> rlist
            if rl.Count = 0 then AttributeValue(NULL = true)
            else
                AttributeValue(BS = rl)

    override __.OfField bss = __.Coerce bss

    override __.ToField a =
        if a.NULL then ctor [||]
        elif a.IsBSSet then a.BS |> Seq.map (fun ms -> ms.ToArray()) |> ctor
        else invalidCast a

    interface ICollectionConverter with
        member __.ElementConverter = new BytesConverter() :> _

type NumSetConverter<'Set, 'T when 'Set :> seq<'T>> (ctor : seq<'T> -> 'Set, tconv : NumRepresentableFieldConverter<'T>) =
    inherit FieldConverter<'Set>()
    override __.DefaultValue = ctor [||]
    override __.Representation = FieldRepresentation.NumberSet
    override __.ConverterType = ConverterType.Value
    override __.Coerce obj =
        match obj with
        | null -> AttributeValue(NULL = true)
        | :? 'T as t -> AttributeValue(NS = rlist[|tconv.UnParse t|])
        | _ ->
            let rl = obj |> unbox<seq<'T>> |> Seq.map tconv.UnParse |> rlist
            if rl.Count = 0 then AttributeValue(NULL = true)
            else AttributeValue(NS = rl)

    override __.OfField set = __.Coerce set

    override __.ToField a =
        if a.NULL then ctor [||]
        elif a.IsNSSet then a.NS |> Seq.map tconv.Parse |> ctor
        else invalidCast a

    interface ICollectionConverter with
        member __.ElementConverter = tconv :> _

type StringSetConverter<'Set, 'T when 'Set :> seq<'T>> (ctor : seq<'T> -> 'Set, tconv : StringRepresentableFieldConverter<'T>) =
    inherit FieldConverter<'Set>()
    override __.DefaultValue = ctor [||]
    override __.Representation = FieldRepresentation.StringSet
    override __.ConverterType = ConverterType.Value
    override __.Coerce obj =
        match obj with
        | null -> AttributeValue(NULL = true)
        | :? 'T as t -> AttributeValue(SS = rlist[|tconv.UnParse t|])
        | _ ->
            let rl = obj |> unbox<seq<'T>> |> Seq.map tconv.UnParse |> rlist
            if rl.Count = 0 then AttributeValue(NULL = true)
            else AttributeValue(SS = rl)

    override __.OfField set = __.Coerce set

    override __.ToField a =
        if a.NULL then ctor [||]
        elif a.IsSSSet then a.SS |> Seq.map tconv.Parse |> ctor
        else invalidCast a

    interface ICollectionConverter with
        member __.ElementConverter = tconv :> _

let mkSetConverter<'Set, 'T when 'Set :> seq<'T>> ctor (tconv : FieldConverter<'T>) : FieldConverter<'Set> =
    match tconv with
    | :? NumRepresentableFieldConverter<'T> as tc -> NumSetConverter<'Set, 'T>(ctor, tc) :> _
    | :? StringRepresentableFieldConverter<'T> as tc -> StringSetConverter<'Set, 'T>(ctor, tc) :> _
    | _ -> UnSupportedField.Raise typeof<'Set>

type MapConverter<'Map, 'Value when 'Map :> seq<KeyValuePair<string, 'Value>>>
                    (ctor : seq<KeyValuePair<string, 'Value>> -> 'Map, isNullOrEmpty : 'Map -> bool,
                        vconv : FieldConverter<'Value>) =

    inherit FieldConverter<'Map>()
    override __.Representation = FieldRepresentation.Map
    override __.ConverterType = ConverterType.Value
    override __.DefaultValue = ctor [||]
    override __.Coerce obj =
        match obj with
        | null -> AttributeValue(NULL = true)
        | :? KeyValuePair<string, 'Value> as kv -> 
            AttributeValue(M = cdict [|keyVal kv.Key (vconv.OfField kv.Value)|])

        | :? (string * 'Value) as kv -> 
            AttributeValue(M = cdict [|keyVal (fst kv) (vconv.OfField (snd kv))|])

        | :? seq<KeyValuePair<string, 'Value>> as kvs ->
            let m = kvs |> Seq.map (fun kv -> keyVal kv.Key (vconv.OfField kv.Value)) |> cdict
            if m.Count = 0 then AttributeValue(NULL = true)
            else AttributeValue(M = m)

        | :? seq<string * 'Value> as kvs ->
            let m = kvs |> Seq.map (fun (k,v) -> keyVal k (vconv.OfField v)) |> cdict
            if m.Count = 0 then AttributeValue(NULL = true)
            else AttributeValue(M = m)

        | _ -> raise <| InvalidCastException()

    override __.OfField map = __.Coerce map

    override __.ToField a =
        if a.NULL then ctor [||]
        elif a.IsMSet then a.M |> Seq.map (fun kv -> keyVal kv.Key (vconv.ToField kv.Value)) |> ctor
        else invalidCast a

    interface ICollectionConverter with
        member __.ElementConverter = vconv :> _