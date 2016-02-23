[<AutoOpen>]
module internal FSharp.DynamoDB.CollectionPicklers

open System
open System.Collections
open System.Collections.Generic
open System.IO
open System.Reflection

open Amazon.Util
open Amazon.DynamoDBv2.Model

open FSharp.DynamoDB

type ListPickler<'List, 'T when 'List :> seq<'T>>(ctor : seq<'T> -> 'List, nullV : 'List, tconv : Pickler<'T>) =
    inherit Pickler<'List>()
    override __.PickleType = PickleType.List
    override __.PicklerType = PicklerType.Value
    override __.DefaultValue = ctor [||]
    override __.Coerce obj =
        match obj with
        | null -> Some <| AttributeValue(NULL = true)
        | :? 'T as t -> 
            match tconv.Pickle t with
            | None -> None
            | Some av -> Some <| AttributeValue(L = rlist [|av|])
        | _ ->
            let rl = unbox<seq<'T>> obj |> Seq.choose tconv.Pickle |> rlist
            if rl.Count = 0 then None
            else
                Some <| AttributeValue(L = rl)

    override __.Pickle list = __.Coerce list

    override __.UnPickle a =
        if a.NULL then nullV
        elif a.IsLSet then a.L |> Seq.map tconv.UnPickle |> ctor
        else invalidCast a

    interface ICollectionPickler with
        member __.ElementConverter = tconv :> _

type BytesSetPickler() =
    inherit Pickler<Set<byte[]>>()
    override __.PickleType = PickleType.BytesSet
    override __.PicklerType = PicklerType.Value
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

    override __.Pickle bss = __.Coerce bss

    override __.UnPickle a =
        if a.NULL then Set.empty
        elif a.IsBSSet then a.BS |> Seq.map (fun ms -> ms.ToArray()) |> set
        else invalidCast a

    interface ICollectionPickler with
        member __.ElementConverter = new ByteArrayPickler() :> _

type NumSetPickler<'T when 'T : comparison> (tconv : NumRepresentablePickler<'T>) =
    inherit Pickler<Set<'T>>()
    override __.DefaultValue = Set.empty
    override __.PickleType = PickleType.NumberSet
    override __.PicklerType = PicklerType.Value
    override __.Coerce obj =
        match obj with
        | null -> Some <| AttributeValue(NULL = true)
        | :? 'T as t -> Some <| AttributeValue(NS = rlist[|tconv.UnParse t|])
        | _ ->
            let rl = obj |> unbox<seq<'T>> |> Seq.map tconv.UnParse |> rlist
            if rl.Count = 0 then None
            else Some <| AttributeValue(NS = rl)

    override __.Pickle set = __.Coerce set

    override __.UnPickle a =
        if a.NULL then Set.empty
        elif a.IsNSSet then a.NS |> Seq.map tconv.Parse |> set
        else invalidCast a

    interface ICollectionPickler with
        member __.ElementConverter = tconv :> _

type StringSetPickler<'T when 'T : comparison> (tconv : StringRepresentablePickler<'T>) =
    inherit Pickler<Set<'T>>()
    override __.DefaultValue = Set.empty
    override __.PickleType = PickleType.StringSet
    override __.PicklerType = PicklerType.Value
    override __.Coerce obj =
        match obj with
        | null -> AttributeValue(NULL = true) |> Some
        | :? 'T as t -> AttributeValue(SS = rlist[|tconv.UnParse t|]) |> Some
        | _ ->
            let rl = obj |> unbox<seq<'T>> |> Seq.map tconv.UnParse |> rlist
            if rl.Count = 0 then AttributeValue(NULL = true) |> Some
            else AttributeValue(SS = rl) |> Some

    override __.Pickle set = __.Coerce set

    override __.UnPickle a =
        if a.NULL then Set.empty
        elif a.IsSSSet then a.SS |> Seq.map tconv.Parse |> set
        else invalidCast a

    interface ICollectionPickler with
        member __.ElementConverter = tconv :> _

let mkSetPickler<'T when 'T : comparison>(tconv : Pickler<'T>) : Pickler<Set<'T>> =
    if typeof<'T> = typeof<byte[]> then BytesSetPickler() |> unbox else
    match tconv with
    | :? NumRepresentablePickler<'T> as tc -> NumSetPickler<'T>(tc) :> _
    | :? StringRepresentablePickler<'T> as tc -> StringSetPickler<'T>(tc) :> _
    | _ -> UnSupportedType.Raise typeof<Set<'T>>

type MapPickler<'Value>(vconv : Pickler<'Value>) =
    inherit Pickler<Map<string,'Value>>()
    override __.PickleType = PickleType.Map
    override __.PicklerType = PicklerType.Value
    override __.DefaultValue = Map.empty
    override __.Pickle map =
        if isNull map then AttributeValue(NULL = true) |> Some
        elif map.Count = 0 then None
        else
            let m = 
                map 
                |> Seq.choose (fun kv -> 
                    match vconv.Pickle kv.Value with 
                    | None -> None 
                    | Some av -> Some (keyVal kv.Key av)) 
                |> cdict

            AttributeValue(M = m) |> Some
            

    override __.UnPickle a =
        if a.NULL then Map.empty
        elif a.IsMSet then a.M |> Seq.map (fun kv -> kv.Key, vconv.UnPickle kv.Value) |> Map.ofSeq
        else invalidCast a

    interface ICollectionPickler with
        member __.ElementConverter = vconv :> _