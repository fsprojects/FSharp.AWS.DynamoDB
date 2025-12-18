[<AutoOpen>]
module internal FSharp.AWS.DynamoDB.CollectionPicklers

open System.IO

open Amazon.DynamoDBv2.Model

open FSharp.AWS.DynamoDB

//
//  Pickler implementations for collection types
//

type ListPickler<'List, 'T when 'List :> seq<'T>>(ctor: seq<'T> -> 'List, nullV: 'List, tp: Pickler<'T>) =
    inherit Pickler<'List>()
    override _.PickleType = PickleType.List
    override _.PicklerType = PicklerType.Value
    override _.DefaultValue = ctor [||]
    override _.PickleCoerced obj =
        match obj with
        | null -> Some <| AttributeValue(NULL = true)
        | :? 'T as t ->
            match tp.Pickle t with
            | None -> None
            | Some av -> Some <| AttributeValue(L = rlist [| av |])
        | _ ->
            let rl = unbox<seq<'T>> obj |> Seq.choose tp.Pickle |> rlist
            if rl.Count = 0 then
                None
            else
                Some <| AttributeValue(L = rl)

    override __.Pickle list = __.PickleCoerced list

    override _.UnPickle a =
        if a.IsNULL then nullV
        elif a.IsLSet then a.L |> Seq.map tp.UnPickle |> ctor
        else invalidCast a

    interface ICollectionPickler with
        member _.ElementPickler = tp :> _



type BytesSetPickler() =
    inherit Pickler<Set<byte[]>>()
    override _.PickleType = PickleType.BytesSet
    override _.PicklerType = PicklerType.Value
    override _.DefaultValue = Set.empty
    override _.PickleCoerced obj =
        match obj with
        | null -> Some <| AttributeValue(NULL = true)
        | :? (byte[]) as bs ->
            if bs.Length = 0 then
                None
            else
                Some <| AttributeValue(BS = rlist [| new MemoryStream(bs) |])

        | _ ->
            let rl =
                unbox<seq<byte[]>> obj
                |> Seq.choose (fun bs -> if bs.Length = 0 then None else Some(new MemoryStream(bs)))
                |> rlist

            if rl.Count = 0 then
                None
            else
                Some <| AttributeValue(BS = rl)

    override __.Pickle bss = __.PickleCoerced bss

    override _.UnPickle a =
        if a.IsNULL then
            Set.empty
        elif a.IsBSSet then
            a.BS |> Seq.map _.ToArray() |> set
        else
            invalidCast a

    interface ICollectionPickler with
        member _.ElementPickler = ByteArrayPickler() :> _



type NumSetPickler<'T when 'T: comparison>(tp: NumRepresentablePickler<'T>) =
    inherit Pickler<Set<'T>>()
    override _.DefaultValue = Set.empty
    override _.PickleType = PickleType.NumberSet
    override _.PicklerType = PicklerType.Value
    override _.PickleCoerced obj =
        match obj with
        | null -> Some <| AttributeValue(NULL = true)
        | :? 'T as t -> Some <| AttributeValue(NS = rlist [| tp.UnParse t |])
        | _ ->
            let rl = obj |> unbox<seq<'T>> |> Seq.map tp.UnParse |> rlist
            if rl.Count = 0 then
                None
            else
                Some <| AttributeValue(NS = rl)

    override __.Pickle set = __.PickleCoerced set

    override _.UnPickle a =
        if a.IsNULL then Set.empty
        elif a.IsNSSet then a.NS |> Seq.map tp.Parse |> set
        else invalidCast a

    interface ICollectionPickler with
        member _.ElementPickler = tp :> _



type StringSetPickler<'T when 'T: comparison>(tp: StringRepresentablePickler<'T>) =
    inherit Pickler<Set<'T>>()
    override _.DefaultValue = Set.empty
    override _.PickleType = PickleType.StringSet
    override _.PicklerType = PicklerType.Value
    override _.PickleCoerced obj =
        match obj with
        | null -> AttributeValue(NULL = true) |> Some
        | :? 'T as t -> AttributeValue(SS = rlist [| tp.UnParse t |]) |> Some
        | _ ->
            let rl = obj |> unbox<seq<'T>> |> Seq.map tp.UnParse |> rlist
            if rl.Count = 0 then
                None
            else
                AttributeValue(SS = rl) |> Some

    override __.Pickle set = __.PickleCoerced set

    override _.UnPickle a =
        if a.IsNULL then Set.empty
        elif a.IsSSSet then a.SS |> Seq.map tp.Parse |> set
        else invalidCast a

    interface ICollectionPickler with
        member _.ElementPickler = tp :> _

let mkSetPickler<'T when 'T: comparison> (tp: Pickler<'T>) : Pickler<Set<'T>> =
    if typeof<'T> = typeof<byte[]> then
        BytesSetPickler() |> unbox
    else
        match tp with
        | :? NumRepresentablePickler<'T> as tc -> NumSetPickler<'T>(tc) :> _
        | :? StringRepresentablePickler<'T> as tc -> StringSetPickler<'T>(tc) :> _
        | _ -> UnSupportedType.Raise typeof<Set<'T>>



type MapPickler<'Value>(vp: Pickler<'Value>) =
    inherit Pickler<Map<string, 'Value>>()
    override _.PickleType = PickleType.Map
    override _.PicklerType = PicklerType.Value
    override _.DefaultValue = Map.empty
    override _.Pickle map =
        if isNull map then
            AttributeValue(NULL = true) |> Some
        elif map.Count = 0 then
            None
        else
            let m =
                map
                |> Seq.choose (fun kv ->
                    if not <| isValidFieldName kv.Key then
                        let msg = sprintf "unsupported key name '%s'. should be 1 to 64k long (as utf8)." kv.Key
                        invalidArg "map" msg

                    match vp.Pickle kv.Value with
                    | None -> None
                    | Some av -> Some(keyVal kv.Key av))
                |> cdict

            if m.Count = 0 then
                None
            else

                AttributeValue(M = m) |> Some


    override _.UnPickle a =
        if a.IsNULL then
            Map.empty
        elif a.IsMSet then
            a.M |> Seq.map (fun kv -> kv.Key, vp.UnPickle kv.Value) |> Map.ofSeq
        else
            invalidCast a

    interface ICollectionPickler with
        member _.ElementPickler = vp :> _
