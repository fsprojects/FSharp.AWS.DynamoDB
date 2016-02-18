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

type ListConverter<'List, 'T when 'List :> seq<'T>>(ctor : seq<'T> -> 'List, isEmpty : 'List -> bool, tconv : FieldConverter<'T>) =
    inherit FieldConverter<'List>()
    override __.Representation = FieldRepresentation.List
    override __.ConverterType = ConverterType.Value
    override __.DefaultValue = ctor [||]
    override __.OfField list = 
        if isNull list || isEmpty list then AttributeValue(NULL = true)
        else 
            AttributeValue(L = (list |> Seq.map tconv.OfField |> rlist))

    override __.ToField a =
        if a.NULL then ctor [||]
        elif a.IsLSet then a.L |> Seq.map tconv.ToField |> ctor
        else invalidCast a

type BytesSetConverter<'BSet when 'BSet :> seq<byte[]>>(ctor : seq<byte []> -> 'BSet, isEmpty : 'BSet -> bool) =
    inherit FieldConverter<'BSet>()
    override __.Representation = FieldRepresentation.BytesSet
    override __.ConverterType = ConverterType.Value
    override __.DefaultValue = ctor [||]
    override __.OfField bss = 
        if isNull bss || isEmpty bss then AttributeValue(NULL = true)
        else AttributeValue (BS = (bss |> Seq.map (fun bs -> new MemoryStream(bs)) |> rlist))

    override __.ToField a =
        if a.NULL then ctor [||]
        elif a.IsBSSet then a.BS |> Seq.map (fun ms -> ms.ToArray()) |> ctor
        else invalidCast a

type NumSetConverter<'Set, 'T when 'Set :> seq<'T>> (ctor : seq<'T> -> 'Set, isEmpty : 'Set -> bool, tconv : NumRepresentableFieldConverter<'T>) =
    inherit FieldConverter<'Set>()
    override __.DefaultValue = ctor [||]
    override __.Representation = FieldRepresentation.NumberSet
    override __.ConverterType = ConverterType.Value
    override __.OfField set = 
        if isNull set || isEmpty set then AttributeValue(NULL = true)
        else
            AttributeValue(NS = (set |> Seq.map tconv.UnParse |> rlist))

    override __.ToField a =
        if a.NULL then ctor [||]
        elif a.IsNSSet then a.NS |> Seq.map tconv.Parse |> ctor
        else invalidCast a

type StringSetConverter<'Set, 'T when 'Set :> seq<'T>> (ctor : seq<'T> -> 'Set, isEmpty : 'Set -> bool, tconv : StringRepresentableFieldConverter<'T>) =
    inherit FieldConverter<'Set>()
    override __.DefaultValue = ctor [||]
    override __.Representation = FieldRepresentation.StringSet
    override __.ConverterType = ConverterType.Value
    override __.OfField set = 
        if isNull set || isEmpty set then AttributeValue(NULL = true)
        else AttributeValue(SS = (set |> Seq.map tconv.UnParse |> rlist))

    override __.ToField a =
        if a.NULL then ctor [||]
        elif a.IsSSSet then a.SS |> Seq.map tconv.Parse |> ctor
        else invalidCast a

let mkSetConverter<'Set, 'T when 'Set :> seq<'T>> ctor isEmpty (tconv : FieldConverter<'T>) : FieldConverter<'Set> =
    match tconv with
    | :? NumRepresentableFieldConverter<'T> as tc -> NumSetConverter<'Set, 'T>(ctor, isEmpty, tc) :> _
    | :? StringRepresentableFieldConverter<'T> as tc -> StringSetConverter<'Set, 'T>(ctor, isEmpty, tc) :> _
    | _ -> UnSupportedField.Raise typeof<'Set>

type MapConverter<'Map, 'Value when 'Map :> seq<KeyValuePair<string, 'Value>>>
                    (ctor : seq<KeyValuePair<string, 'Value>> -> 'Map, isNullOrEmpty : 'Map -> bool,
                        vconv : FieldConverter<'Value>) =

    inherit FieldConverter<'Map>()
    override __.Representation = FieldRepresentation.Map
    override __.ConverterType = ConverterType.Value
    override __.DefaultValue = ctor [||]
    override __.OfField map =
        if isNullOrEmpty map then AttributeValue(NULL = true)
        else
            let m = map |> Seq.map (fun kv -> keyVal kv.Key (vconv.OfField kv.Value)) |> cdict
            AttributeValue(M = m)

    override __.ToField a =
        if a.NULL then ctor [||]
        elif a.IsMSet then a.M |> Seq.map (fun kv -> keyVal kv.Key (vconv.ToField kv.Value)) |> ctor
        else invalidCast a