[<AutoOpen>]
module internal FSharp.AWS.DynamoDB.DynamoUtils

open System.IO
open System.Collections.Generic
open System.Text.RegularExpressions

open Amazon.DynamoDBv2.Model

type AttributeValueComparer() =
    static let areEqualMemoryStreams (m: MemoryStream) (m': MemoryStream) =
        if m.Length <> m'.Length then
            false
        else
            m.ToArray() = m'.ToArray()

    static let areEqualResizeArrays (ra: ResizeArray<'T>) (ra': ResizeArray<'T>) =
        if ra.Count <> ra'.Count then
            false
        else
            let mutable areEqual = true
            let mutable i = 0
            while areEqual && i < ra.Count do
                areEqual <- ra.[i] = ra'.[i]
                i <- i + 1

            areEqual

    static let rec areEqualAttributeValues (av: AttributeValue) (av': AttributeValue) =
        if av.NULL.HasValue then
            av'.NULL = av.NULL
        elif av.BOOL.HasValue then
            av.BOOL = av'.BOOL
        elif notNull av.S then
            notNull av'.S && av.S = av'.S
        elif notNull av.N then
            notNull av'.N && av.N = av'.N
        elif notNull av.B then
            notNull av'.B && areEqualMemoryStreams av.B av'.B
        elif av.SS.Count > 0 then
            av'.SS.Count > 0 && areEqualResizeArrays av.SS av'.SS
        elif av.NS.Count > 0 then
            av'.NS.Count > 0 && areEqualResizeArrays av.NS av'.NS
        elif av.BS.Count > 0 then
            av'.BS.Count > 0 && av.BS.Count = av'.BS.Count && Seq.forall2 areEqualMemoryStreams av.BS av'.BS
        elif av.IsLSet then
            av'.IsLSet && av.L.Count = av'.L.Count && Seq.forall2 areEqualAttributeValues av.L av'.L
        elif av.IsMSet then
            av'.IsMSet
            && av.M.Count = av'.M.Count
            && av.M
               |> Seq.forall (fun kv ->
                   let ok, found = av'.M.TryGetValue kv.Key
                   if ok then areEqualAttributeValues kv.Value found else false)
        else
            true

    static let getSeqHash (eh: 'T -> int) (ts: seq<'T>) =
        let mutable h = 13
        for t in ts do
            h <- combineHash h (eh t)
        h

    static let rec getAttributeValueHashCode (av: AttributeValue) =
        if av.NULL.HasValue then
            0
        elif av.IsBOOLSet then
            hash av.BOOL
        elif notNull av.S then
            hash av.S
        elif notNull av.N then
            hash av.N
        elif notNull av.B then
            hash av.B.Length
        elif av.SS.Count > 0 then
            getSeqHash hash av.SS
        elif av.NS.Count > 0 then
            getSeqHash hash av.NS
        elif av.BS.Count > 0 then
            av.BS |> getSeqHash (fun m -> hash m.Length)
        elif av.IsLSet then
            getSeqHash getAttributeValueHashCode av.L
        elif av.IsMSet then
            av.M |> getSeqHash (fun kv -> hash2 kv.Key (getAttributeValueHashCode kv.Value))
        else
            -1

    static member Equals(l, r) = areEqualAttributeValues l r
    static member GetHashCode av = getAttributeValueHashCode av

    interface IEqualityComparer<AttributeValue> with
        member __.Equals(l, r) = areEqualAttributeValues l r
        member __.GetHashCode av = getAttributeValueHashCode av

/// Struct AttributeValue wrapper with modified equality semantics
[<Struct; CustomEquality; NoComparison>]
type AttributeValueEqWrapper(av: AttributeValue) =
    member __.AttributeValue = av
    override __.Equals(o) =
        match o with
        | :? AttributeValueEqWrapper as av' -> AttributeValueComparer.Equals(av, av')
        | _ -> false

    override __.GetHashCode() = AttributeValueComparer.GetHashCode av

let inline wrap av = new AttributeValueEqWrapper(av)
let inline unwrap (avw: AttributeValueEqWrapper) = avw.AttributeValue

type AttributeValue with

    member inline av.IsSSSet = av.SS.Count > 0
    member inline av.IsNSSet = av.NS.Count > 0
    member inline av.IsBSSet = av.BS.Count > 0

    member av.Print() =
        if av.NULL.GetValueOrDefault false then
            "{ NULL = true }"
        elif av.IsBOOLSet then
            sprintf "{ BOOL = %b }" (av.BOOL.GetValueOrDefault false)
        elif av.S <> null then
            sprintf "{ S = %s }" av.S
        elif av.N <> null then
            sprintf "{ N = %s }" av.N
        elif av.B <> null then
            sprintf "{ N = %A }" (av.B.ToArray())
        elif av.SS.Count > 0 then
            sprintf "{ SS = %A }" (Seq.toArray av.SS)
        elif av.NS.Count > 0 then
            sprintf "{ SN = %A }" (Seq.toArray av.NS)
        elif av.BS.Count > 0 then
            av.BS |> Seq.map (fun bs -> bs.ToArray()) |> Seq.toArray |> sprintf "{ BS = %A }"
        elif av.IsLSet then
            av.L |> Seq.map (fun av -> av.Print()) |> Seq.toArray |> sprintf "{ L = %A }"
        elif av.IsMSet then
            av.M |> Seq.map (fun kv -> (kv.Key, kv.Value.Print())) |> Seq.toArray |> sprintf "{ M = %A }"
        else
            "{ }"

// DynamoDB Name limitations, see:
// http://docs.aws.amazon.com/amazondynamodb/latest/developerguide/Limits.html
let private tableNameRegex = Regex("^[\w\-_\.]*$", RegexOptions.Compiled)
let isValidTableName (tableName: string) =
    if tableName.Length < 3 || tableName.Length > 255 then false
    elif not <| tableNameRegex.IsMatch tableName then false
    else true

let private utf8Length (str: string) = System.Text.Encoding.UTF8.GetBytes(str).Length

let isValidFieldName (name: string) = name <> null && name.Length > 0 && utf8Length name <= 65535

let isValidKeyName (name: string) = name <> null && name.Length > 0 && utf8Length name <= 255
