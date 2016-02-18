[<AutoOpen>]
module internal FSharp.DynamoDB.DynamoUtils

open System.IO
open System.Collections.Generic

open Amazon.DynamoDBv2.Model

type FsAttributeValue =
    | Null
    | Bool of bool
    | String of string
    | Number of string
    | Bytes of byte[]
    | StringSet of string[]
    | NumberSet of string[]
    | BytesSet of byte[][]
    | List of FsAttributeValue[]
    | Map of KeyValuePair<string, FsAttributeValue>[]
with
    static member FromAttributeValue(av : AttributeValue) =
        if av.NULL then Null
        elif av.IsBOOLSet then Bool av.BOOL
        elif av.S <> null then String av.S
        elif av.N <> null then Number av.N
        elif av.B <> null then Bytes (av.B.ToArray())
        elif av.SS.Count > 0 then StringSet (Seq.toArray av.SS)
        elif av.NS.Count > 0 then NumberSet (Seq.toArray av.NS)
        elif av.BS.Count > 0 then av.BS |> Seq.map (fun bs -> bs.ToArray()) |> Seq.toArray |> BytesSet
        elif av.IsLSet then av.L |> Seq.map FsAttributeValue.FromAttributeValue |> Seq.toArray |> List
        elif av.IsMSet then 
            av.M 
            |> Seq.map (fun kv -> KeyValuePair(kv.Key, FsAttributeValue.FromAttributeValue kv.Value)) 
            |> Seq.toArray
            |> Map

        else invalidArg "av" "undefined attribute value"

    static member ToAttributeValue(fsav : FsAttributeValue) =
        match fsav with
        | Null -> AttributeValue(NULL = true)
        | Bool b -> AttributeValue(BOOL = b)
        | String null -> AttributeValue(NULL = true)
        | String s -> AttributeValue(s)
        | Number null -> invalidArg "fsav" "Number attribute contains null as value."
        | Number n -> AttributeValue(N = n)
        | Bytes null -> AttributeValue(NULL = true)
        | Bytes bs -> AttributeValue(B = new MemoryStream(bs))
        | StringSet (null | [||]) -> AttributeValue(NULL = true)
        | StringSet ss -> AttributeValue(rlist ss)
        | NumberSet (null | [||]) -> AttributeValue(NULL = true)
        | NumberSet ns -> AttributeValue(NS = rlist ns)
        | BytesSet (null | [||]) -> AttributeValue(NULL = true)
        | BytesSet bss -> AttributeValue(BS = (bss |> Seq.map (fun bs -> new MemoryStream(bs)) |> rlist))
        | List (null | [||]) -> AttributeValue(NULL = true)
        | List attrs -> AttributeValue(L = (attrs |> Seq.map FsAttributeValue.ToAttributeValue |> rlist))
        | Map (null | [||]) -> AttributeValue(NULL = true)
        | Map attrs -> 
            AttributeValue(M =
                (attrs
                |> Seq.map (fun kv -> KeyValuePair(kv.Key, FsAttributeValue.ToAttributeValue kv.Value)) 
                |> cdict))


type AttributeValue with
    member inline av.IsSSSet = av.SS.Count > 0
    member inline av.IsNSSet = av.NS.Count > 0
    member inline av.IsBSSet = av.BS.Count > 0

    member av.Print() =
        if av.NULL then "{ NULL = true }"
        elif av.IsBOOLSet then sprintf "{ BOOL = %b }" av.BOOL
        elif av.S <> null then sprintf "{ S = %s }" av.S
        elif av.N <> null then sprintf "{ N = %s }" av.N
        elif av.B <> null then sprintf "{ N = %A }" (av.B.ToArray())
        elif av.SS.Count > 0 then sprintf "{ SS = %A }" (Seq.toArray av.SS)
        elif av.NS.Count > 0 then sprintf "{ SN = %A }" (Seq.toArray av.NS)
        elif av.BS.Count > 0 then 
            av.BS 
            |> Seq.map (fun bs -> bs.ToArray()) 
            |> Seq.toArray
            |> sprintf "{ BS = %A }"

        elif av.IsLSet then 
            av.L |> Seq.map (fun av -> av.Print()) |> Seq.toArray |> sprintf "{ L = %A }"

        elif av.IsMSet then 
            av.M 
            |> Seq.map (fun kv -> (kv.Key, kv.Value.Print())) 
            |> Seq.toArray
            |> sprintf "{ M = %A }"

        else
            "{ }"