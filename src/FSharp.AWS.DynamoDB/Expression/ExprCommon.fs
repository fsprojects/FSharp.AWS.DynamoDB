module internal FSharp.AWS.DynamoDB.ExprCommon

open System
open System.Collections.Generic
open System.Reflection

open Microsoft.FSharp.Quotations
open Microsoft.FSharp.Quotations.Patterns
open Microsoft.FSharp.Quotations.DerivedPatterns
open Microsoft.FSharp.Quotations.ExprShape

open Amazon.DynamoDBv2.Model

open Swensen.Unquote

//
//  Implementation of recognizers for expressions of shape 'r.A.B.[0].C'
//  where 'r' is an F# record.
//

/// Nested Attribute field id
type NestedAttribute =
    | FField of string // nested field
    | FIndex of int // nested list element
    | FParam of vIndex:int // Attribute parameter
with
    member nf.Print() =
        match nf with
        | FParam i -> sprintf "<$param%d>" i
        | FField f when not <| isValidFieldName f ->
            sprintf "map keys must be alphanumeric and not starting with a digit."
            |> invalidArg f

        | FField f -> "." + f
        | FIndex i -> sprintf "[%d]" i

/// DynamoDB Attribute identifier
type AttributeId = 
    { 
        RootName : string
        RootId : string
        NestedAttributes : NestedAttribute list
        Type : AttributeType 
    }
with
    member id.IsParametric =
        id.NestedAttributes |> List.exists (function FParam _ -> true | _ -> false)

    member id.Id = 
        mkString(fun append ->
            append id.RootId
            for nf in id.NestedAttributes do append <| nf.Print())

    member id.Name =
        mkString(fun append ->
            append id.RootId
            for nf in id.NestedAttributes do append <| nf.Print())

    member id.Tokens = 
        seq { yield id.RootName ; yield! id.NestedAttributes |> Seq.map (fun nf -> nf.Print()) }

    member id.IsHashKey = id.Type = AttributeType.HashKey
    member id.IsRangeKey = id.Type = AttributeType.RangeKey
    member id.Append nf = { id with NestedAttributes = id.NestedAttributes @ [nf] }
    member id.Apply (inputs : obj[]) =
        let applyField nf =
            match nf with
            | FParam i -> 
                match inputs.[i] with 
                | :? string as f -> FField f 
                | :? int as i -> 
                    if i < 0 then raise <| ArgumentOutOfRangeException()
                    else FIndex i 
                | _ -> raise <| new InvalidCastException()
            | _ -> nf

        { id with NestedAttributes = id.NestedAttributes |> List.map applyField }

    static member FromKeySchema(schema : TableKeySchema) =
        let rootId = "#HKEY"
        let hkName = schema.HashKey.AttributeName
        { RootId = rootId ; RootName = hkName ; NestedAttributes = [] ; Type = AttributeType.HashKey }

type RecordPropertyInfo with
    /// Gets an attribute Id for given record property that
    /// is recognizable by DynamoDB
    member rp.AttrId = sprintf "#ATTR%d" rp.Index

/// Represents a nested field of an F# record type
type QuotedAttribute =
    | Root of RecordPropertyInfo
    | Nested of RecordPropertyInfo * parent:QuotedAttribute
    | Item of NestedAttribute * pickler:Pickler * parent:QuotedAttribute
    | Optional of pickler:Pickler * parent:QuotedAttribute
with
    /// Gets the pickler corresponding to the type pointed to by the attribute path
    member ap.Pickler =
        match ap with
        | Root rp -> rp.Pickler
        | Nested (rp,_) -> rp.Pickler
        | Item(_,pickler,_) -> pickler
        | Optional(p,_) -> p

    /// Gets the root record property of given attribute path
    member ap.RootProperty =
        let rec aux ap =
            match ap with
            | Root rp -> rp
            | Nested(_,p) -> aux p
            | Item(_,_,p) -> aux p
            | Optional(_,p)-> aux p

        aux ap

    /// Gets an attribute identifier for given Quoted attribute instace
    member ap.Id =
        let rec getTokens acc ap =
            match ap with
            | Nested (rp,p) -> getTokens (FField rp.Name :: acc) p
            | Item(nf,_,p) -> getTokens (nf :: acc) p
            | Optional(_,p) -> getTokens acc p
            | Root rp ->
                {
                    RootId = rp.AttrId
                    RootName = rp.Name
                    NestedAttributes = acc
                    Type = rp.AttributeType
                }

        getTokens [] ap

    /// Iterates through all resolved picklers of a given attribute path
    member ap.Iter(f : Pickler -> unit) =
        let rec aux ap =
            match ap with
            | Root rp -> f rp.Pickler
            | Nested (rp,p) -> f rp.Pickler ; aux p
            | Item(_,pickler,p) -> f pickler ; aux p
            | Optional(pickler,p) -> f pickler; aux p

        aux ap

    /// Attempt to extract an attribute path for given record info and expression
    static member TryExtract ((|PVar|_|) : Expr -> int option) (record : Var) (info : RecordInfo) (e : Expr) =
        let tryGetPropInfo (info : RecordInfo) isFinalProp (p : PropertyInfo) =
            match info.Properties |> Array.tryFind (fun rp -> rp.PropertyInfo = p) with
            | None -> None
            | Some rp when rp.Pickler.PicklerType = PicklerType.Serialized && not isFinalProp ->
                invalidArg "expr" "cannot access nested properties of serialized fields."
            | Some rp when rp.Pickler.PicklerType = PicklerType.Union && not isFinalProp ->
                invalidArg "expr" "cannot access nested properties of union fields."
            | Some _ as r -> r

        let rec extractProps props e =
            match e with
            | PropertyGet(Some (Var r'), p, []) when record = r' -> 
                match tryGetPropInfo info (List.isEmpty props) p with
                | None -> None
                | Some rp -> mkAttrPath (Root rp) rp.NestedRecord props

            | SpecificProperty <@ fun (t : _ option) -> t.Value @> (Some e,[et],_) ->
                extractProps (Choice2Of3 et :: props) e

            | SpecificProperty <@ fun (r : _ ref) -> r.Value @> (Some e,_,_) ->
                let p = e.Type.GetProperty("contents")
                extractProps (Choice1Of3 p :: props) e

            | PropertyGet(Some e, p, []) -> extractProps (Choice1Of3 p :: props) e

            | SpecificCall2 <@ fst @> (None, _, _, [e]) -> 
                let p = e.Type.GetProperty("Item1") 
                extractProps (Choice1Of3 p :: props) e

            | SpecificCall2 <@ snd @> (None, _, _, [e]) -> 
                let p = e.Type.GetProperty("Item2")
                extractProps (Choice1Of3 p :: props) e

            | SpecificCall2 <@ Option.get @> (None, _, [et], [e]) ->
                extractProps (Choice2Of3 et :: props) e

            | IndexGet(e, et, i) -> 
                extractProps (Choice3Of3 (et, i) :: props) e

            | _ -> None

        and mkAttrPath acc (ctx : RecordInfo option) rest =
            match rest, ctx with
            | [], _ -> Some acc
            | Choice1Of3 p :: tail, Some rI ->
                match tryGetPropInfo rI (List.isEmpty tail) p with
                | None -> None
                | Some rp -> mkAttrPath (Nested(rp, acc)) rp.NestedRecord tail

            | Choice2Of3 opt :: tail, None ->
                let pickler = Pickler.resolveUntyped opt
                mkAttrPath (Optional(pickler, acc)) ctx tail

            | Choice3Of3 (et, ie) :: tail, None ->
                let pickler = Pickler.resolveUntyped et
                let ctx = match box pickler with :? IRecordPickler as rc -> Some rc.RecordInfo | _ -> None
                let inline mkAttrPath indexV = mkAttrPath (Item(indexV, pickler, acc)) ctx tail
                match ie with
                | _ when ie.IsClosed ->
                    match evalRaw ie : obj with
                    | :? int as i -> 
                        if i < 0 then raise <| ArgumentOutOfRangeException() else
                        mkAttrPath (FIndex i)
                    | :? string as f -> mkAttrPath (FField f)
                    | _ -> None
                | PVar i -> mkAttrPath (FParam i)
                | _ -> None

            | _ -> None

        extractProps [] e

/// Wrapper API for writing attribute names and values for Dynamo query expressions
type AttributeWriter(names : Dictionary<string, string>, values : Dictionary<string, AttributeValue>) =
    static let cmp = new AttributeValueComparer()
    let vcontents = new Dictionary<AttributeValue, string>(cmp)

    new () = new AttributeWriter(new Dictionary<_,_>(), new Dictionary<_,_>())

    member __.Names  = names
    member __.Values = values

    member __.WriteValue(av : AttributeValue) =
        let ok, found = vcontents.TryGetValue av
        if ok then found
        else
            let id = sprintf ":val%d" values.Count
            vcontents.Add(av, id)
            values.Add(id, av)
            id

    member __.WriteAttibute(attr : AttributeId) =
        names.[attr.RootId] <- attr.RootName
        attr.Id

/// Recognizes exprs of shape <@ fun p1 p2 ... -> body @>
let extractExprParams (recordInfo : RecordInfo) (expr : Expr) =
    let vars = new Dictionary<Var, int> ()
    let rec aux i expr =
        match expr with
        | Lambda(v, body) when v.Type <> recordInfo.Type ->
            vars.Add(v, i)
            aux (i + 1) body
        | _ -> expr

    let expr' = aux 0 expr
    let tryFindIndex e =
        match e with
        | Var v ->
            let ok,i = vars.TryGetValue v
            if ok then Some i
            else None
        | _ -> None

    vars.Count, tryFindIndex, expr'

// Detects conflicts in a collection of attribute paths
// e.g. 'r.Foo.Bar.[0]' and 'r.Foo' are conflicting
// however 'r.Foo.Bar.[0]' and 'r.Foo.Bar.[1]' are not conflicting
type private AttributeNode = { Value : string ; Children : ResizeArray<AttributeNode> }
/// Detects conflicts in a collection of attribute paths
let tryFindConflictingPaths (attrs : seq<AttributeId>) =
    let root = new ResizeArray<AttributeNode>()
    let tryAppendPath (attr : AttributeId) =
        let tokens = attr.Tokens
        let enum = tokens.GetEnumerator()
        let mutable ctx = root
        let mutable isNodeAdded = false
        let mutable isLeafFound = false
        let acc = new ResizeArray<_>()
        while not isLeafFound && enum.MoveNext() do
            let t = enum.Current
            let child =
                match ctx.FindIndex(fun n -> n.Value = t) with
                | -1 -> 
                    isNodeAdded <- true
                    let ch = { Value = t ; Children = new ResizeArray<_>() }
                    ctx.Add ch
                    ch

                | i ->
                    let ch = ctx.[i]
                    if ch.Children.Count = 0 then isLeafFound <- true
                    ch

            acc.Add t
            ctx <- child.Children

        let concat xs = String.concat "" xs
        if isLeafFound then Some(concat tokens, concat acc)
        elif not isNodeAdded then
            while ctx.Count > 0 do
                let ch = ctx.[0]
                acc.Add ch.Value
                ctx <- ch.Children

            Some(concat tokens, concat acc)

        else None

    attrs |> Seq.tryPick tryAppendPath