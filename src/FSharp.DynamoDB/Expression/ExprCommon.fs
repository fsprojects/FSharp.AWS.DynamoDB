module internal FSharp.DynamoDB.ExprCommon

open System
open System.Reflection

open Microsoft.FSharp.Quotations
open Microsoft.FSharp.Quotations.Patterns
open Microsoft.FSharp.Quotations.DerivedPatterns
open Microsoft.FSharp.Quotations.ExprShape

open Swensen.Unquote

open FSharp.DynamoDB.FieldConverter

type AttributePath =
    | Root of RecordPropertyInfo
    | Nested of RecordPropertyInfo * parent:AttributePath
    | Item of index:int * conv:FieldConverter * parent:AttributePath
    | Suffix of id:string * parent:AttributePath
with
    member ap.Converter =
        match ap with
        | Root rp -> rp.Converter
        | Nested (rp,_) -> rp.Converter
        | Item(_,conv,_) -> conv
        | _ -> invalidArg "ap" "internal error: no converter found here"

    member ap.RootProperty =
        let rec aux ap =
            match ap with
            | Root rp -> rp
            | Nested(_,p) -> aux p
            | Item(_,_,p) -> aux p
            | Suffix(_,p) -> aux p

        aux ap

    member ap.RootId = sprintf "#ATTR%d" ap.RootProperty.Index
    member ap.RootName = ap.RootProperty.Name

    member ap.Id =
        let rec getTokens acc ap =
            match ap with
            | Root rp -> sprintf "#ATTR%d" rp.Index :: acc
            | Nested (rp,p) -> getTokens ("." + rp.Name :: acc) p
            | Item(i,_,p) -> getTokens (sprintf "[%d]" i :: acc) p
            | Suffix (id,p) -> getTokens ("." + id :: acc) p

        getTokens [] ap |> String.concat ""

    member ap.Iter(f : FieldConverter -> unit) =
        let rec aux ap =
            match ap with
            | Root rp -> f rp.Converter
            | Nested (rp,p) -> f rp.Converter ; aux p
            | Item(_,conv,p) -> f conv ; aux p
            | Suffix(_,p) -> aux p

        aux ap

    static member Extract (record : Var) (info : RecordInfo) (e : Expr) =
        let tryGetPropInfo (info : RecordInfo) (p : PropertyInfo) =
            info.Properties |> Array.tryFind (fun rp -> rp.PropertyInfo = p)

        let rec extractProps props e =
            match e with
            | PropertyGet(Some (Var r'), p, []) when record = r' -> 
                match tryGetPropInfo info p with
                | None -> None
                | Some rp -> mkAttrPath (Root rp) rp.NestedRecord props

            | PropertyGet(Some e, p, []) -> extractProps (Choice1Of3 p :: props) e
            | SpecificCall2 <@ fst @> (None, _, _, [e]) -> extractProps (Choice2Of3 0 :: props) e
            | SpecificCall2 <@ snd @> (None, _, _, [e]) -> extractProps (Choice2Of3 1 :: props) e
            | IndexGet(e, et, i) when i.IsClosed -> extractProps (Choice3Of3 (et, i) :: props) e
            | _ -> None

        and mkAttrPath acc (ctx : RecordInfo option) rest =
            match rest, ctx with
            | [], _ -> Some acc
            | Choice1Of3 p :: tail, Some rI ->
                match rI.Properties |> Array.tryFind (fun rp -> rp.PropertyInfo = p) with
                | None -> None
                | Some rp -> mkAttrPath (Nested(rp, acc)) rp.NestedRecord tail

            | Choice2Of3 i :: tail, Some rI ->
                let rp = rI.Properties.[i]
                mkAttrPath (Nested(rp, acc)) rp.NestedRecord tail

            | Choice3Of3 (et, ie) :: tail, None ->
                let conv = FieldConverter.resolveUntyped et
                let i = evalRaw ie
                let ctx = match box conv with :? IRecordConverter as rc -> Some rc.RecordInfo | _ -> None
                mkAttrPath (Item(i, conv, acc)) ctx tail

            | _ -> None

        extractProps [] e