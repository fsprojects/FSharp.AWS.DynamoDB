module internal FSharp.DynamoDB.ExprCommon

open Microsoft.FSharp.Quotations
open Microsoft.FSharp.Quotations.Patterns
open Microsoft.FSharp.Quotations.DerivedPatterns
open Microsoft.FSharp.Quotations.ExprShape

open FSharp.DynamoDB.FieldConverter

type AttributeGetInfo = 
    {
        Root   : RecordPropertyInfo
        Nested : RecordPropertyInfo list
        ValueConverter : FieldConverter
        Suffix : string option
    }
with
    member a.WithSuffix(suffix) = { a with Suffix = Some suffix }
    member a.GetId(rootId:string) = 
        seq { 
            yield rootId 
            for n in a.Nested -> n.Name 
            match a.Suffix with Some s -> yield s | None -> ()
        } |> String.concat "."

    member a.Iter f = f a.Root ; for n in a.Nested do f n

    static member Extract (record : Var) (info : RecordInfo) (e : Expr) =
        let rec extractProps props e =
            match e with
            | PropertyGet(Some (Var r'), p, []) when record = r' -> Some (Choice1Of2 p :: props)
            | PropertyGet(Some e, p, []) -> extractProps (Choice1Of2 p :: props) e
            | SpecificCall2 <@ fst @> (None, _, _, [e]) -> extractProps (Choice2Of2 1 :: props) e
            | SpecificCall2 <@ snd @> (None, _, _, [e]) -> extractProps (Choice2Of2 2 :: props) e
            | _ -> None

        let rec resolveRecordProps acc (ctx : RecordInfo option) curr =
            match curr, ctx with
            | [], _ -> 
                let conv = List.head(acc).Converter
                let rev = List.rev acc
                Some { Root = List.head rev ; Nested = List.tail rev ; ValueConverter = conv ; Suffix = None }

            | Choice1Of2 p :: tail, Some rI ->
                match rI.Properties |> Array.tryFind (fun rp -> rp.PropertyInfo = p) with
                | None -> None
                | Some rp -> resolveRecordProps (rp :: acc) rp.NestedRecord tail

            | Choice2Of2 i :: tail, Some rp -> 
                let rp = ctx.Value.Properties.[i-1]
                resolveRecordProps (rp :: acc) rp.NestedRecord tail

            | _ -> None

        extractProps [] e |> Option.bind (resolveRecordProps [] (Some info))