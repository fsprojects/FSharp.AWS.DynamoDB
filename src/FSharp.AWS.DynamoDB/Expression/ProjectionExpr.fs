module internal FSharp.AWS.DynamoDB.ProjectionExpr

open System
open System.Collections.Generic

open Microsoft.FSharp.Reflection
open Microsoft.FSharp.Quotations
open Microsoft.FSharp.Quotations.Patterns

open Amazon.DynamoDBv2.Model

open FSharp.AWS.DynamoDB.ExprCommon

///////////////////////////////
//
// Extracts projection expressions from an F# quotation of the form
// <@ fun record -> record.A, record.B, record.B.[0].C @>
//
//  c.f. http://docs.aws.amazon.com/amazondynamodb/latest/developerguide/Expressions.AccessingItemAttributes.html
//
///////////////////////////////

type AttributeId with

    member id.View(ro: RestObject, value: byref<AttributeValue>) : bool =
        let notFound rest =
            // only raise if the last component of the document path is missing
            if List.isEmpty rest then
                false
            else
                sprintf "Document path '%s' not found." id.Name |> KeyNotFoundException |> raise

        let rec aux result rest (av: AttributeValue) =
            match rest with
            | [] ->
                result := av
                true
            | FField f :: tl ->
                if av.IsMSet then
                    let ok, nested = av.M.TryGetValue f
                    if not ok then notFound tl else aux result tl nested
                else
                    av.Print()
                    |> sprintf "Expected map, but was '%s'."
                    |> InvalidCastException
                    |> raise

            | FIndex i :: tl ->
                if av.IsLSet then
                    if i < 0 || i >= av.L.Count then
                        sprintf "Indexed path '%s' out of range." id.Name
                        |> ArgumentOutOfRangeException
                        |> raise
                    else
                        aux result tl av.L.[i]
                else
                    av.Print()
                    |> sprintf "Expected list, but was '%s'."
                    |> InvalidCastException
                    |> raise

            | FParam _ :: _ -> sprintf "internal error; unexpected attribute path '%s'." id.Name |> invalidOp

        let ok, prop = ro.TryGetValue id.RootName

        if ok then
            let cell = ref null

            if aux cell id.NestedAttributes prop then
                value <- cell.Value
                true
            else
                false
        else
            notFound id.NestedAttributes

type ProjectionExpr =
    { Attributes: AttributeId[]
      Ctor: Dictionary<string, AttributeValue> -> obj }


    static member Extract (recordInfo: RecordTableInfo) (expr: Expr<'TRecord -> 'Tuple>) =
        let invalidExpr () =
            invalidArg "expr" "supplied expression is not a valid projection."

        match expr with
        | Lambda(r, body) when r.Type = recordInfo.Type ->
            let (|AttributeGet|_|) expr =
                QuotedAttribute.TryExtract (fun _ -> None) r recordInfo expr

            let (|Ignore|_|) e =
                match e with
                | Value(null, t) when t = typeof<unit> -> Some()
                | SpecificCall2 <@ ignore @> _ -> Some()
                | _ -> None

            match body with
            | Ignore ->
                let attr = AttributeId.FromKeySchema recordInfo.PrimaryKeySchema

                { Attributes = [| attr |]
                  Ctor = fun _ -> box () }

            | AttributeGet qa ->
                let pickler = qa.Pickler
                let attr = qa.Id

                let ctor (ro: RestObject) =
                    let mutable av = null
                    let ok = attr.View(ro, &av)

                    if ok then
                        pickler.UnPickleUntyped av
                    else
                        pickler.DefaultValueUntyped

                { Attributes = [| attr |]; Ctor = ctor }

            | NewTuple values ->
                let qAttrs =
                    values
                    |> Seq.map (function
                        | AttributeGet qa -> qa
                        | _ -> invalidExpr ())
                    |> Seq.toArray

                let attrs = qAttrs |> Array.map (fun qa -> qa.Id)
                let picklers = qAttrs |> Array.map (fun qa -> qa.Pickler)

                // check for conflicting projection attributes
                match tryFindConflictingPaths attrs with
                | Some(p1, p2) ->
                    let msg =
                        sprintf "found conflicting paths '%s' and '%s' being accessed in projection expression." p1 p2

                    invalidArg "expr" msg
                | None -> ()

                let tupleCtor = FSharpValue.PreComputeTupleConstructor typeof<'Tuple>

                let ctor (ro: RestObject) =
                    let values = Array.zeroCreate<obj> attrs.Length

                    for i = 0 to attrs.Length - 1 do
                        let mutable av = null
                        let ok = attrs.[i].View(ro, &av)

                        if ok then
                            values.[i] <- picklers.[i].UnPickleUntyped av
                        else
                            values.[i] <- picklers.[i].DefaultValueUntyped

                    tupleCtor values

                { Attributes = attrs; Ctor = ctor }

            | _ -> invalidArg "expr" "projection type must either be a single property, or tuple of properties."
        | _ -> invalidExpr ()

    member __.Write(writer: AttributeWriter) =
        let sb = new System.Text.StringBuilder()
        let inline (!) (x: string) = sb.Append x |> ignore
        let mutable isFirst = true

        for attr in __.Attributes do
            if isFirst then isFirst <- false else ! ", "

            !(writer.WriteAttibute attr)

        sb.ToString()

    member __.GetDebugData() =
        let aw = new AttributeWriter()
        let expr = __.Write(aw)
        let names = aw.Names |> Seq.map (fun kv -> kv.Key, kv.Value) |> Seq.toList
        expr, names
