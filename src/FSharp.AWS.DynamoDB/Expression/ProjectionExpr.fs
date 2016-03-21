module internal FSharp.AWS.DynamoDB.ProjectionExpr

open System
open System.Collections.Generic
open System.Reflection

open Microsoft.FSharp.Reflection
open Microsoft.FSharp.Quotations
open Microsoft.FSharp.Quotations.Patterns
open Microsoft.FSharp.Quotations.DerivedPatterns
open Microsoft.FSharp.Quotations.ExprShape

open Amazon.DynamoDBv2
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

type ProjectionExpr = 
    {
        Attributes : AttributeId []
        Ctor : Dictionary<string, AttributeValue> -> obj
    }

with

    static member Extract (recordInfo : RecordInfo) (expr : Expr<'TRecord -> 'Tuple>) =
        let invalidExpr () = invalidArg "expr" "supplied expression is not a valid projection."
        match expr with
        | Lambda(r, body) when r.Type = recordInfo.Type ->
            let (|AttributeGet|_|) expr = QuotedAttribute.TryExtract (fun _ -> None) r recordInfo expr

            match body with
            | AttributeGet qa ->
                let pickler = qa.Pickler
                let attrId = qa.RootProperty.Name
                let attr = qa.Id
                let ctor (ro : RestObject) = 
                    let ok, av = ro.TryGetValue attrId
                    if ok then pickler.UnPickleUntyped av
                    else pickler.DefaultValueUntyped

                { Attributes = [|attr|] ; Ctor = ctor }

            | NewTuple values -> 
                let qas = 
                    values 
                    |> Seq.map (function AttributeGet qa -> qa | _ -> invalidExpr ()) 
                    |> Seq.toArray

                // check for conflicting projection attributes
                qas 
                |> Seq.groupBy (fun qa -> qa.RootProperty.AttrId)
                |> Seq.filter (fun (_,rps) -> Seq.length rps > 1)
                |> Seq.iter (fun (attr,_) ->
                    sprintf "Projection expression accessing conflicting property '%s'." attr
                    |> invalidArg "expr")

                let attrs = qas |> Array.map (fun qa -> qa.Id)
                let picklers = qas |> Array.map (fun attr -> attr.Pickler)
                let attrIds = qas |> Array.map (fun attr -> attr.RootProperty.Name)
                let tupleCtor = FSharpValue.PreComputeTupleConstructor typeof<'Tuple>

                let ctor (ro : RestObject) =
                    let values = Array.zeroCreate<obj> picklers.Length
                    for i = 0 to picklers.Length - 1 do
                        let id = attrIds.[i]
                        let ok, av = ro.TryGetValue (attrIds.[i])
                        if ok then values.[i] <- picklers.[i].UnPickleUntyped av
                        else values.[i] <- picklers.[i].DefaultValueUntyped

                    tupleCtor values

                { Attributes = attrs ; Ctor = ctor }

            | _ -> invalidArg "expr" "projection type must either be a single property, or tuple of properties."
        | _ -> invalidExpr ()

    member __.Write (writer : AttributeWriter) =
        let sb = new System.Text.StringBuilder()
        let inline (!) (x:string) = sb.Append x |> ignore
        let mutable isFirst = true
        for attr in __.Attributes do
            if isFirst then isFirst <- false
            else ! ", "

            ! (writer.WriteAttibute attr)

        sb.ToString()

    member __.GetDebugData() =
        let aw = new AttributeWriter()
        let expr = __.Write (aw)
        let names = aw.Names |> Seq.map (fun kv -> kv.Key, kv.Value) |> Seq.toList
        expr, names