[<AutoOpen>]
module internal FSharp.DynamoDB.UnionConverter

open System
open System.Text.RegularExpressions
open System.Collections
open System.Collections.Generic
open System.IO
open System.Reflection

open Microsoft.FSharp.Reflection

open Amazon.Util
open Amazon.DynamoDBv2.Model

open FSharp.DynamoDB

type private UnionCaseData = 
    { 
        UCI : UnionCaseInfo
        CaseCtor : MethodInfo
        Properties : RecordPropertyInfo []
    }

type UnionConverter<'U>(resolver : IFieldConverterResolver) =
    inherit FieldConverter<'U>()
    
    let caseAttr = "Union_Case"
    let ucis = FSharpType.GetUnionCases(typeof<'U>, true)
    let tagReader = FSharpValue.PreComputeUnionTagReader(typeof<'U>, true)
    let mkUCD uci =
        let ctor = FSharpValue.PreComputeUnionConstructorInfo(uci, true)
        let props = uci.GetFields() |> Array.mapi (RecordPropertyInfo.FromPropertyInfo resolver)
        { UCI = uci ; CaseCtor = ctor ; Properties = props }
    
    let cases = ucis |> Array.map mkUCD

    member __.OfUnion (union : 'U) : RestObject =
        let values = new RestObject()
        let tag = tagReader union
        let case = cases.[tag]
        values.Add(caseAttr, AttributeValue(case.UCI.Name))
        for prop in case.Properties do
            let field = prop.PropertyInfo.GetValue union
            match prop.Converter.OfFieldUntyped field with
            | None -> ()
            | Some av -> values.Add(prop.Name, av)            

        values

    member __.ToUnion (ro : RestObject) : 'U =
        let notFound name = raise <| new KeyNotFoundException(sprintf "attribute %A not found." name)
        let tag =
            let ok, av = ro.TryGetValue caseAttr
            if ok then
                match av.S with 
                | null -> invalidCast av
                | tag -> tag
            else
                notFound caseAttr

        match cases |> Array.tryFind (fun c -> c.UCI.Name = tag) with
        | None -> 
            let msg = sprintf "union case name %A does not correspond to type '%O'." tag typeof<'U>
            raise <| new InvalidDataException(msg)

        | Some case ->
            let values = Array.zeroCreate<obj> case.Properties.Length
            for i = 0 to values.Length - 1 do
                let prop = case.Properties.[i]
                let ok, av = ro.TryGetValue prop.Name
                if ok then values.[i] <- prop.Converter.ToFieldUntyped av
                elif prop.NoDefaultValue then notFound prop.Name
                else values.[i] <- prop.Converter.DefaultValueUntyped

            case.CaseCtor.Invoke(null, values) :?> 'U

    override __.ConverterType = ConverterType.Union
    override __.Representation = FieldRepresentation.Map
    override __.DefaultValue = invalidOp <| sprintf "default values not supported for records."

    override __.OfField (union : 'U) =
        let ro = __.OfUnion union 
        Some <| AttributeValue(M = ro)

    override __.ToField a =
        if a.IsMSet then __.ToUnion a.M
        else invalidCast a