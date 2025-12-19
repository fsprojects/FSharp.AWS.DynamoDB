[<AutoOpen>]
module internal FSharp.AWS.DynamoDB.UnionPickler

open System.Collections.Generic
open System.IO
open System.Reflection

open Microsoft.FSharp.Reflection

open Amazon.DynamoDBv2.Model

open FSharp.AWS.DynamoDB

//
//  Pickler implementation for F# Union types
//

type private UnionCaseData = { UCI: UnionCaseInfo; CaseCtor: MethodInfo; Properties: PropertyMetadata[] }

type UnionPickler<'U>(resolver: IPicklerResolver) =
    inherit Pickler<'U>()

    let caseAttr = "Union_Case"
    let ucis = FSharpType.GetUnionCases(typeof<'U>, true)
    let tagReader = FSharpValue.PreComputeUnionTagReader(typeof<'U>, true)
    let mkUCD uci =
        let ctor = FSharpValue.PreComputeUnionConstructorInfo(uci, true)
        let props = uci.GetFields() |> Array.mapi (PropertyMetadata.FromPropertyInfo resolver)
        { UCI = uci; CaseCtor = ctor; Properties = props }

    let cases = ucis |> Array.map mkUCD

    member _.OfUnion(union: 'U) : RestObject =
        let values = RestObject()
        let tag = tagReader union
        let case = cases[tag]
        values.Add(caseAttr, AttributeValue(case.UCI.Name))
        for prop in case.Properties do
            let field = prop.PropertyInfo.GetValue union
            match prop.Pickler.PickleUntyped field with
            | None -> ()
            | Some av -> values.Add(prop.Name, av)

        values

    member _.ToUnion(ro: RestObject) : 'U =
        let notFound name = raise <| KeyNotFoundException(sprintf "attribute %A not found." name)
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
            raise <| InvalidDataException(msg)

        | Some case ->
            let values = Array.zeroCreate<obj> case.Properties.Length
            for i = 0 to values.Length - 1 do
                let prop = case.Properties[i]
                let ok, av = ro.TryGetValue prop.Name
                if ok then values[i] <- prop.Pickler.UnPickleUntyped av
                elif prop.NoDefaultValue then notFound prop.Name
                else values[i] <- prop.Pickler.DefaultValueUntyped

            case.CaseCtor.Invoke(null, values) :?> 'U

    override _.PicklerType = PicklerType.Union
    override _.PickleType = PickleType.Map
    override _.DefaultValue = invalidOp "default values not supported for unions."

    override __.Pickle(union: 'U) =
        let ro = __.OfUnion union
        Some <| AttributeValue(M = ro)

    override __.UnPickle a = if a.IsMSet then __.ToUnion a.M else invalidCast a
