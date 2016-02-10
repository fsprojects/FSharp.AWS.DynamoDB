#I "../../bin"
#r "Rql.Core.dll"
#r "Unquote.dll"

open Rql.Core

type Union = A | B | C of string

type Record =
    {
        [<CustomName("Primary")>]
        A : string
        [<CustomName("Secondary")>]
        B : string

        F : string option

        Value : Map<int, string>

        [<BinaryFormatter>]
        Union : Union
    }

let rfm = RecordFieldExtractor.Create<Record>()

let fields = rfm.ExtractFields { A = "test1" ; B = "test2" ; F = Some "42" ; Value = Map.ofList [42, "hello"] ; Union = C "asda" }

rfm.Rebuild fields
rfm.Rebuild <| dict [("Primary", box "asda") ; ("Secondary", box "sda") ; ("Union", fields.["Union"]) ]

rfm.ExtractFields <@ fun r -> { r with A = "test" ; B = "B" ; F = Some "42" } @>

let q = rfm.ExtractQuery <@ fun r -> r.A = "hello" + "world" && r.Union = r.Union @>
let q' = rfm.ExtractQuery <@ fun r -> r.A = "hello" + "world" && r.Union = r.Union @>

rfm.ExtractQuery <@ fun r -> r.A >= "hello" @>