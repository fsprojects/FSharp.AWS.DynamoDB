namespace FSharp.AWS.DynamoDB.Tests

open System.Collections.Generic
open System.IO

open Swensen.Unquote
open Xunit

open FSharp.AWS.DynamoDB

open Amazon.DynamoDBv2.Model

type ``DynamoUtils Tests``() =

    [<Fact>]
    let ``AttributeValue.Print() works for S`` () =
        let attributeValue = AttributeValue(S = "abc")
        let expected = "{ S = abc }"

        test <@ attributeValue.Print() = expected @>

    [<Fact>]
    let ``AttributeValue.Print() works for N`` () =
        let attributeValue = AttributeValue(N = "123")
        let expected = "{ N = 123 }"

        test <@ attributeValue.Print() = expected @>

    [<Fact>]
    let ``AttributeValue.Print() works for B`` () =
        let attributeValue = AttributeValue(B = new MemoryStream([| 1uy; 2uy; 3uy |]))
        let expected = "{ B = [|1; 2; 3|] }"

        test <@ attributeValue.Print() = expected @>

    [<Fact>]
    let ``AttributeValue.Print() works for SS`` () =
        let attributeValue = AttributeValue(SS = ResizeArray [ "a"; "b"; "c" ])
        let expected = "{ SS = [|\"a\"; \"b\"; \"c\"|] }"

        test <@ attributeValue.Print() = expected @>

    [<Fact>]
    let ``AttributeValue.Print() works for NS`` () =
        let attributeValue = AttributeValue(NS = ResizeArray [ "1"; "2"; "3" ])
        let expected = "{ NS = [|\"1\"; \"2\"; \"3\"|] }"

        test <@ attributeValue.Print() = expected @>

    [<Fact>]
    let ``AttributeValue.Print() works for BS`` () =
        let attributeValue =
            AttributeValue(BS = ResizeArray [ new MemoryStream([| 1uy; 2uy; 3uy |]); new MemoryStream([| 4uy; 5uy; 6uy |]) ])
        let expected = "{ BS = [|[|1; 2; 3|]; [|4; 5; 6|]|] }"

        test <@ attributeValue.Print() = expected @>

    [<Fact>]
    let ``AttributeValue.Print() works for L`` () =
        let attributeValue = AttributeValue(L = ResizeArray [ AttributeValue(S = "a"); AttributeValue(S = "b"); AttributeValue(S = "c") ])
        let expected = "{ L = [|{ S = a }; { S = b }; { S = c }|] }"

        test <@ attributeValue.Print() = expected @>

    [<Fact>]
    let ``AttributeValue.Print() works for M`` () =
        let attributeValue =
            AttributeValue(
                M =
                    Dictionary<string, AttributeValue>(
                        [ "a", AttributeValue(S = "1"); "b", AttributeValue(S = "2"); "c", AttributeValue(S = "3") ]
                        |> Seq.map (fun (k, v) -> KeyValuePair(k, v))
                    )
            )

        let expected = "{ M = [|(\"a\", { S = 1 }); (\"b\", { S = 2 }); (\"c\", { S = 3 })|] }"

        test <@ attributeValue.Print() = expected @>
