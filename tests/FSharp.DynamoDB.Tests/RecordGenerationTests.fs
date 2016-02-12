namespace FSharp.DynamoDB.Tests

open System

open Xunit
open FsUnit.Xunit

open FSharp.DynamoDB

module ``Record Generation Tests`` =

    type ``Record lacking key attributes`` = { A1 : string ; B1 : string }

    [<Fact>]
    let ``Record lacking key attributes should fail``() =
        fun () -> RecordDescriptor.Create<``Record lacking key attributes``>()
        |> shouldFailwith<_, ArgumentException>