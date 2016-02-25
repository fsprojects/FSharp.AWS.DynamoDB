namespace FSharp.DynamoDB.Tests

open System
open Amazon.DynamoDBv2
open Amazon.DynamoDBv2.DataModel

open Xunit
open FsUnit.Xunit
open FsCheck

open FSharp.DynamoDB

module ``Record Generation Tests`` =

    let testRoundTrip<'Record> () =
        let rt = RecordTemplate.Define<'Record>()
        let roundTrip (r : 'Record) =
            rt.ToAttributeValues r
            |> rt.OfAttributeValues
            |> should equal r

        Check.Quick round

    // Section A. Simple Record schemata

    type ``S Record`` = { [<HashKey>] A1 : string }
    type ``N Record`` = { [<HashKey>] A1 : uint16 }
    type ``B Record`` = { [<HashKey>] A1 : byte[] }

    type ``SN Record`` = { [<HashKey>] A1 : string ; [<RangeKey>] B1 : int64  }
    type ``NB Record`` = { [<HashKey>] A1 : uint16 ; [<RangeKey>] B1 : byte[] }
    type ``BS Record`` = { [<HashKey>] A1 : byte[] ; [<RangeKey>] B1 : string }

    [<HashKeyConstant("HashKey", 42)>]
    type ``NS Constant HashKey Record`` = { [<RangeKey>] B1 : string }

    [<RangeKeyConstant("RangeKey", "Constant")>]
    type ``BS Constant RangeKey Record`` = { [<HashKey>] A1 : byte[] }

    type ``Custom HashKey Name Record`` = { [<HashKey; CustomName("CustomHashKeyName")>] B1 : string }

    [<Fact>]
    let ``Generate correct schema for S Record`` () =
        let rt = RecordTemplate.Define<``S Record``>()
        rt.KeySchema.HashKey.AttributeName |> should equal "A1"
        rt.KeySchema.HashKey.KeyType |> should equal ScalarAttributeType.S
        rt.KeySchema.RangeKey |> should equal None

    [<Fact>]
    let ``Generate correct schema for N Record`` () =
        let rt = RecordTemplate.Define<``N Record``>()
        rt.KeySchema.HashKey.AttributeName |> should equal "A1"
        rt.KeySchema.HashKey.KeyType |> should equal ScalarAttributeType.N
        rt.KeySchema.RangeKey |> should equal None

    [<Fact>]
    let ``Generate correct schema for B Record`` () =
        let rt = RecordTemplate.Define<``B Record``>()
        rt.KeySchema.HashKey.AttributeName |> should equal "A1"
        rt.KeySchema.HashKey.KeyType |> should equal ScalarAttributeType.B
        rt.KeySchema.RangeKey |> should equal None

    [<Fact>]
    let ``Generate correct schema for SN Record`` () =
        let rt = RecordTemplate.Define<``SN Record``>()
        rt.KeySchema.HashKey.AttributeName |> should equal "A1"
        rt.KeySchema.HashKey.KeyType |> should equal ScalarAttributeType.S
        rt.KeySchema.RangeKey |> should equal (Some { AttributeName = "B1" ; KeyType = ScalarAttributeType.N })

    [<Fact>]
    let ``Generate correct schema for NB Record`` () =
        let rt = RecordTemplate.Define<``NB Record``>()
        rt.KeySchema.HashKey.AttributeName |> should equal "A1"
        rt.KeySchema.HashKey.KeyType |> should equal ScalarAttributeType.N
        rt.KeySchema.RangeKey |> should equal (Some { AttributeName = "B1" ; KeyType = ScalarAttributeType.B })

    [<Fact>]
    let ``Generate correct schema for BS Record`` () =
        let rt = RecordTemplate.Define<``BS Record``>()
        rt.KeySchema.HashKey.AttributeName |> should equal "A1"
        rt.KeySchema.HashKey.KeyType |> should equal ScalarAttributeType.B
        rt.KeySchema.RangeKey |> should equal (Some { AttributeName = "B1" ; KeyType = ScalarAttributeType.S })

    [<Fact>]
    let ``Generate correct schema for NS Constant HashKey Record`` () =
        let rt = RecordTemplate.Define<``NS Constant HashKey Record``> ()
        rt.KeySchema.HashKey.AttributeName |> should equal "HashKey"
        rt.KeySchema.HashKey.KeyType |> should equal ScalarAttributeType.N
        rt.KeySchema.RangeKey |> should equal (Some { AttributeName = "B1" ; KeyType = ScalarAttributeType.S })

    [<Fact>]
    let ``Generate correct schema for BS Constant RangeKey Record`` () =
        let rt = RecordTemplate.Define<``BS Constant RangeKey Record``> ()
        rt.KeySchema.HashKey.AttributeName |> should equal "A1"
        rt.KeySchema.HashKey.KeyType |> should equal ScalarAttributeType.B
        rt.KeySchema.RangeKey |> should equal (Some { AttributeName = "RangeKey" ; KeyType = ScalarAttributeType.S })

    [<Fact>]
    let ``Generate correct schema for Custom HashKey Name Record`` () =
        let rt = RecordTemplate.Define<``Custom HashKey Name Record``> ()
        rt.KeySchema.HashKey.AttributeName |> should equal "CustomHashKeyName"
        rt.KeySchema.HashKey.KeyType |> should equal ScalarAttributeType.S
        rt.KeySchema.RangeKey |> should equal None


    [<Fact>]
    let ``Attribute value roundtrip for S Record``() =
        testRoundTrip<``S Record``> ()

    [<Fact>]
    let ``Attribute value roundtrip for N Record``() =
        testRoundTrip<``N Record``> ()

    [<Fact>]
    let ``Attribute value roundtrip for B Record``() =
        testRoundTrip<``B Record``> ()

    [<Fact>]
    let ``Attribute value roundtrip for SN Record``() =
        testRoundTrip<``SN Record``> ()

    [<Fact>]
    let ``Attribute value roundtrip for NB Record``() =
        testRoundTrip<``NB Record``> ()

    [<Fact>]
    let ``Attribute value roundtrip for BS Record``() =
        testRoundTrip<``BS Record``> ()

    [<Fact>]
    let ``Attribute value roundtrip for NS constant HashKey Record`` () =
        testRoundTrip<``NS Constant HashKey Record``> ()


    // Section B. Complex Record schemata

    type ``Complex Record A`` = 
        { 
            [<HashKey>]HashKey : string
            [<RangeKey>]RangeKey : string

            TimeSpan : TimeSpan

            Tuple : int * string * System.Reflection.BindingFlags option

            DateTimeOffset : DateTimeOffset
            Values : int list
            Map : Map<string, TimeSpan>
            Set : Set<TimeSpan>

            [<BinaryFormatter>]
            BlobValue : (int * string) [][]
        }

    type NestedRecord = { A : int ; B : string }

    type NestedUnion = UA of int | UB of string | UC of byte[] * DateTimeOffset

    type ``Complex Record B`` = 
        { 
            [<HashKey>]HashKey : byte[]
            [<RangeKey>]RangeKey : float

            Values : byte [][]
            Map : Map<string, decimal>
            Set : Set<string> ref

            Nested : NestedRecord
            Union : NestedUnion

            [<BinaryFormatter>]
            BlobValue : (int * string) [][]
        }

    type ``Complex Record C`` = 
        { 
            [<HashKey>]HashKey : decimal
            [<RangeKey>]RangeKey : byte

            Bool : bool
            Byte : byte
            SByte : sbyte
            Int16 : int16
            Int32 : int32
            Int64 : int64
            UInt16 : uint16
            UInt32 : uint32
            UInt64 : uint64
            Single : single
            Double : double
            Decimal : decimal

            Nested : NestedRecord * NestedUnion
        }

    type ``Complex Record D`` =
        {
            [<HashKey>]HashKey : byte[]
            [<RangeKey>]RangeKey : int64

            Guid : Guid
            Enum : System.Reflection.BindingFlags
            EnumArray : System.Reflection.BindingFlags []
            GuidSet : Set<Guid>
            Nullable : Nullable<int64>
            Optional : string option
            Bytess : Set<byte[]>
            Ref : byte[] ref ref ref
            Unions : Choice<NestedUnion [], int>
        }



    [<Fact>]
    let ``Roundtrip complex record A`` () =
        testRoundTrip<``Complex Record A``> ()

    [<Fact>]
    let ``Roundtrip complex record B`` () =
        testRoundTrip<``Complex Record B``> ()

    [<Fact>]
    let ``Roundtrip complex record C`` () =
        testRoundTrip<``Complex Record C``> ()

    [<Fact>]
    let ``Roundtrip complex record D`` () =
        testRoundTrip<``Complex Record D``> ()


    // Section C: test errors

    type ``Record lacking key attributes`` = { A1 : string ; B1 : string }

    [<Fact>]
    let ``Record lacking key attributes should fail``() =
        fun () -> RecordTemplate.Define<``Record lacking key attributes``>()
        |> shouldFailwith<_, ArgumentException>


    type ``Record lacking hashkey attribute`` = { A1 : string ; [<RangeKey>] B1 : string }

    [<Fact>]
    let ``Record lacking hashkey attribute should fail`` () =
        fun () -> RecordTemplate.Define<``Record lacking hashkey attribute``>()
        |> shouldFailwith<_, ArgumentException>


    type ``Record containing unsupported attribute type`` = { [<HashKey>]A1 : string ; [<RangeKey>] B1 : string ; C1 : string list }

    [<Fact>]
    let ``Record containing unsupported attribute type should fail`` () =
        fun () -> RecordTemplate.Define<``Record lacking hashkey attribute``>()
        |> shouldFailwith<_, ArgumentException>

    type ``Record containing key field of unsupported type`` = { [<HashKey>]A1 : bool }

    [<Fact>]
    let ``Record containing key field of unsupported type should fail`` () =
        fun () -> RecordTemplate.Define<``Record containing key field of unsupported type``>()
        |> shouldFailwith<_, ArgumentException>


    type ``Record containing multiple HashKey attributes`` = { [<HashKey>]A1 : bool ; [<HashKey>]A2 : string }

    [<Fact>]
    let ``Record containing multiple HashKey attributes should fail`` () =
        fun () -> RecordTemplate.Define<``Record containing multiple HashKey attributes``>()
        |> shouldFailwith<_, ArgumentException>

    [<HashKeyConstant("HashKey", "HashKeyValue")>]
    type ``Record containing costant HashKey attribute lacking RangeKey attribute`` = { Value : int }

    [<Fact>]
    let ``Record containing costant HashKey attribute lacking RangeKey attribute should fail`` () =
        fun () -> RecordTemplate.Define<``Record containing costant HashKey attribute lacking RangeKey attribute``>()
        |> shouldFailwith<_, ArgumentException>

    [<HashKeyConstant("HashKey", "HashKeyValue")>]
    type ``Record containing costant HashKey attribute with HashKey attribute`` = 
        { [<HashKey>]HashKey : string ; [<RangeKey>]RangeKey : string }

    [<Fact>]
    let ``Record containing costant HashKey attribute with HashKey attribute should fail`` () =
        fun () -> RecordTemplate.Define<``Record containing costant HashKey attribute with HashKey attribute``>()
        |> shouldFailwith<_, ArgumentException>