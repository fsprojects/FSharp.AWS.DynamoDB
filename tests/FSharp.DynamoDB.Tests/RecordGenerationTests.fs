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
        let rd = RecordDescriptor.Create<'Record>()
        let roundTrip (r : 'Record) =
            rd.ToAttributeValues r
            |> rd.OfAttributeValues
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

    type ``Custom HashKey Name Record`` = { [<HashKey; CustomName("CustomHashKeyName")>] B1 : string }

    [<Fact>]
    let ``Generate correct schema for S Record`` () =
        let rd = RecordDescriptor.Create<``S Record``>()
        rd.KeySchema.HashKey.AttributeName |> should equal "A1"
        rd.KeySchema.HashKey.KeyType |> should equal ScalarAttributeType.S
        rd.KeySchema.RangeKey |> should equal None

    [<Fact>]
    let ``Generate correct schema for N Record`` () =
        let rd = RecordDescriptor.Create<``N Record``>()
        rd.KeySchema.HashKey.AttributeName |> should equal "A1"
        rd.KeySchema.HashKey.KeyType |> should equal ScalarAttributeType.N
        rd.KeySchema.RangeKey |> should equal None

    [<Fact>]
    let ``Generate correct schema for B Record`` () =
        let rd = RecordDescriptor.Create<``B Record``>()
        rd.KeySchema.HashKey.AttributeName |> should equal "A1"
        rd.KeySchema.HashKey.KeyType |> should equal ScalarAttributeType.B
        rd.KeySchema.RangeKey |> should equal None

    [<Fact>]
    let ``Generate correct schema for SN Record`` () =
        let rd = RecordDescriptor.Create<``SN Record``>()
        rd.KeySchema.HashKey.AttributeName |> should equal "A1"
        rd.KeySchema.HashKey.KeyType |> should equal ScalarAttributeType.S
        rd.KeySchema.RangeKey |> should equal (Some { AttributeName = "B1" ; KeyType = ScalarAttributeType.N })

    [<Fact>]
    let ``Generate correct schema for NB Record`` () =
        let rd = RecordDescriptor.Create<``NB Record``>()
        rd.KeySchema.HashKey.AttributeName |> should equal "A1"
        rd.KeySchema.HashKey.KeyType |> should equal ScalarAttributeType.N
        rd.KeySchema.RangeKey |> should equal (Some { AttributeName = "B1" ; KeyType = ScalarAttributeType.B })

    [<Fact>]
    let ``Generate correct schema for BS Record`` () =
        let rd = RecordDescriptor.Create<``BS Record``>()
        rd.KeySchema.HashKey.AttributeName |> should equal "A1"
        rd.KeySchema.HashKey.KeyType |> should equal ScalarAttributeType.B
        rd.KeySchema.RangeKey |> should equal (Some { AttributeName = "B1" ; KeyType = ScalarAttributeType.S })

    [<Fact>]
    let ``Generate correct schema for NS Constant HashKey Record`` () =
        let rd = RecordDescriptor.Create<``NS Constant HashKey Record``> ()
        rd.KeySchema.HashKey.AttributeName |> should equal "HashKey"
        rd.KeySchema.HashKey.KeyType |> should equal ScalarAttributeType.N
        rd.KeySchema.RangeKey |> should equal (Some { AttributeName = "B1" ; KeyType = ScalarAttributeType.S })

    [<Fact>]
    let ``Generate correct schema for Custom HashKey Name Record`` () =
        let rd = RecordDescriptor.Create<``Custom HashKey Name Record``> ()
        rd.KeySchema.HashKey.AttributeName |> should equal "CustomHashKeyName"
        rd.KeySchema.HashKey.KeyType |> should equal ScalarAttributeType.S
        rd.KeySchema.RangeKey |> should equal None


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

    type ``Complex Record B`` = 
        { 
            [<HashKey>]HashKey : byte[]
            [<RangeKey>]RangeKey : float

            Values : byte [][]
            Map : System.Collections.Generic.Dictionary<string, decimal>
            Set : System.Collections.Generic.HashSet<string> ref

            Nested : NestedRecord

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

            Nested : NestedRecord * NestedRecord
        }

    type ``Complex Record D`` =
        {
            [<HashKey>]HashKey : byte[]
            [<RangeKey>]RangeKey : int64

            Guid : Guid
            Enum : System.Reflection.BindingFlags
            EnumArray : System.Reflection.BindingFlags []
            GuidArray : System.Collections.Generic.HashSet<Guid>
            Nullable : Nullable<int64>
            Optional : string option
            Bytess : Set<byte[]>
            Ref : byte[] ref ref ref
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
        fun () -> RecordDescriptor.Create<``Record lacking key attributes``>()
        |> shouldFailwith<_, ArgumentException>


    type ``Record lacking hashkey attribute`` = { A1 : string ; [<RangeKey>] B1 : string }

    [<Fact>]
    let ``Record lacking hashkey attribute should fail`` () =
        fun () -> RecordDescriptor.Create<``Record lacking hashkey attribute``>()
        |> shouldFailwith<_, ArgumentException>


    type ``Record containing unsupported attribute type`` = { [<HashKey>]A1 : string ; [<RangeKey>] B1 : string ; C1 : string list }

    [<Fact>]
    let ``Record containing unsupported attribute type should fail`` () =
        fun () -> RecordDescriptor.Create<``Record lacking hashkey attribute``>()
        |> shouldFailwith<_, ArgumentException>

    type ``Record containing key field of unsupported type`` = { [<HashKey>]A1 : bool }

    [<Fact>]
    let ``Record containing key field of unsupported type should fail`` () =
        fun () -> RecordDescriptor.Create<``Record containing key field of unsupported type``>()
        |> shouldFailwith<_, ArgumentException>


    type ``Record containing multiple HashKey attributes`` = { [<HashKey>]A1 : bool ; [<HashKey>]A2 : string }

    [<Fact>]
    let ``Record containing multiple HashKey attributes should fail`` () =
        fun () -> RecordDescriptor.Create<``Record containing multiple HashKey attributes``>()
        |> shouldFailwith<_, ArgumentException>

    [<HashKeyConstant("HashKey", "HashKeyValue")>]
    type ``Record containing costant HashKey attribute lacking RangeKey attribute`` = { Value : int }

    [<Fact>]
    let ``Record containing costant HashKey attribute lacking RangeKey attribute should fail`` () =
        fun () -> RecordDescriptor.Create<``Record containing costant HashKey attribute lacking RangeKey attribute``>()
        |> shouldFailwith<_, ArgumentException>

    [<HashKeyConstant("HashKey", "HashKeyValue")>]
    type ``Record containing costant HashKey attribute with HashKey attribute`` = 
        { [<HashKey>]HashKey : string ; [<RangeKey>]RangeKey : string }

    [<Fact>]
    let ``Record containing costant HashKey attribute with HashKey attribute should fail`` () =
        fun () -> RecordDescriptor.Create<``Record containing costant HashKey attribute with HashKey attribute``>()
        |> shouldFailwith<_, ArgumentException>