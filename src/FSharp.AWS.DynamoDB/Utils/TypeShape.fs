module internal FSharp.DynamoDB.TypeShape

open System
open System.Collections.Generic
open System.Reflection

type IFunc<'R> =
    abstract Invoke<'T> : unit -> 'R

[<AbstractClass>]
type TypeShape internal () =
    abstract Type : Type
    abstract Accept : ITypeShapeVisitor<'R> -> 'R
    abstract Accept : IFunc<'R> -> 'R
    override s.ToString() = sprintf "%O" (s.GetType())

and [<AbstractClass>] TypeShape<'T> internal () =
    inherit TypeShape()
    override __.Type = typeof<'T>
    member __.TypeCode = Type.GetTypeCode typeof<'T>
    override __.Accept<'R> (func : IFunc<'R>) = func.Invoke<'T> ()

and ShapeUnknown<'T> internal () =
    inherit TypeShape<'T>()
    override __.Accept(v : ITypeShapeVisitor<'R>) = v.VisitUnknown<'T>()

//////////// Primitives

and ShapeBool() =
    inherit TypeShape<bool> ()
    override __.Accept (v : ITypeShapeVisitor<'R>) = v.VisitBool()

and ShapeByte() =
    inherit TypeShape<byte> ()
    override __.Accept (v : ITypeShapeVisitor<'R>) = v.VisitByte()

and ShapeSByte() =
    inherit TypeShape<sbyte> ()
    override __.Accept (v : ITypeShapeVisitor<'R>) = v.VisitSByte()

and ShapeInt16() =
    inherit TypeShape<int16> ()
    override __.Accept (v : ITypeShapeVisitor<'R>) = v.VisitInt16()

and ShapeInt32() =
    inherit TypeShape<int32> ()
    override __.Accept (v : ITypeShapeVisitor<'R>) = v.VisitInt32()

and ShapeInt64() =
    inherit TypeShape<int64> ()
    override __.Accept (v : ITypeShapeVisitor<'R>) = v.VisitInt64()

and ShapeUInt16() =
    inherit TypeShape<uint16> ()
    override __.Accept (v : ITypeShapeVisitor<'R>) = v.VisitUInt16()

and ShapeUInt32() =
    inherit TypeShape<uint32> ()
    override __.Accept (v : ITypeShapeVisitor<'R>) = v.VisitUInt32()

and ShapeUInt64() =
    inherit TypeShape<uint64> ()
    override __.Accept (v : ITypeShapeVisitor<'R>) = v.VisitUInt64()

and ShapeNativeInt() =
    inherit TypeShape<nativeint> ()
    override __.Accept (v : ITypeShapeVisitor<'R>) = v.VisitNativeInt()

and ShapeUNativeInt() =
    inherit TypeShape<unativeint> ()
    override __.Accept (v : ITypeShapeVisitor<'R>) = v.VisitUNativeInt()

and ShapeSingle() =
    inherit TypeShape<single> ()
    override __.Accept (v : ITypeShapeVisitor<'R>) = v.VisitSingle()

and ShapeDouble() =
    inherit TypeShape<double> ()
    override __.Accept (v : ITypeShapeVisitor<'R>) = v.VisitDouble()

and ShapeString() =
    inherit TypeShape<string> ()
    override __.Accept (v : ITypeShapeVisitor<'R>) = v.VisitString()

and ShapeGuid() =
    inherit TypeShape<Guid> ()
    override __.Accept (v : ITypeShapeVisitor<'R>) = v.VisitGuid()

and ShapeDecimal() =
    inherit TypeShape<decimal> ()
    override __.Accept (v : ITypeShapeVisitor<'R>) = v.VisitDecimal()

and ShapeTimeSpan() =
    inherit TypeShape<TimeSpan> ()
    override __.Accept (v : ITypeShapeVisitor<'R>) = v.VisitTimeSpan()

and ShapeDateTime() =
    inherit TypeShape<DateTime> ()
    override __.Accept (v : ITypeShapeVisitor<'R>) = v.VisitDateTime()

and ShapeDateTimeOffset() = 
    inherit TypeShape<DateTimeOffset> ()
    override __.Accept (v : ITypeShapeVisitor<'R>) = v.VisitDateTimeOffset()

/////////////// Enumerations

and IEnumVisitor<'R> =
    abstract VisitEnum<'Enum, 'Underlying when 'Enum : enum<'Underlying>> : unit -> 'R

and IShapeEnum =
    abstract Accept : IEnumVisitor<'R> -> 'R

and ShapeEnum<'Enum, 'Underlying when 'Enum : enum<'Underlying>>() =
    inherit TypeShape<'Enum> ()
    override __.Accept(v : ITypeShapeVisitor<'R>) = v.VisitEnum<'Enum, 'Underlying> ()
    interface IShapeEnum with
        member __.Accept v = v.VisitEnum<'Enum, 'Underlying> ()

/////////////// Collections & IEnumerable

and IEnumerableVisitor<'R> =
    abstract VisitEnumerable<'T> : unit -> 'R

and IShapeEnumerable =
    abstract Accept : IEnumerableVisitor<'R> -> 'R

and ShapeEnumerable<'T>() =
    inherit TypeShape<seq<'T>> ()
    override __.Accept(v : ITypeShapeVisitor<'R>) = v.VisitEnumerable<'T> ()
    interface IShapeEnumerable with
        member __.Accept v = v.VisitEnumerable<'T> ()

and ICollectionVisitor<'R> =
    abstract VisitCollection<'T> : unit -> 'R

and IShapeCollection =
    abstract Accept : ICollectionVisitor<'R> -> 'R

and ShapeCollection<'T>() =
    inherit TypeShape<ICollection<'T>>()
    override __.Accept(v : ITypeShapeVisitor<'R>) = v.VisitCollection<'T> ()
    interface IShapeCollection with
        member __.Accept v = v.VisitCollection<'T> ()

    interface IShapeEnumerable with
        member __.Accept v = v.VisitEnumerable<'T> ()

/////////////// Nullable

and INullableVisitor<'R> =
    abstract VisitNullable<'T when 'T : (new : unit -> 'T) and 'T :> ValueType and 'T : struct> : unit -> 'R

and IShapeNullable =
    abstract Accept : INullableVisitor<'R> -> 'R

and ShapeNullable<'T when 'T : (new : unit -> 'T) and 'T :> ValueType and 'T : struct>() =
    inherit TypeShape<Nullable<'T>>()
    override __.Accept (v : ITypeShapeVisitor<'R>) = v.VisitNullable<'T> ()
    interface IShapeNullable with
        member __.Accept v = v.VisitNullable<'T> ()


///////// KeyValuePair

and IKeyValuePairVisitor<'R> =
    abstract VisitKeyValuePair<'K, 'V> : unit -> 'R

and IShapeKeyValuePair =
    abstract Accept : IKeyValuePairVisitor<'R> -> 'R

and ShapeKeyValuePair<'K,'V> () =
    inherit TypeShape<KeyValuePair<'K,'V>> ()
    override __.Accept (v : ITypeShapeVisitor<'R>) = v.VisitKeyValuePair<'K, 'V>()

/////////////// System.Array

and IArrayVisitor<'R> =
    abstract VisitArray<'T> : unit -> 'R

and IShapeArray =
    abstract Accept : IArrayVisitor<'R> -> 'R

and ShapeArray<'T>() =
    inherit TypeShape<'T []> ()
    override __.Accept(v : ITypeShapeVisitor<'R>) = v.VisitArray<'T> ()
    interface IShapeArray with
        member __.Accept v = v.VisitArray<'T> ()

    interface IShapeCollection with
        member __.Accept v = v.VisitCollection<'T> ()

    interface IShapeEnumerable with
        member __.Accept v = v.VisitEnumerable<'T> ()

and ShapeByteArray() =
    inherit ShapeArray<byte> ()

and IArray2DVisitor<'R> =
    abstract VisitArray2D<'T> : unit -> 'R

and IShapeArray2D =
    abstract Accept : IArray2DVisitor<'R> -> 'R

and ShapeArray2D<'T>() =
    inherit TypeShape<'T [,]> ()
    override __.Accept(v : ITypeShapeVisitor<'R>) = v.VisitArray2D<'T> ()
    interface IShapeArray2D with
        member __.Accept v = v.VisitArray2D<'T> ()


and IArray3DVisitor<'R> =
    abstract VisitArray3D<'T> : unit -> 'R

and IShapeArray3D =
    abstract Accept : IArray3DVisitor<'R> -> 'R

and ShapeArray3D<'T>() =
    inherit TypeShape<'T [,,]> ()
    override __.Accept(v : ITypeShapeVisitor<'R>) = v.VisitArray3D<'T> ()
    interface IShapeArray3D with
        member __.Accept v = v.VisitArray3D<'T> ()


and IArray4DVisitor<'R> =
    abstract VisitArray4D<'T> : unit -> 'R

and IShapeArray4D =
    abstract Accept : IArray4DVisitor<'R> -> 'R

and ShapeArray4D<'T>() =
    inherit TypeShape<'T [,,,]> ()
    override __.Accept(v : ITypeShapeVisitor<'R>) = v.VisitArray4D<'T> ()
    interface IShapeArray4D with
        member __.Accept v = v.VisitArray4D<'T> ()

////////////// System.Collections.List

and IResizeArrayVisitor<'R> =
    abstract VisitResizeArray<'T> : unit -> 'R

and IShapeResizeArray =
    abstract Accept : IResizeArrayVisitor<'R> -> 'R

and ShapeResizeArray<'T> () =
    inherit TypeShape<ResizeArray<'T>> ()
    override __.Accept(v : ITypeShapeVisitor<'R>) = v.VisitResizeArray<'T> ()
    interface IShapeResizeArray with
        member __.Accept v = v.VisitResizeArray<'T> ()



////////////// System.Collections.Dictionary

and IDictionaryVisitor<'R> =
    abstract VisitDictionary<'K, 'V when 'K : equality> : unit -> 'R

and IShapeDictionary =
    abstract Accept : IDictionaryVisitor<'R> -> 'R

and ShapeDictionary<'K, 'V when 'K : equality> () =
    inherit TypeShape<Dictionary<'K, 'V>> ()
    override __.Accept(v : ITypeShapeVisitor<'R>) = v.VisitDictionary<'K, 'V> ()
    interface IShapeDictionary with
        member __.Accept v = v.VisitDictionary<'K, 'V> ()
    interface IShapeEnumerable with
        member __.Accept v = v.VisitEnumerable<KeyValuePair<'K, 'V>> ()
    interface IShapeCollection with
        member __.Accept v = v.VisitCollection<KeyValuePair<'K, 'V>> ()

////////////// System.Collections.HashSet

and IHashSetVisitor<'R> =
    abstract VisitHashSet<'T when 'T : equality> : unit -> 'R

and IShapeHashSet =
    abstract Accept : IHashSetVisitor<'R> -> 'R

and ShapeHashSet<'T when 'T : equality> () =
    inherit TypeShape<HashSet<'T>> ()
    override __.Accept(v : ITypeShapeVisitor<'R>) = v.VisitHashSet<'T> ()
    interface IShapeHashSet with
        member __.Accept v = v.VisitHashSet<'T> ()
    interface IShapeEnumerable with
        member __.Accept v = v.VisitEnumerable<'T> ()
    interface IShapeCollection with
        member __.Accept v = v.VisitCollection<'T> ()


//////// System.Tuple

and ITuple1Visitor<'R> =
    abstract VisitTuple<'T> : unit -> 'R

and IShapeTuple1 =
    abstract Accept : ITuple1Visitor<'R> -> 'R

and ShapeTuple<'T> () =
    inherit TypeShape<System.Tuple<'T>> ()
    override __.Accept(v : ITypeShapeVisitor<'R>) = v.VisitTuple<'T> ()
    interface IShapeTuple1 with
        member __.Accept v = v.VisitTuple<'T> ()

and ITuple2Visitor<'R> =
    abstract VisitTuple<'T1, 'T2> : unit -> 'R

and IShapeTuple2 =
    abstract Accept : ITuple2Visitor<'R> -> 'R

and ShapeTuple<'T1, 'T2> () =
    inherit TypeShape<'T1 * 'T2> ()
    override __.Accept(v : ITypeShapeVisitor<'R>) = v.VisitTuple<'T1,'T2> ()
    interface IShapeTuple2 with
        member __.Accept v = v.VisitTuple<'T1,'T2> ()

and ITuple3Visitor<'R> =
    abstract VisitTuple<'T1, 'T2, 'T3> : unit -> 'R

and IShapeTuple3 =
    abstract Accept : ITuple3Visitor<'R> -> 'R

and ShapeTuple<'T1, 'T2, 'T3> () =
    inherit TypeShape<'T1 * 'T2 * 'T3> ()
    override __.Accept(v : ITypeShapeVisitor<'R>) = v.VisitTuple<'T1, 'T2, 'T3> ()
    interface IShapeTuple3 with
        member __.Accept v = v.VisitTuple<'T1, 'T2, 'T3> ()

and ITuple4Visitor<'R> =
    abstract VisitTuple<'T1, 'T2, 'T3, 'T4> : unit -> 'R

and IShapeTuple4 =
    abstract Accept : ITuple4Visitor<'R> -> 'R

and ShapeTuple<'T1, 'T2, 'T3, 'T4> () =
    inherit TypeShape<'T1 * 'T2 * 'T3 * 'T4> ()
    override __.Accept(v : ITypeShapeVisitor<'R>) = v.VisitTuple<'T1, 'T2, 'T3, 'T4> ()
    interface IShapeTuple4 with
        member __.Accept v = v.VisitTuple<'T1, 'T2, 'T3, 'T4> ()

and ITuple5Visitor<'R> =
    abstract VisitTuple<'T1, 'T2, 'T3, 'T4, 'T5> : unit -> 'R

and IShapeTuple5 =
    abstract Accept : ITuple5Visitor<'R> -> 'R

and ShapeTuple<'T1, 'T2, 'T3, 'T4, 'T5> () =
    inherit TypeShape<'T1 * 'T2 * 'T3 * 'T4 * 'T5> ()
    override __.Accept(v : ITypeShapeVisitor<'R>) = v.VisitTuple<'T1, 'T2, 'T3, 'T4, 'T5> ()
    interface IShapeTuple5 with
        member __.Accept v = v.VisitTuple<'T1, 'T2, 'T3, 'T4, 'T5> ()

and ITuple6Visitor<'R> =
    abstract VisitTuple<'T1, 'T2, 'T3, 'T4, 'T5, 'T6> : unit -> 'R

and IShapeTuple6 =
    abstract Accept : ITuple6Visitor<'R> -> 'R

and ShapeTuple<'T1, 'T2, 'T3, 'T4, 'T5, 'T6> () =
    inherit TypeShape<'T1 * 'T2 * 'T3 * 'T4 * 'T5 * 'T6> ()
    override __.Accept(v : ITypeShapeVisitor<'R>) = v.VisitTuple<'T1, 'T2, 'T3, 'T4, 'T5, 'T6> ()
    interface IShapeTuple6 with
        member __.Accept v = v.VisitTuple<'T1, 'T2, 'T3, 'T4, 'T5, 'T6> ()

and ITuple7Visitor<'R> =
    abstract VisitTuple<'T1, 'T2, 'T3, 'T4, 'T5, 'T6, 'T7> : unit -> 'R

and IShapeTuple7 =
    abstract Accept : ITuple7Visitor<'R> -> 'R

and ShapeTuple<'T1, 'T2, 'T3, 'T4, 'T5, 'T6, 'T7> () =
    inherit TypeShape<'T1 * 'T2 * 'T3 * 'T4 * 'T5 * 'T6 * 'T7> ()
    override __.Accept(v : ITypeShapeVisitor<'R>) = v.VisitTuple<'T1, 'T2, 'T3, 'T4, 'T5, 'T6, 'T7> ()
    interface IShapeTuple7 with
        member __.Accept v = v.VisitTuple<'T1, 'T2, 'T3, 'T4, 'T5, 'T6, 'T7> ()

and ITuple8Visitor<'R> =
    abstract VisitTuple<'T1, 'T2, 'T3, 'T4, 'T5, 'T6, 'T7, 'TRest> : unit -> 'R

and IShapeTuple8 =
    abstract Accept : ITuple8Visitor<'R> -> 'R

and ShapeTuple<'T1, 'T2, 'T3, 'T4, 'T5, 'T6, 'T7, 'TRest> () =
    inherit TypeShape<Tuple<'T1, 'T2, 'T3, 'T4, 'T5, 'T6, 'T7, 'TRest>> ()
    override __.Accept(v : ITypeShapeVisitor<'R>) = v.VisitTuple<'T1, 'T2, 'T3, 'T4, 'T5, 'T6, 'T7, 'TRest> ()
    interface IShapeTuple8 with
        member __.Accept v = v.VisitTuple<'T1, 'T2, 'T3, 'T4, 'T5, 'T6, 'T7, 'TRest> ()

/////////////////
/// F# Types


///// F# Option

and IFSharpOptionVisitor<'R> =
    abstract VisitFSharpOption<'T> : unit -> 'R

and IShapeFSharpOption =
    abstract Accept : IFSharpOptionVisitor<'R> -> 'R

and ShapeFSharpOption<'T> () =
    inherit TypeShape<'T option> ()
    override __.Accept(v : ITypeShapeVisitor<'R>) = v.VisitFSharpOption<'T> ()
    interface IShapeFSharpOption with
        member __.Accept v = v.VisitFSharpOption<'T> ()

///// F# List

and IFSharpListVisitor<'R> =
    abstract VisitFSharpList<'T> : unit -> 'R

and IShapeFSharpList =
    abstract Accept : IFSharpListVisitor<'R> -> 'R

and ShapeFSharpList<'T> () =
    inherit TypeShape<'T list> ()
    override __.Accept(v : ITypeShapeVisitor<'R>) = v.VisitFSharpList<'T> ()
    interface IShapeFSharpList with
        member __.Accept v = v.VisitFSharpList<'T> ()
    interface IShapeEnumerable with
        member __.Accept v = v.VisitEnumerable<'T> ()
    interface IShapeCollection with
        member __.Accept v = v.VisitCollection<'T> ()

///////// F# Ref

and IFSharpRefVisitor<'R> =
    abstract VisitFSharpRef<'T> : unit -> 'R

and IShapeFSharpRef =
    abstract Accept : IFSharpRefVisitor<'R> -> 'R

and ShapeFSharpRef<'T> () =
    inherit TypeShape<'T ref> ()
    override __.Accept(v : ITypeShapeVisitor<'R>) = v.VisitFSharpRef<'T> ()
    interface IShapeFSharpRef with
        member __.Accept v = v.VisitFSharpRef<'T> ()


///////// F# Choice

and IFSharpChoice2Visitor<'R> =
    abstract VisitFSharpChoice<'T1, 'T2> : unit -> 'R

and IShapeFSharpChoice2 =
    abstract Accept : IFSharpChoice2Visitor<'R> -> 'R

and ShapeFSharpChoice<'T1, 'T2>() =
    inherit TypeShape<Choice<'T1, 'T2>>()
    override __.Accept(v : ITypeShapeVisitor<'R>) = v.VisitFSharpChoice<'T1, 'T2>()
    interface IShapeFSharpChoice2 with
        member __.Accept v = v.VisitFSharpChoice<'T1, 'T2>()

and IFSharpChoice3Visitor<'R> =
    abstract VisitFSharpChoice<'T1, 'T2, 'T3> : unit -> 'R

and IShapeFSharpChoice3 =
    abstract Accept : IFSharpChoice3Visitor<'R> -> 'R

and ShapeFSharpChoice<'T1, 'T2, 'T3>() =
    inherit TypeShape<Choice<'T1, 'T2, 'T3>>()
    override __.Accept(v : ITypeShapeVisitor<'R>) = v.VisitFSharpChoice<'T1, 'T2, 'T3>()
    interface IShapeFSharpChoice3 with
        member __.Accept v = v.VisitFSharpChoice<'T1, 'T2, 'T3>()

and IFSharpChoice4Visitor<'R> =
    abstract VisitFSharpChoice<'T1, 'T2, 'T3, 'T4> : unit -> 'R

and IShapeFSharpChoice4 =
    abstract Accept : IFSharpChoice4Visitor<'R> -> 'R

and ShapeFSharpChoice<'T1, 'T2, 'T3, 'T4>() =
    inherit TypeShape<Choice<'T1, 'T2, 'T3, 'T4>>()
    override __.Accept(v : ITypeShapeVisitor<'R>) = v.VisitFSharpChoice<'T1, 'T2, 'T3, 'T4>()
    interface IShapeFSharpChoice4 with
        member __.Accept v = v.VisitFSharpChoice<'T1, 'T2, 'T3, 'T4>()

and IFSharpChoice5Visitor<'R> =
    abstract VisitFSharpChoice<'T1, 'T2, 'T3, 'T4, 'T5> : unit -> 'R

and IShapeFSharpChoice5 =
    abstract Accept : IFSharpChoice5Visitor<'R> -> 'R

and ShapeFSharpChoice<'T1, 'T2, 'T3, 'T4, 'T5>() =
    inherit TypeShape<Choice<'T1, 'T2, 'T3, 'T4, 'T5>>()
    override __.Accept(v : ITypeShapeVisitor<'R>) = v.VisitFSharpChoice<'T1, 'T2, 'T3, 'T4, 'T5>()
    interface IShapeFSharpChoice5 with
        member __.Accept v = v.VisitFSharpChoice<'T1, 'T2, 'T3, 'T4, 'T5>()

and IFSharpChoice6Visitor<'R> =
    abstract VisitFSharpChoice<'T1, 'T2, 'T3, 'T4, 'T5, 'T6> : unit -> 'R

and IShapeFSharpChoice6 =
    abstract Accept : IFSharpChoice6Visitor<'R> -> 'R

and ShapeFSharpChoice<'T1, 'T2, 'T3, 'T4, 'T5, 'T6>() =
    inherit TypeShape<Choice<'T1, 'T2, 'T3, 'T4, 'T5, 'T6>>()
    override __.Accept(v : ITypeShapeVisitor<'R>) = v.VisitFSharpChoice<'T1, 'T2, 'T3, 'T4, 'T5, 'T6>()
    interface IShapeFSharpChoice6 with
        member __.Accept v = v.VisitFSharpChoice<'T1, 'T2, 'T3, 'T4, 'T5, 'T6>()

and IFSharpChoice7Visitor<'R> =
    abstract VisitFSharpChoice<'T1, 'T2, 'T3, 'T4, 'T5, 'T6, 'T7> : unit -> 'R

and IShapeFSharpChoice7 =
    abstract Accept : IFSharpChoice7Visitor<'R> -> 'R

and ShapeFSharpChoice<'T1, 'T2, 'T3, 'T4, 'T5, 'T6, 'T7>() =
    inherit TypeShape<Choice<'T1, 'T2, 'T3, 'T4, 'T5, 'T6, 'T7>>()
    override __.Accept(v : ITypeShapeVisitor<'R>) = v.VisitFSharpChoice<'T1, 'T2, 'T3, 'T4, 'T5, 'T6, 'T7>()
    interface IShapeFSharpChoice7 with
        member __.Accept v = v.VisitFSharpChoice<'T1, 'T2, 'T3, 'T4, 'T5, 'T6, 'T7>()


///////// F# Set

and IFSharpSetVisitor<'R> =
    abstract VisitFSharpSet<'T when 'T : comparison> : unit -> 'R

and IShapeFSharpSet =
    abstract Accept : IFSharpSetVisitor<'R> -> 'R

and ShapeFSharpSet<'T when 'T : comparison> () =
    inherit TypeShape<Set<'T>> ()
    override __.Accept(v : ITypeShapeVisitor<'R>) = v.VisitFSharpSet<'T> ()
    interface IShapeFSharpSet with
        member __.Accept v = v.VisitFSharpSet<'T> ()
    interface IShapeEnumerable with
        member __.Accept v = v.VisitEnumerable<'T> ()
    interface IShapeCollection with
        member __.Accept v = v.VisitCollection<'T> ()

/////////// F# Map

and IFSharpMapVisitor<'R> =
    abstract VisitFSharpMap<'K, 'V when 'K : comparison> : unit -> 'R

and IShapeFSharpMap =
    abstract Accept : IFSharpMapVisitor<'R> -> 'R

and ShapeFSharpMap<'K, 'V when 'K : comparison> () =
    inherit TypeShape<Map<'K,'V>> ()
    override __.Accept(v : ITypeShapeVisitor<'R>) = v.VisitFSharpMap<'K, 'V>()
    interface IShapeFSharpMap with
        member __.Accept v = v.VisitFSharpMap<'K, 'V>()
    interface IShapeEnumerable with
        member __.Accept v = v.VisitEnumerable<KeyValuePair<'K, 'V>> ()
    interface IShapeCollection with
        member __.Accept v = v.VisitCollection<KeyValuePair<'K, 'V>> ()


/////////// F# func

and IFSharpFuncVisitor<'R> =
    abstract VisitFSharpFunc<'T, 'U> : unit -> 'R

and IShapeFSharpFunc =
    abstract Accept : IFSharpFuncVisitor<'R> -> 'R

and ShapeFSharpFunc<'T, 'U> () =
    inherit TypeShape<'T -> 'U> ()
    override __.Accept(v : ITypeShapeVisitor<'R>) = v.VisitFSharpFunc<'T, 'U> ()
    interface IShapeFSharpFunc with
        member __.Accept v = v.VisitFSharpFunc<'T, 'U> ()

/////////// F# Exception

and IFSharpExceptionVisitor<'R> =
    abstract VisitFSharpException<'exn when 'exn :> exn> : unit -> 'R

and IShapeFSharpException =
    abstract Accept : IFSharpExceptionVisitor<'R> -> 'R

and ShapeFSharpException<'exn when 'exn :> exn> internal () =
    inherit TypeShape<'exn> ()
    override __.Accept(v : ITypeShapeVisitor<'R>) = v.VisitFSharpException<'exn> ()
    interface IShapeFSharpException with
        member __.Accept v = v.VisitFSharpException<'exn> ()


/// VISITORS

and ITypeShapeVisitor<'R> =
    abstract VisitBool  : unit -> 'R
    abstract VisitByte  : unit -> 'R
    abstract VisitSByte : unit -> 'R
    abstract VisitInt16 : unit -> 'R
    abstract VisitInt32 : unit -> 'R
    abstract VisitInt64 : unit -> 'R
    abstract VisitUInt16 : unit -> 'R
    abstract VisitUInt32 : unit -> 'R
    abstract VisitUInt64 : unit -> 'R
    abstract VisitNativeInt : unit -> 'R
    abstract VisitUNativeInt : unit -> 'R
    abstract VisitSingle : unit -> 'R
    abstract VisitDouble : unit -> 'R
    abstract VisitString : unit -> 'R
    abstract VisitGuid : unit -> 'R
    abstract VisitTimeSpan : unit -> 'R
    abstract VisitDateTime : unit -> 'R
    abstract VisitDateTimeOffset : unit -> 'R
    abstract VisitDecimal : unit -> 'R

    abstract VisitUnknown<'T> : unit -> 'R

    inherit INullableVisitor<'R>
    inherit IEnumVisitor<'R>
    inherit IEnumerableVisitor<'R>
    inherit ICollectionVisitor<'R>
    inherit IKeyValuePairVisitor<'R>
    inherit IDictionaryVisitor<'R>
    inherit IHashSetVisitor<'R>
    inherit IResizeArrayVisitor<'R>

    inherit IArrayVisitor<'R>
    inherit IArray2DVisitor<'R>
    inherit IArray3DVisitor<'R>
    inherit IArray4DVisitor<'R>

    inherit ITuple1Visitor<'R>
    inherit ITuple2Visitor<'R>
    inherit ITuple3Visitor<'R>
    inherit ITuple4Visitor<'R>
    inherit ITuple5Visitor<'R>
    inherit ITuple6Visitor<'R>
    inherit ITuple7Visitor<'R>
    inherit ITuple8Visitor<'R>

    inherit IFSharpListVisitor<'R>
    inherit IFSharpOptionVisitor<'R>
    inherit IFSharpRefVisitor<'R>
    inherit IFSharpSetVisitor<'R>
    inherit IFSharpMapVisitor<'R>
    inherit IFSharpFuncVisitor<'R>
    inherit IFSharpExceptionVisitor<'R>

    inherit IFSharpChoice2Visitor<'R>
    inherit IFSharpChoice3Visitor<'R>
    inherit IFSharpChoice4Visitor<'R>
    inherit IFSharpChoice5Visitor<'R>
    inherit IFSharpChoice6Visitor<'R>
    inherit IFSharpChoice7Visitor<'R>


exception UnsupportedShape of Type:Type
 with
    override __.Message = sprintf "Unsupported TypeShape '%O'" __.Type

module private TypeShapeImpl =

    open System.Reflection
    open Microsoft.FSharp.Reflection

    let allConstructors = BindingFlags.Instance ||| BindingFlags.NonPublic ||| BindingFlags.Public

    let private allMembers =
        BindingFlags.NonPublic ||| BindingFlags.Public |||
            BindingFlags.Instance ||| BindingFlags.Static |||
                BindingFlags.FlattenHierarchy


    // typedefof does not work properly with 'enum' constraints
    let getGenericEnumType () = 
        typeof<ShapeEnum<BindingFlags,int>>.GetGenericTypeDefinition()

    let activate (gt : Type) (tp : Type []) =
        let ti = gt.MakeGenericType tp
        let ctor = ti.GetConstructor(allConstructors, null, CallingConventions.Standard, [||], [||])
        ctor.Invoke [||] :?> TypeShape

    let activate1 (gt : Type) (tp : Type) = activate gt [|tp|]
    let activate2 (gt : Type) (p1 : Type) (p2 : Type) = activate gt [|p1 ; p2|]

    let canon = Type.GetType("System.__Canon")

    /// correctly resolves if type is assignable to interface
    let rec private isAssignableFrom (interfaceTy : Type) (ty : Type) =
        let proj (t : Type) = t.Assembly, t.Namespace, t.Name, t.MetadataToken
        if interfaceTy = ty then true
        elif ty.GetInterfaces() |> Array.exists(fun if0 -> proj if0 = proj interfaceTy) then true
        else
            match ty.BaseType with
            | null -> false
            | bt -> isAssignableFrom interfaceTy bt
        
    /// use reflection to bootstrap a shape instance
    let resolveTypeShape (t : Type) : TypeShape =
        if t.IsGenericTypeDefinition then raise <| UnsupportedShape t
        elif t.IsGenericParameter then raise <| UnsupportedShape t
        elif t = canon then raise <| UnsupportedShape t
        elif t.IsPrimitive then
            if t = typeof<bool> then ShapeBool() :> _
            elif t = typeof<byte> then ShapeByte() :> _
            elif t = typeof<sbyte> then ShapeSByte() :> _
            elif t = typeof<uint16> then ShapeUInt16() :> _
            elif t = typeof<uint32> then ShapeUInt32() :> _
            elif t = typeof<uint64> then ShapeUInt64() :> _
            elif t = typeof<int16> then ShapeInt16() :> _
            elif t = typeof<int32> then ShapeInt32() :> _
            elif t = typeof<int64> then ShapeInt64() :> _
            elif t = typeof<single> then ShapeSingle() :> _
            elif t = typeof<double> then ShapeDouble() :> _
            elif t = typeof<nativeint> then ShapeNativeInt() :> _
            elif t = typeof<unativeint> then ShapeUNativeInt() :> _
            else activate1 typedefof<ShapeUnknown<_>> t

        elif t = typeof<decimal> then ShapeDecimal() :> _
        elif t = typeof<string> then ShapeString() :> _
        elif t = typeof<Guid> then ShapeGuid() :> _
        elif t = typeof<TimeSpan> then ShapeTimeSpan() :> _
        elif t = typeof<DateTime> then ShapeDateTime() :> _
        elif t = typeof<DateTimeOffset> then ShapeDateTimeOffset() :> _

        elif t.IsEnum then 
            activate2 (getGenericEnumType()) t <| Enum.GetUnderlyingType t

        elif t.IsArray then
            let et = t.GetElementType()
            match t.GetArrayRank() with
            | 1 when et = typeof<byte> -> new ShapeByteArray() :> _
            | 1 -> activate1 typedefof<ShapeArray<_>> et
            | 2 -> activate1 typedefof<ShapeArray2D<_>> et
            | 3 -> activate1 typedefof<ShapeArray3D<_>> et
            | 4 -> activate1 typedefof<ShapeArray4D<_>> et
            | _ -> raise <| UnsupportedShape t

        elif FSharpType.IsTuple t then
            let gas = t.GetGenericArguments()
            match gas.Length with
            | 1 -> activate typedefof<ShapeTuple<_>> gas
            | 2 -> activate typedefof<ShapeTuple<_,_>> gas
            | 3 -> activate typedefof<ShapeTuple<_,_,_>> gas
            | 4 -> activate typedefof<ShapeTuple<_,_,_,_>> gas
            | 5 -> activate typedefof<ShapeTuple<_,_,_,_,_>> gas
            | 6 -> activate typedefof<ShapeTuple<_,_,_,_,_,_>> gas
            | 7 -> activate typedefof<ShapeTuple<_,_,_,_,_,_,_>> gas
            | 8 -> activate typedefof<ShapeTuple<_,_,_,_,_,_,_,_>> gas
            | _ -> raise <| UnsupportedShape t

        elif FSharpType.IsExceptionRepresentation(t, true) then
            activate1 typedefof<ShapeFSharpException<_>> t

        elif FSharpType.IsFunction(t) then
            let d,c = FSharpType.GetFunctionElements t
            activate2 typedefof<ShapeFSharpFunc<_,_>> d c

        elif t.IsGenericType then
            let gt = t.GetGenericTypeDefinition()
            let gas = t.GetGenericArguments()

            if gt = typedefof<_ list> then 
                activate typedefof<ShapeFSharpList<_>> gas
            elif gt = typedefof<_ option> then 
                activate typedefof<ShapeFSharpOption<_>> gas
            elif 
                gt.Name.StartsWith "FSharpChoice" && 
                gt.Namespace = "Microsoft.FSharp.Core" && 
                gt.Assembly = typeof<int option>.Assembly 
            then
                match gas.Length with
                | 2 -> activate typedefof<ShapeFSharpChoice<_,_>> gas
                | 3 -> activate typedefof<ShapeFSharpChoice<_,_,_>> gas
                | 4 -> activate typedefof<ShapeFSharpChoice<_,_,_,_>> gas
                | 5 -> activate typedefof<ShapeFSharpChoice<_,_,_,_,_>> gas
                | 6 -> activate typedefof<ShapeFSharpChoice<_,_,_,_,_,_>> gas
                | 7 -> activate typedefof<ShapeFSharpChoice<_,_,_,_,_,_,_>> gas
                | _ -> raise <| UnsupportedShape t

            elif gt = typedefof<_ ref> then
                activate typedefof<ShapeFSharpRef<_>> gas
            elif gt = typedefof<System.Nullable<_>> then
                activate typedefof<ShapeNullable<_>> gas
            elif gt = typedefof<System.Collections.Generic.Dictionary<_,_>> then
                activate typedefof<ShapeDictionary<_,_>> gas
            elif gt = typedefof<System.Collections.Generic.HashSet<_>> then
                activate typedefof<ShapeHashSet<_>> gas
            elif gt = typedefof<System.Collections.Generic.List<_>> then
                activate typedefof<ShapeResizeArray<_>> gas
            elif gt = typedefof<Map<_,_>> then
                activate typedefof<ShapeFSharpMap<_,_>> gas
            elif gt = typedefof<Set<_>> then
                activate typedefof<ShapeFSharpSet<_>> gas
            elif gt = typedefof<KeyValuePair<_,_>> then
                activate typedefof<ShapeKeyValuePair<_,_>> gas
            elif isAssignableFrom typedefof<ICollection<_>> gt then
                activate typedefof<ShapeCollection<_>> gas
            elif isAssignableFrom typedefof<IEnumerable<_>> gt then
                activate typedefof<ShapeEnumerable<_>> gas
            else
                activate1 typedefof<ShapeUnknown<_>> t
        else 
            activate1 typedefof<ShapeUnknown<_>> t


    let dict = new System.Collections.Concurrent.ConcurrentDictionary<Type, TypeShape>()
    let resolveTypeShapeCached(t : Type) = dict.GetOrAdd(t, resolveTypeShape)

[<AutoOpen>]
module TypeShapeModule =

    /// Computes the type shape for given type
    let getShape (t : Type) = TypeShapeImpl.resolveTypeShapeCached t
    /// Computes the type shape for given object
    let getObjectShape (obj : obj) = TypeShapeImpl.resolveTypeShapeCached (obj.GetType())
    /// Computes the type shape for given type
    let shapeof<'T> = TypeShapeImpl.resolveTypeShapeCached typeof<'T>

    let inline private test<'If> (t : TypeShape) =
        match box t with
        | :? 'If as f -> Some f
        | _ -> None

    let (|ShapeBool|_|) t = test<ShapeBool> t
    let (|ShapeByte|_|) t = test<ShapeByte> t
    let (|ShapeSByte|_|) t = test<ShapeSByte> t
    let (|ShapeInt16|_|) t = test<ShapeInt16> t
    let (|ShapeInt32|_|) t = test<ShapeInt32> t
    let (|ShapeInt64|_|) t = test<ShapeInt64> t
    let (|ShapeUInt16|_|) t = test<ShapeUInt16> t
    let (|ShapeUInt32|_|) t = test<ShapeUInt32> t
    let (|ShapeUInt64|_|) t = test<ShapeUInt64> t
    let (|ShapeSingle|_|) t = test<ShapeSingle> t
    let (|ShapeDouble|_|) t = test<ShapeDouble> t

    let (|ShapeString|_|) t = test<ShapeString> t
    let (|ShapeGuid|_|) t = test<ShapeGuid> t
    let (|ShapeDecimal|_|) t = test<ShapeDecimal> t
    let (|ShapeTimeSpan|_|) t = test<ShapeTimeSpan> t
    let (|ShapeDateTime|_|) t = test<ShapeDateTime> t
    let (|ShapeDateTimeOffset|_|) t = test<ShapeDateTimeOffset> t
    
    let (|ShapeNullable|_|) t = test<IShapeNullable> t
    let (|ShapeEnum|_|) t = test<IShapeEnum> t
    let (|ShapeKeyValuePair|_|) t = test<IShapeKeyValuePair> t
    let (|ShapeDictionary|_|) t = test<IShapeDictionary> t
    let (|ShapeHashSet|_|) t = test<IShapeHashSet> t
    let (|ShapeResizeArray|_|) t = test<IShapeResizeArray> t

    let (|ShapeArray|_|) t = test<IShapeArray> t
    let (|ShapeByteArray|_|) t = test<ShapeByteArray> t
    let (|ShapeArray2D|_|) t = test<IShapeArray2D> t
    let (|ShapeArray3D|_|) t = test<IShapeArray3D> t
    let (|ShapeArray4D|_|) t = test<IShapeArray4D> t

    let (|ShapeTuple1|_|) t = test<IShapeTuple1> t
    let (|ShapeTuple2|_|) t = test<IShapeTuple2> t
    let (|ShapeTuple3|_|) t = test<IShapeTuple3> t
    let (|ShapeTuple4|_|) t = test<IShapeTuple4> t
    let (|ShapeTuple5|_|) t = test<IShapeTuple5> t
    let (|ShapeTuple6|_|) t = test<IShapeTuple6> t
    let (|ShapeTuple7|_|) t = test<IShapeTuple7> t
    let (|ShapeTuple8|_|) t = test<IShapeTuple8> t

    let (|ShapeFSharpList|_|) t = test<IShapeFSharpList> t
    let (|ShapeFSharpOption|_|) t = test<IShapeFSharpOption> t
    let (|ShapeFSharpRef|_|) t = test<IShapeFSharpRef> t
    let (|ShapeFSharpSet|_|) t = test<IShapeFSharpSet> t
    let (|ShapeFSharpMap|_|) t = test<IShapeFSharpMap> t
    let (|ShapeFSharpFunc|_|) t = test<IShapeFSharpFunc> t
    let (|ShapeFSharpException|_|) t = test<IShapeFSharpException> t

    let (|ShapeFSharpChoice2|_|) t = test<IShapeFSharpChoice2> t
    let (|ShapeFSharpChoice3|_|) t = test<IShapeFSharpChoice3> t
    let (|ShapeFSharpChoice4|_|) t = test<IShapeFSharpChoice4> t
    let (|ShapeFSharpChoice5|_|) t = test<IShapeFSharpChoice5> t
    let (|ShapeFSharpChoice6|_|) t = test<IShapeFSharpChoice6> t
    let (|ShapeFSharpChoice7|_|) t = test<IShapeFSharpChoice7> t

    let (|ShapeCollection|_|) t = test<IShapeCollection> t
    let (|ShapeEnumerable|_|) t = test<IShapeEnumerable> t

    let private SomeU = Some()

    let (|ShapePrimitive|_|) (t : TypeShape) =
        match t with
        | :? ShapeBool      -> SomeU
        | :? ShapeByte      -> SomeU
        | :? ShapeSByte     -> SomeU
        | :? ShapeInt16     -> SomeU
        | :? ShapeInt32     -> SomeU
        | :? ShapeInt64     -> SomeU
        | :? ShapeUInt16    -> SomeU
        | :? ShapeUInt32    -> SomeU
        | :? ShapeUInt64    -> SomeU
        | :? ShapeSingle    -> SomeU
        | :? ShapeDouble    -> SomeU
        | _ -> None

    let (|ShapeTuple|_|) (t : TypeShape) =
        match box t with
        | :? IShapeTuple1 -> SomeU
        | :? IShapeTuple2 -> SomeU
        | :? IShapeTuple3 -> SomeU
        | :? IShapeTuple4 -> SomeU
        | :? IShapeTuple5 -> SomeU
        | :? IShapeTuple6 -> SomeU
        | :? IShapeTuple7 -> SomeU
        | :? IShapeTuple8 -> SomeU
        | _ -> None