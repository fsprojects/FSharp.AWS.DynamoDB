namespace FSharp.DynamoDB.FieldConverter

open System
open System.Collections
open System.Collections.Generic
open System.Collections.Concurrent
open System.IO
open System.Reflection

open Microsoft.FSharp.Reflection

open FSharp.DynamoDB
open FSharp.DynamoDB.TypeShape
open FSharp.DynamoDB.FieldConverter
open FSharp.DynamoDB.FieldConverter.ValueConverters
open FSharp.DynamoDB.FieldConverter.RecordConverter

[<AutoOpen>]
module private ResolverImpl =

    let resolveFieldConverter (resolver : IFieldConverterResolver) (t : Type) : FieldConverter =
        let resolveSR (requestingType : Type) =
            match resolver.Resolve<'T> () with
            | :? StringRepresentableFieldConverter<'T> as sr -> sr
            | _ -> UnSupportedField.Raise requestingType 

        match getShape t with
        | :? ShapeBool -> new BoolConverter() :> _
        | :? ShapeByte -> mkNumericalConverter<byte> () :> _
        | :? ShapeSByte -> mkNumericalConverter<sbyte> () :> _
        | :? ShapeInt16 -> mkNumericalConverter<int16> () :> _
        | :? ShapeInt32 -> mkNumericalConverter<int32> () :> _
        | :? ShapeInt64 -> mkNumericalConverter<int64> () :> _
        | :? ShapeUInt16 -> mkNumericalConverter<uint16> () :> _
        | :? ShapeUInt32 -> mkNumericalConverter<uint32> () :> _
        | :? ShapeUInt64 -> mkNumericalConverter<uint64> () :> _
        | :? ShapeSingle -> mkNumericalConverter<single> () :> _
        | :? ShapeDouble -> mkNumericalConverter<double> () :> _
        | :? ShapeDecimal -> mkNumericalConverter<decimal> () :> _
        | :? ShapeString -> new StringConverter() :> _
        | :? ShapeGuid -> new GuidConverter() :> _
        | :? ShapeByteArray -> new BytesConverter() :> _
        | :? ShapeTimeSpan -> new TimeSpanConverter() :> _
        | :? ShapeDateTime -> UnSupportedField.Raise(t, "please use DateTimeOffset instead.")
        | :? ShapeDateTimeOffset -> new DateTimeOffsetConverter() :> _
        | ShapeEnum s ->
            s.Accept {
                new IEnumVisitor<FieldConverter> with
                    member __.VisitEnum<'E, 'U when 'E : enum<'U>> () =
                        new EnumerationConverter<'E, 'U>(resolveSR typeof<'E>) :> _ }

        | ShapeNullable s ->
            s.Accept {
                new INullableVisitor<FieldConverter> with
                    member __.VisitNullable<'T when 'T : (new : unit -> 'T) and 'T :> ValueType and 'T : struct> () = 
                        new NullableConverter<'T>(resolver.Resolve()) :> _ }

        | ShapeFSharpRef s ->
            s.Accept {
                new IFSharpRefVisitor<FieldConverter> with
                    member __.VisitFSharpRef<'T> () =
                        mkFSharpRefConverter<'T> (resolver.Resolve()) :> _ }

        | ShapeFSharpOption s ->
            s.Accept {
                new IFSharpOptionVisitor<FieldConverter> with
                    member __.VisitFSharpOption<'T> () =
                        let tconv = resolver.Resolve<'T>()
                        new OptionConverter<'T>(tconv) :> _ }

        | ShapeArray s ->
            s.Accept {
                new IArrayVisitor<FieldConverter> with
                    member __.VisitArray<'T> () =
                        let tconv = resolver.Resolve<'T>()
                        new ListConverter<'T [], 'T>(Seq.toArray, tconv) :> _ }

        | ShapeFSharpList s ->
            s.Accept {
                new IFSharpListVisitor<FieldConverter> with
                    member __.VisitFSharpList<'T> () =
                        let tconv = resolver.Resolve<'T>()
                        new ListConverter<'T list, 'T>(List.ofSeq, tconv) :> _ }

        | ShapeResizeArray s ->
            s.Accept {
                new IResizeArrayVisitor<FieldConverter> with
                    member __.VisitResizeArray<'T> () =
                        let tconv = resolver.Resolve<'T>()
                        new ListConverter<ResizeArray<'T>, 'T>(rlist, tconv) :> _ }

        | ShapeHashSet s ->
            s.Accept {
                new IHashSetVisitor<FieldConverter> with
                    member __.VisitHashSet<'T when 'T : equality> () =
                        if typeof<'T> = typeof<byte []> then
                            BytesSetConverter<HashSet<byte []>>(HashSet) :> _
                        else
                            mkSetConverter<_,'T> HashSet (resolver.Resolve()) :> _ }

        | ShapeFSharpSet s ->
            s.Accept {
                new IFSharpSetVisitor<FieldConverter> with
                    member __.VisitFSharpSet<'T when 'T : comparison> () =
                        if typeof<'T> = typeof<byte []> then
                            BytesSetConverter<Set<byte []>>(Set.ofSeq) :> _
                        else
                            mkSetConverter<_,'T> Set.ofSeq (resolver.Resolve()) :> _ }

        | ShapeDictionary s ->
            s.Accept {
                new IDictionaryVisitor<FieldConverter> with
                    member __.VisitDictionary<'K, 'V when 'K : equality> () =
                        new MapConverter<Dictionary<'K, 'V>, 'K, 'V>(cdict, resolveSR typeof<Dictionary<'K,'V>>, resolver.Resolve()) :> _ }

        | ShapeFSharpMap s ->
            s.Accept { 
                new IFSharpMapVisitor<FieldConverter> with
                    member __.VisitFSharpMap<'K, 'V when 'K : comparison> () =
                        let mkMap (kvs : seq<KeyValuePair<'K,'V>>) =
                            kvs |> Seq.map (fun kv -> kv.Key, kv.Value) |> Map.ofSeq

                        new MapConverter<Map<'K,'V>, 'K, 'V>(mkMap, resolveSR typeof<Map<'K,'V>>, resolver.Resolve()) :> _ }

        | ShapeCollection s ->
            s.Accept {
                new ICollectionVisitor<FieldConverter> with
                    member __.VisitCollection<'T> () =
                        let tconv = resolver.Resolve<'T>()
                        new ListConverter<ICollection<'T>, 'T>(Seq.toArray >> unbox, tconv) :> _ }

        | ShapeEnumerable s ->
            s.Accept {
                new IEnumerableVisitor<FieldConverter> with
                    member __.VisitEnumerable<'T> () =
                        let tconv = resolver.Resolve<'T>()
                        new ListConverter<seq<'T>, 'T>(Seq.toArray >> unbox, tconv) :> _ }

        | ShapeTuple as s ->
            s.Accept {
                new IFunc<FieldConverter> with
                    member __.Invoke<'T> () = mkTupleConverter<'T> resolver :> _ }

        | s when FSharpType.IsRecord(t, true) ->
            s.Accept {
                new IFunc<FieldConverter> with
                    member __.Invoke<'T>() = mkFSharpRecordConverter<'T> resolver :> _   }

        | _ -> UnSupportedField.Raise t

    type CachedResolver private () as self =
        let cache = new System.Collections.Concurrent.ConcurrentDictionary<Type, FieldConverter>()
        let resolve t = cache.GetOrAdd(t, resolveFieldConverter self)
        static let instance = new CachedResolver()

        static member Instance = instance :> IFieldConverterResolver

        interface IFieldConverterResolver with
            member __.Resolve(t : Type) = resolve t
            member __.Resolve<'T> () = resolve typeof<'T> :?> FieldConverter<'T>

[<CompilationRepresentation(CompilationRepresentationFlags.ModuleSuffix)>]
module FieldConverter =

    let resolver = CachedResolver.Instance
    let resolveUntyped (t : Type) = CachedResolver.Instance.Resolve t
    let resolve<'T> () = CachedResolver.Instance.Resolve<'T> ()