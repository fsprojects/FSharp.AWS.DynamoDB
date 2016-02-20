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

[<AutoOpen>]
module private ResolverImpl =

    let resolveFieldConverter (resolver : IFieldConverterResolver) (t : Type) : FieldConverter =
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
                        let uconv = resolver.Resolve<'U>() :?> NumRepresentableFieldConverter<'U>
                        new EnumerationConverter<'E, 'U>(uconv) :> _ }

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
                        new ListConverter<'T [], 'T>(Seq.toArray, null, tconv) :> _ }

        | ShapeFSharpList s ->
            s.Accept {
                new IFSharpListVisitor<FieldConverter> with
                    member __.VisitFSharpList<'T> () =
                        let tconv = resolver.Resolve<'T>()
                        new ListConverter<'T list, 'T>(List.ofSeq, [], tconv) :> _ }

        | ShapeResizeArray s ->
            s.Accept {
                new IResizeArrayVisitor<FieldConverter> with
                    member __.VisitResizeArray<'T> () =
                        let tconv = resolver.Resolve<'T>()
                        new ListConverter<ResizeArray<'T>, 'T>(rlist, null, tconv) :> _ }

        | ShapeHashSet s ->
            s.Accept {
                new IHashSetVisitor<FieldConverter> with
                    member __.VisitHashSet<'T when 'T : equality> () =
                        if typeof<'T> = typeof<byte []> then
                            BytesSetConverter<HashSet<byte []>>(HashSet, null) :> _
                        else
                            mkSetConverter<_,'T> HashSet null (resolver.Resolve()) :> _ }

        | ShapeFSharpSet s ->
            s.Accept {
                new IFSharpSetVisitor<FieldConverter> with
                    member __.VisitFSharpSet<'T when 'T : comparison> () =
                        if typeof<'T> = typeof<byte []> then
                            BytesSetConverter<Set<byte []>>(Set.ofSeq, Set.empty) :> _
                        else
                            mkSetConverter<_,'T> Set.ofSeq Set.empty (resolver.Resolve()) :> _ }

        | ShapeDictionary s ->
            s.Accept {
                new IDictionaryVisitor<FieldConverter> with
                    member __.VisitDictionary<'K, 'V when 'K : equality> () =
                        if typeof<'K> <> typeof<string> then
                            UnSupportedField.Raise(t, "Dictionary types must have key of type string.")

                        new MapConverter<Dictionary<string, 'V>, 'V>(cdict, null, resolver.Resolve()) :> _ }

        | ShapeFSharpMap s ->
            s.Accept { 
                new IFSharpMapVisitor<FieldConverter> with
                    member __.VisitFSharpMap<'K, 'V when 'K : comparison> () =
                        if typeof<'K> <> typeof<string> then
                            UnSupportedField.Raise(t, "Map types must have key of type string.")

                        let mkMap (kvs : seq<KeyValuePair<string,'V>>) =
                            kvs |> Seq.map (fun kv -> kv.Key, kv.Value) |> Map.ofSeq

                        new MapConverter<Map<string,'V>, 'V>(mkMap, Map.empty, resolver.Resolve()) :> _ }

        | ShapeCollection s ->
            s.Accept {
                new ICollectionVisitor<FieldConverter> with
                    member __.VisitCollection<'T> () =
                        let tconv = resolver.Resolve<'T>()
                        new ListConverter<ICollection<'T>, 'T>(Seq.toArray >> unbox, null, tconv) :> _ }

        | ShapeEnumerable s ->
            s.Accept {
                new IEnumerableVisitor<FieldConverter> with
                    member __.VisitEnumerable<'T> () =
                        let tconv = resolver.Resolve<'T>()
                        new ListConverter<seq<'T>, 'T>(Seq.toArray >> unbox, null, tconv) :> _ }

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
        static let globalCache = new ConcurrentDictionary<Type, FieldConverter>()
        let stack = new Stack<Type>()
        let resolve t = 
            if stack.Contains t then
                UnSupportedField.Raise(t, "recursive types not supported.")
                
            stack.Push t
            let conv = globalCache.GetOrAdd(t, resolveFieldConverter self)
            let _ = stack.Pop()
            conv

        static member Create() = new CachedResolver() :> IFieldConverterResolver

        interface IFieldConverterResolver with
            member __.Resolve(t : Type) = resolve t
            member __.Resolve<'T> () = resolve typeof<'T> :?> FieldConverter<'T>

[<CompilationRepresentation(CompilationRepresentationFlags.ModuleSuffix)>]
module FieldConverter =

    let resolveUntyped (t : Type) = CachedResolver.Create().Resolve t
    let resolve<'T> () = CachedResolver.Create().Resolve<'T> ()