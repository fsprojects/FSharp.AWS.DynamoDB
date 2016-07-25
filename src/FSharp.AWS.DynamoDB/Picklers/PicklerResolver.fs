namespace FSharp.AWS.DynamoDB

open System
open System.Collections
open System.Collections.Generic
open System.Collections.Concurrent
open System.IO
open System.Reflection

open Microsoft.FSharp.Reflection
open TypeShape

open FSharp.AWS.DynamoDB

//
//  Pickler resolution implementation
//

[<AutoOpen>]
module private ResolverImpl =

    let resolvePickler (resolver : IPicklerResolver) (t : Type) : Pickler =
        match getShape t with
        | Shape.Bool -> new BoolPickler() :> _
        | Shape.Byte -> mkNumericalPickler<byte> () :> _
        | Shape.SByte -> mkNumericalPickler<sbyte> () :> _
        | Shape.Int16 -> mkNumericalPickler<int16> () :> _
        | Shape.Int32 -> mkNumericalPickler<int32> () :> _
        | Shape.Int64 -> mkNumericalPickler<int64> () :> _
        | Shape.UInt16 -> mkNumericalPickler<uint16> () :> _
        | Shape.UInt32 -> mkNumericalPickler<uint32> () :> _
        | Shape.UInt64 -> mkNumericalPickler<uint64> () :> _
        | Shape.Single -> mkNumericalPickler<single> () :> _
        | Shape.Decimal -> mkNumericalPickler<decimal> () :> _
        | Shape.Double -> new DoublePickler() :> _
        | Shape.Char -> new CharPickler() :> _
        | Shape.String -> new StringPickler() :> _
        | Shape.Guid -> new GuidPickler() :> _
        | Shape.ByteArray -> new ByteArrayPickler() :> _
        | Shape.TimeSpan -> new TimeSpanPickler() :> _
        | Shape.DateTime -> UnSupportedType.Raise(t, "please use DateTimeOffset instead.")
        | Shape.DateTimeOffset -> new DateTimeOffsetPickler() :> _
        | :? TypeShape<System.IO.MemoryStream> -> new MemoryStreamPickler() :> _
        | Shape.Enum s ->
            s.Accept {
                new IEnumVisitor<Pickler> with
                    member __.Visit<'E, 'U when 'E : enum<'U>> () =
                        new EnumerationPickler<'E, 'U>() :> _ }

        | Shape.Nullable s ->
            s.Accept {
                new INullableVisitor<Pickler> with
                    member __.Visit<'T when 'T : (new : unit -> 'T) and 'T :> ValueType and 'T : struct> () = 
                        new NullablePickler<'T>(resolver.Resolve()) :> _ }

        | Shape.FSharpOption s ->
            s.Accept {
                new IFSharpOptionVisitor<Pickler> with
                    member __.Visit<'T> () =
                        let tp = resolver.Resolve<'T>()
                        new OptionPickler<'T>(tp) :> _ }

        | Shape.Array s ->
            s.Accept {
                new IArrayVisitor<Pickler> with
                    member __.Visit<'T> () =
                        let tp = resolver.Resolve<'T>()
                        new ListPickler<'T [], 'T>(Seq.toArray, null, tp) :> _ }

        | Shape.FSharpList s ->
            s.Accept {
                new IFSharpListVisitor<Pickler> with
                    member __.Visit<'T> () =
                        let tp = resolver.Resolve<'T>()
                        new ListPickler<'T list, 'T>(List.ofSeq, [], tp) :> _ }

        | Shape.ResizeArray s ->
            s.Accept {
                new IResizeArrayVisitor<Pickler> with
                    member __.Visit<'T> () =
                        let tp = resolver.Resolve<'T>()
                        new ListPickler<ResizeArray<'T>, 'T>(rlist, null, tp) :> _ }

        | Shape.FSharpSet s ->
            s.Accept {
                new IFSharpSetVisitor<Pickler> with
                    member __.Visit<'T when 'T : comparison> () =
                        mkSetPickler<'T>(resolver.Resolve()) :> _ }

        | Shape.FSharpMap s ->
            s.Accept { 
                new IFSharpMapVisitor<Pickler> with
                    member __.Visit<'K, 'V when 'K : comparison> () =
                        if typeof<'K> <> typeof<string> then
                            UnSupportedType.Raise(t, "Map types must have key of type string.")

                        new MapPickler<'V>(resolver.Resolve()) :> _ }

        | Shape.Collection s ->
            s.Accept {
                new ICollectionVisitor<Pickler> with
                    member __.Visit<'T> () =
                        let tp = resolver.Resolve<'T>()
                        new ListPickler<ICollection<'T>, 'T>(Seq.toArray >> unbox, null, tp) :> _ }

        | Shape.Enumerable s ->
            s.Accept {
                new IEnumerableVisitor<Pickler> with
                    member __.Visit<'T> () =
                        let tp = resolver.Resolve<'T>()
                        new ListPickler<seq<'T>, 'T>(Seq.toArray >> unbox, null, tp) :> _ }

        | Shape.Tuple _ as s ->
            s.Accept {
                new ITypeShapeVisitor<Pickler> with
                    member __.Visit<'T> () = mkTuplePickler<'T> resolver :> _ }

        | Shape.FSharpRecord _ as s ->
            s.Accept {
                new ITypeShapeVisitor<Pickler> with
                    member __.Visit<'T>() = mkFSharpRecordPickler<'T> resolver :> _   }

        | Shape.FSharpUnion _ as s ->
            s.Accept {
                new ITypeShapeVisitor<Pickler> with
                    member __.Visit<'T>() = new UnionPickler<'T>(resolver) :> _   }

        | _ -> UnSupportedType.Raise t

    type CachedResolver private () as self =
        static let globalCache = new ConcurrentDictionary<Type, Lazy<Pickler>>()
        let stack = new Stack<Type>()
        let resolve t = 
            if stack.Contains t then
                UnSupportedType.Raise(t, "recursive types not supported.")
                
            stack.Push t
            let pf = globalCache.GetOrAdd(t, fun t -> lazy(resolvePickler self t))
            let _ = stack.Pop()
            pf.Value

        interface IPicklerResolver with
            member __.Resolve(t : Type) = resolve t
            member __.Resolve<'T> () = resolve typeof<'T> :?> Pickler<'T>

        static member Resolve(t : Type) =
            let ok, found = globalCache.TryGetValue t
            if ok then found.Value
            else
                (new CachedResolver() :> IPicklerResolver).Resolve t

[<CompilationRepresentation(CompilationRepresentationFlags.ModuleSuffix)>]
module internal Pickler =

    /// Resolves pickler for given type
    let resolveUntyped (t : Type) = CachedResolver.Resolve t
    /// Resolves pickler for given type
    let resolve<'T> () = CachedResolver.Resolve typeof<'T> :?> Pickler<'T>