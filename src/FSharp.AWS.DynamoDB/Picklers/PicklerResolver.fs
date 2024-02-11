namespace FSharp.AWS.DynamoDB

open System
open System.Collections.Concurrent
open System.Collections.Generic

open TypeShape

//
//  Pickler resolution implementation
//

[<AutoOpen>]
module private ResolverImpl =

    let resolvePickler (resolver: IPicklerResolver) (t: Type) : Pickler =
        match TypeShape.Create t with
        | Shape.Bool -> BoolPickler() :> _
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
        | Shape.Double -> DoublePickler() :> _
        | Shape.Char -> CharPickler() :> _
        | Shape.String -> StringPickler() :> _
        | Shape.Guid -> GuidPickler() :> _
        | Shape.ByteArray -> ByteArrayPickler() :> _
        | Shape.TimeSpan -> TimeSpanPickler() :> _
        | Shape.DateTime -> UnSupportedType.Raise(t, "please use DateTimeOffset instead.")
        | Shape.DateTimeOffset -> DateTimeOffsetPickler() :> _
        | :? TypeShape<System.IO.MemoryStream> -> MemoryStreamPickler() :> _
        | Shape.Enum s ->
            s.Accept
                { new IEnumVisitor<Pickler> with
                    member _.Visit<'Enum, 'Underlying
                        when 'Enum: enum<'Underlying> and 'Enum: struct and 'Enum :> ValueType and 'Enum: (new: unit -> 'Enum)>
                        ()
                        =
                        new EnumerationPickler<'Enum, 'Underlying>() :> _ }

        | Shape.Nullable s ->
            s.Accept
                { new INullableVisitor<Pickler> with
                    member _.Visit<'T when 'T: (new: unit -> 'T) and 'T :> ValueType and 'T: struct>() =
                        new NullablePickler<'T>(resolver.Resolve()) :> _ }

        | Shape.FSharpOption s ->
            s.Element.Accept
                { new ITypeVisitor<Pickler> with
                    member _.Visit<'T>() =
                        let tp = resolver.Resolve<'T>()
                        new OptionPickler<'T>(tp) :> _ }

        | Shape.Array s when s.Rank = 1 ->
            s.Element.Accept
                { new ITypeVisitor<Pickler> with
                    member _.Visit<'T>() =
                        let tp = resolver.Resolve<'T>()
                        new ListPickler<'T[], 'T>(Seq.toArray, null, tp) :> _ }

        | Shape.FSharpList s ->
            s.Element.Accept
                { new ITypeVisitor<Pickler> with
                    member _.Visit<'T>() =
                        let tp = resolver.Resolve<'T>()
                        new ListPickler<'T list, 'T>(List.ofSeq, [], tp) :> _ }

        | Shape.ResizeArray s ->
            s.Element.Accept
                { new ITypeVisitor<Pickler> with
                    member _.Visit<'T>() =
                        let tp = resolver.Resolve<'T>()
                        new ListPickler<ResizeArray<'T>, 'T>(rlist, null, tp) :> _ }

        | Shape.FSharpSet s ->
            s.Accept
                { new IFSharpSetVisitor<Pickler> with
                    member _.Visit<'T when 'T: comparison>() = mkSetPickler<'T> (resolver.Resolve()) :> _ }

        | Shape.FSharpMap s ->
            s.Accept
                { new IFSharpMapVisitor<Pickler> with
                    member _.Visit<'K, 'V when 'K: comparison>() =
                        if typeof<'K> <> typeof<string> then
                            UnSupportedType.Raise(t, "Map types must have key of type string.")

                        new MapPickler<'V>(resolver.Resolve()) :> _ }

        | Shape.Tuple _ as s ->
            s.Accept
                { new ITypeVisitor<Pickler> with
                    member _.Visit<'T>() = mkTuplePickler<'T> resolver :> _ }

        | Shape.FSharpRecord _ as s ->
            s.Accept
                { new ITypeVisitor<Pickler> with
                    member _.Visit<'T>() = mkFSharpRecordPickler<'T> resolver :> _ }

        | Shape.FSharpUnion _ as s ->
            s.Accept
                { new ITypeVisitor<Pickler> with
                    member _.Visit<'T>() = new UnionPickler<'T>(resolver) :> _ }

        | _ -> UnSupportedType.Raise t

    type CachedResolver private () as self =
        static let globalCache = ConcurrentDictionary<Type, Lazy<Pickler>>()
        let stack = Stack<Type>()
        let resolve t =
            try
                if stack.Contains t then
                    UnSupportedType.Raise(t, "recursive types not supported.")

                stack.Push t
                let pf = globalCache.GetOrAdd(t, (fun t -> lazy (resolvePickler self t)))
                let _ = stack.Pop()
                pf.Value

            with UnsupportedShape t ->
                UnSupportedType.Raise t

        interface IPicklerResolver with
            member _.Resolve(t: Type) = resolve t
            member _.Resolve<'T>() = resolve typeof<'T> :?> Pickler<'T>

        static member Resolve(t: Type) =
            let ok, found = globalCache.TryGetValue t
            if ok then
                found.Value
            else
                (CachedResolver() :> IPicklerResolver).Resolve t

[<CompilationRepresentation(CompilationRepresentationFlags.ModuleSuffix)>]
module internal Pickler =

    /// Resolves pickler for given type
    let resolveUntyped (t: Type) = CachedResolver.Resolve t
    /// Resolves pickler for given type
    let resolve<'T> () = CachedResolver.Resolve typeof<'T> :?> Pickler<'T>
