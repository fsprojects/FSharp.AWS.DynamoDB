namespace Rql.Core

open System
open System.Collections.Generic
open System.Reflection

[<AbstractClass>]
type TypeShape internal () =
    abstract Type : Type
//    abstract UnderlyingType : Type
    abstract Accept : ITypeShapeVisitor<'R> -> 'R
    override __.ToString() = sprintf "%O" (__.GetType())

and [<AbstractClass>] TypeShape<'T> () =
    inherit TypeShape()
    override __.Type = typeof<'T>

and ShapeType<'T> () =
    inherit TypeShape<'T> ()
//    override __.UnderlyingType = typeof<'T>
    override __.Accept (v : ITypeShapeVisitor<'R>) = v.VisitType<'T> ()

and ShapeNullable<'T when 'T : (new : unit -> 'T) and 'T :> ValueType and 'T : struct> () =
    inherit TypeShape<Nullable<'T>> ()
//    override __.UnderlyingType = typeof<'T>
    override __.Accept (v : ITypeShapeVisitor<'R>) = v.VisitNullable<'T> ()

and ShapeFSharpOption<'T> () =
    inherit TypeShape<'T option> ()
//    override __.UnderlyingType = typeof<'T>
    override __.Accept (v : ITypeShapeVisitor<'R>) = v.VisitFSharpOption<'T> ()

and ShapeFSharpRef<'T> () =
    inherit TypeShape<'T> ()
//    override __.UnderlyingType = typeof<'T>
    override __.Accept (v : ITypeShapeVisitor<'R>) = v.VisitFSharpRef<'T> ()

and ShapeFSharpList<'T> () =
    inherit TypeShape<'T list> ()
//    override __.UnderlyingType = typeof<'T []>
    override __.Accept (v : ITypeShapeVisitor<'R>) = v.VisitFSharpList<'T> ()

and ShapeFSharpSet<'T when 'T : comparison> () =
    inherit TypeShape<Set<'T>> ()
//    override __.UnderlyingType = typeof<'T []>
    override __.Accept (v : ITypeShapeVisitor<'R>) = v.VisitFSharpSet<'T> ()

and ShapeFSharpMap<'K ,'V when 'K : comparison> () =
    inherit TypeShape<Map<'K, 'V>> ()
//    override __.UnderlyingType = typeof<KeyValuePair<'K, 'V> []>
    override __.Accept (v : ITypeShapeVisitor<'R>) = v.VisitFSharpMap<'K, 'V> ()

and ITypeShapeVisitor<'R> =
    abstract VisitType<'T> : unit -> 'R
    abstract VisitNullable<'T when 'T : (new : unit -> 'T) and 'T :> ValueType and 'T : struct> : unit -> 'R
    abstract VisitFSharpOption<'T> : unit -> 'R
    abstract VisitFSharpRef<'T> : unit -> 'R
    abstract VisitFSharpList<'T> : unit -> 'R
    abstract VisitFSharpSet<'T when 'T : comparison> : unit -> 'R
    abstract VisitFSharpMap<'K, 'V when 'K : comparison> : unit -> 'R

[<CompilationRepresentation(CompilationRepresentationFlags.ModuleSuffix)>]
module TypeShape =
    
    let infer (t : Type) =
        let mkType (gt : Type) ga = let gi = gt.MakeGenericType(ga) in Activator.CreateInstance(gi) :?> TypeShape
        match (if t.IsGenericType then Some(t.GetGenericTypeDefinition(), t.GetGenericArguments()) else None) with
        | Some (gt, ga) when gt = typedefof<Nullable<_>> -> mkType typedefof<ShapeNullable<_>> ga
        | Some (gt, ga) when gt = typedefof<_ option> -> mkType typedefof<ShapeFSharpOption<_>> ga
        | Some (gt, ga) when gt = typedefof<_ ref> -> mkType typedefof<ShapeFSharpRef<_>> ga
        | Some (gt, ga) when gt = typedefof<_ list> -> mkType typedefof<ShapeFSharpList<_>> ga
        | Some (gt, ga) when gt = typedefof<Set<_>> -> mkType typedefof<ShapeFSharpSet<_>> ga
        | Some (gt, ga) when gt = typedefof<Map<_,_>> -> mkType typedefof<ShapeFSharpMap<_,_>> ga
        | _ -> mkType typedefof<ShapeType<_>> [|t|]