namespace Rql.Core

open System
open System.Reflection

[<AutoOpen>]
module internal Utils =

    /// taken from mscorlib's Tuple.GetHashCode() implementation
    let inline private combineHash (h1 : int) (h2 : int) =
        ((h1 <<< 5) + h1) ^^^ h2

    /// pair hashcode generation without tuple allocation
    let inline hash2 (t : 'T) (s : 'S) =
        combineHash (hash t) (hash s)
        
    /// triple hashcode generation without tuple allocation
    let inline hash3 (t : 'T) (s : 'S) (u : 'U) =
        combineHash (combineHash (hash t) (hash s)) (hash u)

    /// quadruple hashcode generation without tuple allocation
    let inline hash4 (t : 'T) (s : 'S) (u : 'U) (v : 'V) =
        combineHash (combineHash (combineHash (hash t) (hash s)) (hash u)) (hash v)

    let tryGetAttribute<'Attribute when 'Attribute :> System.Attribute> (attrs : seq<Attribute>) : 'Attribute option =
        attrs |> Seq.tryPick(function :? 'Attribute as a -> Some a | _ -> None)

    let getAttributes<'Attribute when 'Attribute :> System.Attribute> (attrs : seq<Attribute>) : 'Attribute [] =
        attrs |> Seq.choose(function :? 'Attribute as a -> Some a | _ -> None) |> Seq.toArray

    let containsAttribute<'Attribute when 'Attribute :> System.Attribute> (attrs : seq<Attribute>) : bool =
        attrs |> Seq.exists(fun a -> a :? 'Attribute)

    type MemberInfo with
        member m.TryGetAttribute<'Attribute when 'Attribute :> System.Attribute> () : 'Attribute option =
            m.GetCustomAttributes(true) |> Seq.map unbox<Attribute> |> tryGetAttribute

        member m.GetAttributes<'Attribute when 'Attribute :> System.Attribute> () : 'Attribute [] =
            m.GetCustomAttributes(true) |> Seq.map unbox<Attribute> |> getAttributes

        member m.ContainsAttribute<'Attribute when 'Attribute :> System.Attribute> () : bool =
            m.GetCustomAttributes(true) |> Seq.map unbox<Attribute> |> containsAttribute