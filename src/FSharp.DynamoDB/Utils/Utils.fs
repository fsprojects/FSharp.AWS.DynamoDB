namespace FSharp.DynamoDB

open System
open System.Reflection
open System.Threading.Tasks

[<AutoOpen>]
module internal Utils =

    let inline rlist (ts : seq<'T>) = new ResizeArray<_>(ts)

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

    type Task with
        /// Gets the inner exception of the faulted task.
        member t.InnerException =
            let e = t.Exception
            if e.InnerExceptions.Count = 1 then e.InnerExceptions.[0]
            else
                e :> exn

    type Async with
        /// <summary>
        ///     Gets the result of given task so that in the event of exception
        ///     the actual user exception is raised as opposed to being wrapped
        ///     in a System.AggregateException.
        /// </summary>
        /// <param name="task">Task to be awaited.</param>
        static member AwaitTaskCorrect(task : Task<'T>) : Async<'T> =
            Async.FromContinuations(fun (sc,ec,cc) ->
                task.ContinueWith(fun (t : Task<'T>) -> 
                    if task.IsFaulted then ec t.InnerException 
                    elif task.IsCanceled then cc(new OperationCanceledException())
                    else sc t.Result)
                |> ignore)

        /// <summary>
        ///     Gets the result of given task so that in the event of exception
        ///     the actual user exception is raised as opposed to being wrapped
        ///     in a System.AggregateException.
        /// </summary>
        /// <param name="task">Task to be awaited.</param>
        static member AwaitTaskCorrect(task : Task) : Async<unit> =
            Async.FromContinuations(fun (sc,ec,cc) ->
                task.ContinueWith(fun (t : Task) -> 
                    if task.IsFaulted then ec t.InnerException 
                    elif task.IsCanceled then cc(new OperationCanceledException())
                    else sc ())
                |> ignore)