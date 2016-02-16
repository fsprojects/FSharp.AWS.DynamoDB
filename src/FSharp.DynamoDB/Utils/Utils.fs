namespace FSharp.DynamoDB

open System
open System.Collections.Generic
open System.Reflection
open System.Threading.Tasks

open Microsoft.FSharp.Quotations
open Microsoft.FSharp.Quotations.Patterns
open Microsoft.FSharp.Quotations.DerivedPatterns
open Microsoft.FSharp.Quotations.ExprShape

[<AutoOpen>]
module internal Utils =

    let inline rlist (ts : seq<'T>) = new ResizeArray<_>(ts)

    let inline keyVal k v = new KeyValuePair<_,_>(k,v)

    let inline cdict (kvs : seq<KeyValuePair<'K,'V>>) = 
        let d = new Dictionary<'K, 'V>()
        for kv in kvs do d.Add(kv.Key, kv.Value)
        d

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

    type MethodInfo with
        /// Gets the underlying method definition
        /// including the supplied declaring type and method type arguments
        member m.GetUnderlyingMethodDefinition() : MethodInfo * Type[] * Type[] =
            let dt = m.DeclaringType
            if dt.IsGenericType then
                let gt = dt.GetGenericTypeDefinition()
                let gas = dt.GetGenericArguments()
                let mas = m.GetGenericArguments()

                let bindingFlags = 
                    BindingFlags.Public ||| BindingFlags.NonPublic ||| 
                    BindingFlags.Static ||| BindingFlags.Instance ||| 
                    BindingFlags.FlattenHierarchy

                let m = 
                    gt.GetMethods(bindingFlags) 
                    |> Array.find (fun m' -> m.Name = m'.Name && m.MetadataToken = m'.MetadataToken)

                m, gas, mas

            elif m.IsGenericMethod then
                let mas = m.GetGenericArguments()
                m.GetGenericMethodDefinition(), [||], mas

            else
                m, [||], [||]

    type PropertyInfo with
        member p.GetUnderlyingProperty() : PropertyInfo * Type[] =
            let dt = p.DeclaringType
            if dt.IsGenericType then
                let gt = dt.GetGenericTypeDefinition()
                let gas = dt.GetGenericArguments()

                let bindingFlags = 
                    BindingFlags.Public ||| BindingFlags.NonPublic ||| 
                    BindingFlags.Static ||| BindingFlags.Instance ||| 
                    BindingFlags.FlattenHierarchy

                let gp = gt.GetProperty(p.Name, bindingFlags)

                gp, gas
            else
                p, [||]

    type Quotations.Expr with
        member e.IsClosed = e.GetFreeVars() |> Seq.isEmpty
        member e.Substitute(v : Var, sub : Expr) =
            e.Substitute(fun w -> if v = w then Some sub else None)

    /// Variations of DerivedPatterns.SpecificCall which correctly
    /// recognizes methods of generic types
    /// See also https://github.com/fsharp/fsharp/issues/546
    let (|SpecificCall2|_|) (pattern : Expr) =
        match pattern with
        | Lambdas(_, Call(_,mI,_)) | Call(_,mI,_) ->
            let gm,_,_ = mI.GetUnderlyingMethodDefinition()

            fun (input:Expr) ->
                match input with
                | Call(obj, mI', args) ->
                    let gm',ta,ma = mI'.GetUnderlyingMethodDefinition()
                    if gm = gm' then Some(obj, Array.toList ta, Array.toList ma, args)
                    else None
                | _ -> None

        | _ -> invalidArg "pattern" "supplied pattern is not a method call"

    let (|SpecificProperty|_|) (pattern : Expr) =
        match pattern with
        | Lambdas(_, PropertyGet(_,pI,_)) | PropertyGet(_,pI,_) ->
            let gp,_ = pI.GetUnderlyingProperty()

            fun (input:Expr) ->
                match input with
                | PropertyGet(obj,pI',args) ->
                    let gp',ta = pI'.GetUnderlyingProperty()
                    if gp' = gp then Some(obj, Array.toList ta, args)
                    else None
                | _ -> None

        | _ -> invalidArg "pattern" "supplied pattern is not a property getter"

    let (|PipeLeft|_|) (e : Expr) =
        match e with
        | SpecificCall2 <@ (<|) @> (None, _, _, [func; arg]) ->
            let rec unwind (body : Expr) =
                match body with
                | Let(x, value, body) -> unwind(body.Substitute(x, value))
                | Lambda(v, body) -> Some <| body.Substitute(v, arg)
                | _ -> None

            unwind func
        | _ -> None

    let (|PipeRight|_|) (e : Expr) =
        match e with
        | SpecificCall2 <@ (|>) @> (None, _, _, [left ; right]) ->
            let rec unwind (body : Expr) =
                match body with
                | Let(x, value, body) -> unwind(body.Substitute(x, value))
                | Lambda(x, body) -> Some <| body.Substitute(x, left)
                | _ -> None

            unwind right

        | _ -> None


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