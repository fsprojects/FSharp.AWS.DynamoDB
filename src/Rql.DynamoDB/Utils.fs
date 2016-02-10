namespace Rql.DynamoDB

open System
open System.Threading.Tasks

[<AutoOpen>]
module internal Utils =

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