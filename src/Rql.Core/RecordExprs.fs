namespace Rql.Core

open System
open System.Collections.Generic
open System.Reflection

open Microsoft.FSharp.Quotations
open Microsoft.FSharp.Quotations.Patterns
open Microsoft.FSharp.Quotations.DerivedPatterns
open Microsoft.FSharp.Quotations.ExprShape

open Rql.Core

open Swensen.Unquote

module internal RecordExprs =

    let extractPartialRecordExpression (info : RecordInfo) (expr : Expr<'TRecord -> 'TRecord>) =
        match expr with
        | Lambda(x, body) when x.Type = typeof<'TRecord> ->
            let dict = new Dictionary<string, obj> ()
            let append (rp : RecordProperty) (body : Expr) = 
                if body.GetFreeVars() |> Seq.isEmpty |> not then
                    invalidArg "expr" "Supplied expression is not a valid record update operation."
                RecordBuilder.writeFieldData (fun o -> dict.Add(rp.Name, o)) rp (evalRaw body)

            let rec stripBindings (expr : Expr) =
                match expr with
                | Let(v, body, cont) -> 
                    let propInfo = info.Properties |> Array.find (fun p -> p.PropertyInfo.Name = v.Name)
                    append propInfo body 
                    stripBindings cont

                | NewRecord(_,assignments) -> assignments
                | _ -> invalidArg "expr" "Supplied expression is not a valid record update operation."

            let assignments = stripBindings body
            assignments |> List.iteri(fun i ass ->
                match ass with
                | Var v when dict.ContainsKey v.Name -> ()
                | PropertyGet(Some (Var y), _, _) when x = y -> ()
                | _ -> append info.Properties.[i] ass)

            dict :> RecordFields

        | _ -> invalidArg "expr" "Supplied expression is not a valid record update operation."

    let extractQueryExpr (info : RecordInfo) (expr : Expr<'TRecord -> bool>) =
        match expr with
        | Lambda(x, body) when x.Type = typeof<'TRecord> ->
            let rec mkQuery (expr : Expr) =
                match expr with
                | AndAlso(left, right) -> 
                    match mkQuery left, mkQuery right with
                    | False, _ -> False
                    | _, False -> False
                    | True, r -> r
                    | l, True -> l
                    | l, r -> And(l, r)

                | OrElse(left, right) -> 
                    match mkQuery left, mkQuery right with
                    | True, _ -> True
                    | _, True -> True
                    | False, r -> r
                    | l, False -> l
                    | l, r -> Or(mkQuery left, mkQuery right)

                | SpecificCall <@ not @> (None, _, [body]) -> 
                    match mkQuery body with
                    | False -> True
                    | True -> False
                    | Not q' -> q'
                    | q -> Not q

                | e when e.GetFreeVars() |> Seq.isEmpty -> if evalRaw e then True else False
                | Call(None, methodInfo, args) -> Predicate(methodInfo, List.map mkComparand args)
                | e -> 
                    match mkComparand e with
                    | Value o -> if o :?> bool then True else False
                    | Property p -> BooleanProperty p

            and mkComparand (expr : Expr) =
                match expr with
                | PropertyGet(Some(Var y), prop, []) when x = y ->
                    let rp = info.Properties |> Array.find (fun p -> p.PropertyInfo = prop)
                    Property rp
                | e when e.GetFreeVars() |> Seq.isEmpty |> not -> invalidArg "expr" "Supplied expression is not a valid query."
                | e -> Value(evalRaw e)

            mkQuery body

        | _ -> invalidArg "expr" "Supplied expression is not a valid query."