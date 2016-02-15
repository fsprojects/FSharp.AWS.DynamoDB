#I "../../bin"
#r "AWSSDK.Core.dll"
#r "AWSSDK.DynamoDBv2.dll"
#r "FSharp.DynamoDB.dll"

open System

open Amazon
open Amazon.Util
open Amazon.DynamoDBv2
open Amazon.DynamoDBv2.Model

open FSharp.DynamoDB

let account = AWSCredentialsProfile.LoadFrom("default").Credentials
let ddb = new AmazonDynamoDBClient(account, RegionEndpoint.EUCentral1) :> IAmazonDynamoDB

type Test =
    {
        [<HashKey>]
        HashKey : string
        Value : bool
        Values : Map<int, string>
    }

let table = TableContext.GetTableContext<Test>(ddb, "test", createIfNotExists = true) |> Async.RunSynchronously

let value = { HashKey = "1" ; Value = false ; Values = Map.empty }

let key = table.PutItemAsync(value) |> Async.RunSynchronously

table.PutItemAsync({ value with Value = true}, <@ fun r -> r.Value |> not @>) |> Async.RunSynchronously


open Microsoft.FSharp.Quotations
open Microsoft.FSharp.Quotations.Patterns
open FSharp.Quotations.DerivedPatterns
open Microsoft.FSharp.Quotations.ExprShape

let m = (|SpecificCall|_|) <@ fun x y -> x + y @> <@ "a" + "b" @>


let s = set [1]
let m' = (|SpecificCall|_|) <@ fun (s : Set<_>) e -> s.Contains e @> <@ s.Contains 0 @>
//val m' : (Expr option * Type list * Expr list) option = None

let r = Unchecked.defaultof<Map<int,int>>
let m' = (|SpecificCall|_|) <@ fun (m : Map<_,_>) e -> m.ContainsKey e @> <@ r.ContainsKey 0 @>

let foo (x : Expr) = x

(|SpecificCall|_|) 

(|Lambdas|_|)
let (Lambda(_,Lambda(_,Call(None,p,_)))) = foo <@ (|>) @>