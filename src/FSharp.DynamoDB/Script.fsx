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

type Nested = { A : string ; B : System.Reflection.BindingFlags }

type Union = A of int | B of string * int

type Test =
    {
        [<HashKey>]
        HashKey : Guid
        [<RangeKey>]
        RangeKey : string
        Value : int
        Unions : Union list
        String : string ref
        Value2 : int option
        Values : Nested []
        Date : DateTimeOffset
        Map : Map<string, int>
        Set : Set<int64> list
        Bytes : string[]
    }


let table = TableContext.Create<Test>(ddb, "test")
do table.CreateIfNotExists()

let value = { HashKey = Guid.NewGuid() ; RangeKey = "2" ; Value = 40 ; Date = DateTimeOffset.Now + TimeSpan.FromDays 2. ; Value2 = None ; Values = [|{ A = "foo" ; B = System.Reflection.BindingFlags.Instance }|] ; Map = Map.ofList [("A1",1)] ; Set = [set [1L];set [2L]] ; Bytes = [|"a";null|]; String = ref "1a" ; Unions = [A 42; B("42",3)]}

let key = table.PutItem value
table.GetItem key

table.Scan(<@ fun r -> r.Value2.Value < 42 @>)

table.Scan <@ fun t -> t.Date < DateTimeOffset.Now @>