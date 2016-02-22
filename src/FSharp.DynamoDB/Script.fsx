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

type Test =
    {
        [<HashKey>]
        HashKey : string
        [<RangeKey>]
        RangeKey : string
        Value : int
        String : string
        Value2 : int option
        Values : Nested []
        Map : Map<string, int>
        Set : Set<int64> list
        Bytes : string[]
    }


let table = TableContext.GetTableContext<Test>(ddb, "test", createIfNotExists = true) |> Async.RunSynchronously

let value = { HashKey = "1" ; RangeKey = "2" ; Value = 40 ; Value2 = None ; Values = [|{ A = "foo" ; B = System.Reflection.BindingFlags.Instance }|] ; Map = Map.ofList [("A1",1)] ; Set = [set [1L];set [2L]] ; Bytes = [|"a";null|]; String = "a"}

let key = table.PutItemAsync(value) |> Async.RunSynchronously

table.GetItemAsync key |> Async.RunSynchronously
table.PutItemAsync({ value with Value2 = None}, <@ fun r -> r.HashKey.StartsWith "1"@>) |> Async.RunSynchronously

<@ fun (r:Test) -> { r with Set = { r.Set with }}

table.UpdateItemAsync(key, <@ fun r -> { r with Set = r.Set.Re } @>) |> Async.RunSynchronously
open System.Collections.Generic
let up = new UpdateItemRequest()
up.TableName <- table.TableName
up.Key.Add("HashKey", AttributeValue("1"))
up.UpdateExpression <- "DELETE #foo[0] :val0"
up.ExpressionAttributeNames.Add("#foo", "Set")
up.ExpressionAttributeValues.Add(":val0", AttributeValue(NS = new ResizeArray<_>(["1"])))

ddb.UpdateItem(up)

open FSharp.DynamoDB.TypeShape

shapeof<System.Collections.Generic.ICollection<int>>

let dto = DateTimeOffset.Now
dto.ToString( "yyyy-MM-dd\THH:mm:ss.fffffff\Z")

{ value with Values.[0].A = "3" }


let mk (x : int list) = <@ fun r -> x.Length + r @>

mk [1 .. 1000]

ValueType

open System.IO

let x = ResizeArray<string>()
let y = ResizeArray<string>()

x.Add "1"
x.Add "1"
y.Add "1"

areEqualResizeArrays x y