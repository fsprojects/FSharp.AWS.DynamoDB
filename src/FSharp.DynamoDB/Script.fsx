#I "../../bin"
#r "AWSSDK.DynamoDBv2.dll"
#r "FSharp.DynamoDB.dll"


open FSharp.DynamoDB.FieldConverter

let x = resolveConv<Map<int, bool>>()

let m = Map.ofList [for i in 1 .. 10 -> (i, i % 2 = 0)]

x.OfField m |> x.ToField