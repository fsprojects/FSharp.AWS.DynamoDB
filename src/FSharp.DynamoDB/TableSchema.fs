namespace FSharp.DynamoDB

open System

[<Sealed>]
type HashKeyAttribute() =
    inherit Attribute()

[<Sealed>]
type RangeKeyAttribute() =
    inherit Attribute()

type KeyAttributeType = 
    | String 
    | Bytes 
    | Number

type KeySchema = 
    { 
        AttributeName : string
        KeyType : KeyAttributeType 
    }

type TableKeySchema = 
    { 
        HashKey : KeySchema
        RangeKey : KeySchema option 
    }

module internal TableUtils =
    
    open Microsoft.FSharp.Reflection
    open Amazon.DynamoDBv2
    open Amazon.DynamoDBv2.Model
    open FSharp.Core

    let extractKeySchema (rfe : RecordFieldExtractor<'TRecord>) =
        let mkKeySchema (p : RecordProperty) =
            if Option.isSome p.Serializer then
                invalidArg p.Name "DynamoDB Key attributes do not support serialization attributes."

            let keyType =
                if p.UnderlyingType = typeof<string> then String
                elif p.UnderlyingType = typeof<byte []> then Bytes
                else 
                    match Type.GetTypeCode p.UnderlyingType with
                    | TypeCode.Byte
                    | TypeCode.Decimal
                    | TypeCode.Double
                    | TypeCode.Int16
                    | TypeCode.Int32
                    | TypeCode.Int64
                    | TypeCode.SByte
                    | TypeCode.Single
                    | TypeCode.UInt16
                    | TypeCode.UInt32
                    | TypeCode.UInt64 -> Number
                    | _ -> invalidArg p.Name "DynamoDB Key attribute must be of type string, number or byte[]."

            { AttributeName = p.Name ; KeyType = keyType }

        match rfe.Properties |> Array.filter (fun p -> p.ContainsAttribute<HashKeyAttribute>()) with
        | [|hashKey|] ->
            let hkey = mkKeySchema hashKey
            match rfe.Properties |> Array.filter (fun p -> p.ContainsAttribute<RangeKeyAttribute>()) with
            | [||] -> { HashKey = hkey ; RangeKey = None }
            | [|rangeKey|] -> let rkey = mkKeySchema rangeKey in { HashKey = hkey ; RangeKey = Some rkey }
            | _ -> invalidArg (string typeof<'TRecord>) "Found more than one record fields carrying the RangeKey attribute."

        | [||] -> invalidArg (string typeof<'TRecord>) "Found not record fields carrying the HashKey attribute."
        | _ -> invalidArg (string typeof<'TRecord>) "Found more than one record fields carrying the HashKey attribute."

    let ofTableDescription (td : TableDescription) =
        let mkKeySchema (kse : KeySchemaElement) =
            let ad = td.AttributeDefinitions |> Seq.find (fun ad -> ad.AttributeName = kse.AttributeName)
            let kt = 
                match ad.AttributeType with 
                | at when at = ScalarAttributeType.S -> String
                | at when at = ScalarAttributeType.B -> Bytes
                | at when at = ScalarAttributeType.N -> Number
                | at -> invalidOp <| sprintf "invalid attribute type %O" at

            { AttributeName = kse.AttributeName ; KeyType = kt }
            
        let hk = td.KeySchema |> Seq.find (fun ks -> ks.KeyType = KeyType.HASH) |> mkKeySchema
        let rk = td.KeySchema |> Seq.tryPick (fun ks -> if ks.KeyType = KeyType.RANGE then Some(mkKeySchema ks) else None)
        { HashKey = hk ; RangeKey = rk }

    let mkCreateTableRequest (schema : TableKeySchema) (tableName : string) (provisionedThroughput : ProvisionedThroughput) =
        let ctr = new CreateTableRequest(TableName = tableName)
        let addKey kt (ks : KeySchema) =
            let sat = 
                match ks.KeyType with
                | String -> ScalarAttributeType.S 
                | Bytes -> ScalarAttributeType.B
                | Number -> ScalarAttributeType.N

            ctr.KeySchema.Add <| new KeySchemaElement(ks.AttributeName, kt)
            ctr.AttributeDefinitions.Add <| new AttributeDefinition(ks.AttributeName, sat)

        addKey KeyType.HASH schema.HashKey
        schema.RangeKey |> Option.iter (fun rk -> addKey KeyType.RANGE rk)
        ctr.ProvisionedThroughput <- provisionedThroughput
        ctr

//    let mkPutItemRequest (rfe : RecordFieldExtractor<'TRecord>) (tableName : string) (record : 'TRecord) =
//        let pir = new PutItemRequest(TableName = tableName)
//        let writer = { 
//            new IFieldWriter with 
//                member __.WriteValue(propInfo : RecordProperty, value) = 
//                    let attr = new AttributeValue()
//                    match value with
//                    | null -> attr.NULL <- true
//                    | :? string as s -> attr.S <- s
//                    | :? bool as b -> attr.B <- b
//                    | :? int as i -> attr.N <- string 
//        }