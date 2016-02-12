module internal FSharp.DynamoDB.TableOps

open System

open Amazon.DynamoDBv2
open Amazon.DynamoDBv2.Model

open FSharp.DynamoDB.FieldConverter
open FSharp.DynamoDB.Common

type TableKeySchema with
    static member OfKeyStructure(ks : KeyStructure) =
        let mkTableKeySchema h r = { HashKey = h ; RangeKey = r }
        let mkKeySchema (name : string) (conv : FieldConverter) =
            let keyType =
                match conv.Representation with
                | FieldRepresentation.String -> ScalarAttributeType.S
                | FieldRepresentation.Number -> ScalarAttributeType.N
                | FieldRepresentation.Bytes -> ScalarAttributeType.B
                | FieldRepresentation.Serializer ->
                    invalidArg name <| "DynamoDB Key attributes do not support serialization attributes."
                | _ -> invalidArg name <| sprintf "Unsupported type '%O' for DynamoDB Key attribute." conv.Type

            { AttributeName = name ; KeyType = keyType }

        match ks with
        | HashKeyOnly rp -> mkTableKeySchema (mkKeySchema rp.Name rp.Converter) None
        | Combined(hk, rk) -> mkTableKeySchema (mkKeySchema hk.Name hk.Converter) (Some (mkKeySchema rk.Name rk.Converter))
        | DefaultHashKey(name,_,conv,rk) -> mkTableKeySchema (mkKeySchema name conv) (Some (mkKeySchema rk.Name rk.Converter))

    static member OfTableDescription (td : TableDescription) =
        let mkKeySchema (kse : KeySchemaElement) =
            let ad = td.AttributeDefinitions |> Seq.find (fun ad -> ad.AttributeName = kse.AttributeName)
            { AttributeName = kse.AttributeName ; KeyType = ad.AttributeType }
            
        let hk = td.KeySchema |> Seq.find (fun ks -> ks.KeyType = KeyType.HASH) |> mkKeySchema
        let rk = td.KeySchema |> Seq.tryPick (fun ks -> if ks.KeyType = KeyType.RANGE then Some(mkKeySchema ks) else None)
        { HashKey = hk ; RangeKey = rk }

    member schema.CreateCreateTableRequest (tableName : string, provisionedThroughput : ProvisionedThroughput) =
        let ctr = new CreateTableRequest(TableName = tableName)
        let addKey kt (ks : KeySchema) =
            ctr.KeySchema.Add <| new KeySchemaElement(ks.AttributeName, kt)
            ctr.AttributeDefinitions.Add <| new AttributeDefinition(ks.AttributeName, ks.KeyType)

        addKey KeyType.HASH schema.HashKey
        schema.RangeKey |> Option.iter (fun rk -> addKey KeyType.RANGE rk)
        ctr.ProvisionedThroughput <- provisionedThroughput
        ctr