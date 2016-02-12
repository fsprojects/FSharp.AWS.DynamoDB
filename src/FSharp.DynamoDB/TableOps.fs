module internal FSharp.DynamoDB.TableOps

open System

open Amazon.DynamoDBv2
open Amazon.DynamoDBv2.Model

open FSharp.DynamoDB.FieldConverter
open FSharp.DynamoDB.Common

let extractKeySchema (recordType : Type) (properties : RecordProperty[]) =
    let mkKeySchema (p : RecordProperty) =
        let keyType =
            match p.Converter.Representation with
            | FieldRepresentation.String -> ScalarAttributeType.S
            | FieldRepresentation.Number -> ScalarAttributeType.N
            | FieldRepresentation.Bytes -> ScalarAttributeType.B
            | FieldRepresentation.Serializer -> invalidArg p.Name <| "DynamoDB Key attributes do not support serialization attributes."
            | _ -> invalidArg p.Name <| sprintf "Unsupported type '%O' for DynamoDB Key attribute." p.Converter.Type

        { AttributeName = p.Name ; KeyType = keyType }

    match properties |> Array.filter (fun p -> p.ContainsAttribute<HashKeyAttribute>()) with
    | [|hashKeyP|] ->
        let hkey = mkKeySchema hashKeyP
        match properties |> Array.filter (fun p -> p.ContainsAttribute<RangeKeyAttribute>()) with
        | [||] -> hashKeyP, None, { HashKey = hkey ; RangeKey = None }
        | [|rangeKeyP|] -> let rkey = mkKeySchema rangeKeyP in hashKeyP, Some rangeKeyP, { HashKey = hkey ; RangeKey = Some rkey }
        | _ -> invalidArg (string recordType) "Found more than one record fields carrying the RangeKey attribute."

    | [||] -> invalidArg (string recordType) "Found no record fields carrying the HashKey attribute."
    | _ -> invalidArg (string recordType) "Found more than one record fields carrying the HashKey attribute."

let ofTableDescription (td : TableDescription) =
    let mkKeySchema (kse : KeySchemaElement) =
        let ad = td.AttributeDefinitions |> Seq.find (fun ad -> ad.AttributeName = kse.AttributeName)
        { AttributeName = kse.AttributeName ; KeyType = ad.AttributeType }
            
    let hk = td.KeySchema |> Seq.find (fun ks -> ks.KeyType = KeyType.HASH) |> mkKeySchema
    let rk = td.KeySchema |> Seq.tryPick (fun ks -> if ks.KeyType = KeyType.RANGE then Some(mkKeySchema ks) else None)
    { HashKey = hk ; RangeKey = rk }

let mkCreateTableRequest (schema : TableKeySchema) (tableName : string) (provisionedThroughput : ProvisionedThroughput) =
    let ctr = new CreateTableRequest(TableName = tableName)
    let addKey kt (ks : KeySchema) =
        ctr.KeySchema.Add <| new KeySchemaElement(ks.AttributeName, kt)
        ctr.AttributeDefinitions.Add <| new AttributeDefinition(ks.AttributeName, ks.KeyType)

    addKey KeyType.HASH schema.HashKey
    schema.RangeKey |> Option.iter (fun rk -> addKey KeyType.RANGE rk)
    ctr.ProvisionedThroughput <- provisionedThroughput
    ctr