module internal FSharp.DynamoDB.KeySchema

open System
open System.Collections.Generic

open Amazon.DynamoDBv2
open Amazon.DynamoDBv2.Model

//
//  Table key schema extractor methods for F# records
//

/// Describes the key structure of a given F# record
type KeyStructure =
    | HashKeyOnly of hashKeyProperty:RecordPropertyInfo
    | Combined of hashKeyProperty:RecordPropertyInfo * rangeKeyProperty:RecordPropertyInfo
    | DefaultHashKey of hkName:string * hkValue:obj * hkPickler:Pickler * rangeKeyProperty:RecordPropertyInfo
    | DefaultRangeKey of rkName:string * rkValue:obj * rkPickler:Pickler * hashKeyProperty:RecordPropertyInfo
with
    /// Extracts given TableKey to AttributeValue form
    static member ExtractKey(keyStructure : KeyStructure, key : TableKey) =
        let dict = new Dictionary<string, AttributeValue> ()
        let extractKey name (pickler : Pickler) (value:obj) =
            if isNull value then invalidArg name "Key value was not specified."
            let av = pickler.PickleUntyped value |> Option.get
            dict.Add(name, av)

        match keyStructure with
        | HashKeyOnly hkp -> extractKey hkp.Name hkp.Pickler key.HashKey
        | Combined(hkp,rkp) ->
            extractKey hkp.Name hkp.Pickler key.HashKey
            extractKey rkp.Name rkp.Pickler key.RangeKey
        | DefaultHashKey(name, value, pickler, rkp) ->
            if key.IsHashKeySpecified then
                extractKey name pickler key.HashKey
            else
                let av = value |> pickler.PickleUntyped |> Option.get
                dict.Add(name, av)

            extractKey rkp.Name rkp.Pickler key.RangeKey

        | DefaultRangeKey(name, value, pickler, hkp) ->
            extractKey hkp.Name hkp.Pickler key.HashKey

            if key.IsRangeKeySpecified then
                extractKey name pickler key.RangeKey
            else
                let av = value |> pickler.PickleUntyped |> Option.get
                dict.Add(name, av)


        dict

    /// Extract a KeyCondition for records that specify a default hashkey
    static member TryExtractHashKeyCondition keyStructure keySchema =
        match keyStructure with
        | DefaultHashKey(_, value, pickler, _) ->
            let av = pickler.PickleUntyped value |> Option.get
            let cond = ConditionalExpr.mkHashKeyEqualityCondition keySchema av
            let cexpr = new ConditionExpression<'TRecord>(cond)
            Some cexpr

        | _ -> None

    /// Extracts key from given record instance
    static member ExtractKey(keyStructure : KeyStructure, record : 'Record) =
        let inline getValue (rp : RecordPropertyInfo) = rp.PropertyInfo.GetValue(record)
        match keyStructure with
        | HashKeyOnly hkp -> let hashKey = getValue hkp in TableKey.Hash hashKey
        | DefaultHashKey(_, hashKey, _, rkp) ->
            let rangeKey = getValue rkp
            TableKey.Combined(hashKey, rangeKey)
        | DefaultRangeKey(_, rangeKey, _, hkp) ->
            let hashKey = getValue hkp
            TableKey.Combined(hashKey, rangeKey)
        | Combined(hkp,rkp) ->
            let hashKey = getValue hkp
            let rangeKey = getValue rkp
            TableKey.Combined(hashKey, rangeKey)

    /// Builds key structure from supplied F# record info
    static member FromRecordInfo (recordInfo : RecordInfo) =
        let hkcaOpt = recordInfo.Type.TryGetAttribute<ConstantHashKeyAttribute> ()
        let rkcaOpt = recordInfo.Type.TryGetAttribute<ConstantRangeKeyAttribute> ()

        if Option.isSome hkcaOpt && Option.isSome rkcaOpt then
            invalidArg (string recordInfo.Type) "Cannot specify both HashKey and RangeKey constant attributes in record definition."

        match recordInfo.Properties |> Array.filter (fun p -> p.IsHashKey) with
        | [|_|] when Option.isSome hkcaOpt ->
            invalidArg (string recordInfo.Type) "Cannot attach HashKey attribute to records containing HashKeyConstant attribute."

        | [|hashKeyP|] when Option.isSome rkcaOpt ->
            let rkca = Option.get rkcaOpt
            if not <| isValidFieldName rkca.Name then
                invalidArg rkca.Name "invalid rangekey name; must be alphanumeric and should not begin with a number."

            if recordInfo.Properties |> Array.exists(fun p -> p.Name = rkca.Name) then
                invalidArg (string recordInfo.Type) "Default RangeKey attribute contains conflicting name."

            if recordInfo.Properties |> Array.exists(fun p -> p.IsRangeKey) then
                invalidArg (string recordInfo.Type) "Cannot attach RangeKey attribute to records containing RangeKeyConstant attribute."

            let pickler = Pickler.resolveUntyped rkca.HashKeyType
            DefaultRangeKey(rkca.Name, rkca.RangeKey, pickler, hashKeyP)

        | [|hashKeyP|] ->
            match recordInfo.Properties |> Array.filter (fun p -> p.IsRangeKey) with
            | [||] -> HashKeyOnly hashKeyP
            | [|rangeKeyP|] -> Combined(hashKeyP, rangeKeyP)
            | _ -> invalidArg (string recordInfo.Type) "Found more than one record fields carrying the RangeKey attribute."

        | [||] when Option.isSome hkcaOpt ->
            match recordInfo.Properties |> Array.filter (fun p -> p.IsRangeKey) with
            | [||] -> invalidArg (string recordInfo.Type) "Records carrying the RangeKeyConstant attribute must specify a RangeKey property."
            | [|rangeKeyP|] -> 
                let hkca = Option.get hkcaOpt
                if not <| isValidFieldName hkca.Name then
                    invalidArg hkca.Name "invalid hashkey name; must be alphanumeric and should not begin with a number."

                if recordInfo.Properties |> Array.exists(fun p -> p.Name = hkca.Name) then
                    invalidArg (string recordInfo.Type) "Default HashKey attribute contains conflicting name."

                let pickler = Pickler.resolveUntyped hkca.HashKeyType
                DefaultHashKey(hkca.Name, hkca.HashKey, pickler, rangeKeyP)

            | _ -> invalidArg (string recordInfo.Type) "Found more than one record fields carrying the RangeKey attribute."

        | [||] -> invalidArg (string recordInfo.Type) "Found no record fields carrying the HashKey attribute."
        | _ -> invalidArg (string recordInfo.Type) "Found more than one record fields carrying the HashKey attribute."


type TableKeySchema with
    /// Extracts table key schema record from key structure type
    static member OfKeyStructure(ks : KeyStructure) =
        let mkTableKeySchema h r = { HashKey = h ; RangeKey = r }
        let mkKeySchema (name : string) (pickler : Pickler) =
            if pickler.PicklerType <> PicklerType.Value then
                invalidArg name <| "DynamoDB Key attributes do not support serialization attributes."

            let keyType =
                match pickler.PickleType with
                | PickleType.String -> ScalarAttributeType.S
                | PickleType.Number -> ScalarAttributeType.N
                | PickleType.Bytes -> ScalarAttributeType.B
                | _ -> invalidArg name <| sprintf "Unsupported type '%O' for DynamoDB Key attribute." pickler.Type

            { AttributeName = name ; KeyType = keyType }

        match ks with
        | HashKeyOnly rp -> mkTableKeySchema (mkKeySchema rp.Name rp.Pickler) None
        | Combined(hk, rk) -> mkTableKeySchema (mkKeySchema hk.Name hk.Pickler) (Some (mkKeySchema rk.Name rk.Pickler))
        | DefaultHashKey(name,_,pickler,rk) -> mkTableKeySchema (mkKeySchema name pickler) (Some (mkKeySchema rk.Name rk.Pickler))
        | DefaultRangeKey(name,_,pickler,hk) -> mkTableKeySchema (mkKeySchema hk.Name hk.Pickler) (Some (mkKeySchema name pickler))

    /// Extract key schema from DynamoDB table description object
    static member OfTableDescription (td : TableDescription) =
        let mkKeySchema (kse : KeySchemaElement) =
            let ad = td.AttributeDefinitions |> Seq.find (fun ad -> ad.AttributeName = kse.AttributeName)
            { AttributeName = kse.AttributeName ; KeyType = ad.AttributeType }
            
        let hk = td.KeySchema |> Seq.find (fun ks -> ks.KeyType = KeyType.HASH) |> mkKeySchema
        let rk = td.KeySchema |> Seq.tryPick (fun ks -> if ks.KeyType = KeyType.RANGE then Some(mkKeySchema ks) else None)
        { HashKey = hk ; RangeKey = rk }

    /// Create a CreateTableRequest using supplied key schema
    member schema.CreateCreateTableRequest (tableName : string, provisionedThroughput : ProvisionedThroughput) =
        let ctr = new CreateTableRequest(TableName = tableName)
        let addKey kt (ks : KeyAttributeSchema) =
            ctr.KeySchema.Add <| new KeySchemaElement(ks.AttributeName, kt)
            ctr.AttributeDefinitions.Add <| new AttributeDefinition(ks.AttributeName, ks.KeyType)

        addKey KeyType.HASH schema.HashKey
        schema.RangeKey |> Option.iter (fun rk -> addKey KeyType.RANGE rk)
        ctr.ProvisionedThroughput <- provisionedThroughput
        ctr