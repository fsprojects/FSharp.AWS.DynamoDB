module internal FSharp.AWS.DynamoDB.KeySchema

open System
open System.Collections.Generic

open Amazon.DynamoDBv2
open Amazon.DynamoDBv2.Model

open FSharp.AWS.DynamoDB

//
//  Table key schema extractor methods for F# records
//

type RecordPropertySchema =
    {
        Metadata : PropertyMetadata
        Schemata : (bool * TableKeySchema) []
    }

//type RecordSchema =
//    {
//        Properties : RecordPropertySchema []
//    }

/// Describes the key structure of a given F# record
type PrimaryKeyStructure =
    | HashKeyOnly of hashKeyProperty:PropertyMetadata
    | Combined of hashKeyProperty:PropertyMetadata * rangeKeyProperty:PropertyMetadata
    | DefaultHashKey of hkName:string * hkValue:obj * hkPickler:Pickler * rangeKeyProperty:PropertyMetadata
    | DefaultRangeKey of rkName:string * rkValue:obj * rkPickler:Pickler * hashKeyProperty:PropertyMetadata
//with
//    /// Builds key structure from supplied F# record info
//    static member FromRecordInfo (recordInfo : RecordInfo) =
//        let hkcaOpt = recordInfo.Type.TryGetAttribute<ConstantHashKeyAttribute> ()
//        let rkcaOpt = recordInfo.Type.TryGetAttribute<ConstantRangeKeyAttribute> ()
//
//        if Option.isSome hkcaOpt && Option.isSome rkcaOpt then
//            "Cannot specify both HashKey and RangeKey constant attributes in record definition."
//            |> invalidArg (string recordInfo.Type)
//
//        match recordInfo.Properties |> Array.filter (fun p -> p.ContainsAttribute<HashKeyAttribute>()) with
//        | [|_|] when Option.isSome hkcaOpt ->
//            invalidArg (string recordInfo.Type) "Cannot attach HashKey attribute to records containing HashKeyConstant attribute."
//
//        | [|hashKeyP|] when Option.isSome rkcaOpt ->
//            let rkca = Option.get rkcaOpt
//            if not <| isValidFieldName rkca.Name then
//                invalidArg rkca.Name "invalid rangekey name; must be alphanumeric and should not begin with a number."
//
//            if recordInfo.Properties |> Array.exists(fun p -> p.Name = rkca.Name) then
//                invalidArg (string recordInfo.Type) "Default RangeKey attribute contains conflicting name."
//
//            if recordInfo.Properties |> Array.exists(fun p -> p.ContainsAttribute<RangeKeyAttribute>()) then
//                invalidArg (string recordInfo.Type) "Cannot attach RangeKey attribute to records containing RangeKeyConstant attribute."
//
//            let pickler = Pickler.resolveUntyped rkca.HashKeyType
//            DefaultRangeKey(rkca.Name, rkca.RangeKey, pickler, hashKeyP)
//
//        | [|hashKeyP|] ->
//            match recordInfo.Properties |> Array.filter (fun p -> p.ContainsAttribute<RangeKeyAttribute>()) with
//            | [||] -> HashKeyOnly hashKeyP
//            | [|rangeKeyP|] -> Combined(hashKeyP, rangeKeyP)
//            | _ -> invalidArg (string recordInfo.Type) "Found more than one record fields carrying the RangeKey attribute."
//
//        | [||] when Option.isSome hkcaOpt ->
//            match recordInfo.Properties |> Array.filter (fun p -> p.ContainsAttribute<RangeKeyAttribute>()) with
//            | [||] -> invalidArg (string recordInfo.Type) "Records carrying the RangeKeyConstant attribute must specify a RangeKey property."
//            | [|rangeKeyP|] -> 
//                let hkca = Option.get hkcaOpt
//                if not <| isValidFieldName hkca.Name then
//                    invalidArg hkca.Name "invalid hashkey name; must be alphanumeric and should not begin with a number."
//
//                if recordInfo.Properties |> Array.exists(fun p -> p.Name = hkca.Name) then
//                    invalidArg (string recordInfo.Type) "Default HashKey attribute contains conflicting name."
//
//                let pickler = Pickler.resolveUntyped hkca.HashKeyType
//                DefaultHashKey(hkca.Name, hkca.HashKey, pickler, rangeKeyP)
//
//            | _ -> invalidArg (string recordInfo.Type) "Found more than one record fields carrying the RangeKey attribute."
//
//        | [||] -> invalidArg (string recordInfo.Type) "Found no record fields carrying the HashKey attribute."
//        | _ -> invalidArg (string recordInfo.Type) "Found more than one record fields carrying the HashKey attribute."

    /// Extracts given TableKey to AttributeValue form
    static member ExtractKey(keyStructure : PrimaryKeyStructure, key : TableKey) =
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

//    /// Extract a KeyCondition for records that specify a default hashkey
//    static member TryExtractHashKeyCondition keyStructure keySchema =
//        match keyStructure with
//        | DefaultHashKey(_, value, pickler, _) ->
//            let av = pickler.PickleUntyped value |> Option.get
//            let cond = ConditionalExpr.mkHashKeyEqualityCondition keySchema av
//            let cexpr = new ConditionExpression<'TRecord>(cond)
//            Some cexpr
//
//        | _ -> None

    /// Extracts key from given record instance
    static member ExtractKey(keyStructure : PrimaryKeyStructure, record : 'Record) =
        let inline getValue (rp : PropertyMetadata) = rp.PropertyInfo.GetValue(record)
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

type KeyAttributeSchema with
    static member Create (name : string, pickler : Pickler) =
        if pickler.PicklerType <> PicklerType.Value then
            invalidArg name <| "DynamoDB Key attributes do not support serialization attributes."

        let keyType =
            match pickler.PickleType with
            | PickleType.String -> ScalarAttributeType.S
            | PickleType.Number -> ScalarAttributeType.N
            | PickleType.Bytes -> ScalarAttributeType.B
            | _ -> invalidArg name <| sprintf "Unsupported type '%O' for DynamoDB Key attribute." pickler.Type

        { AttributeName = name ; KeyType = keyType }

    static member Create(prop : PropertyMetadata) = KeyAttributeSchema.Create(prop.Name, prop.Pickler)

type TableKeySchema with
    static member OfKeyStructure(ks : PrimaryKeyStructure) : TableKeySchema =
        let inline mkKeySchema (name : string) (pickler : Pickler) = KeyAttributeSchema.Create(name, pickler)
        let inline mkPropSchema (rp : PropertyMetadata) = KeyAttributeSchema.Create(rp)
        let inline mkTableKeySchema h r = { HashKey = h ; RangeKey = r ; Type = PrimaryKey }

        match ks with
        | HashKeyOnly rp -> mkTableKeySchema (mkPropSchema rp) None
        | Combined(hk, rk) -> mkTableKeySchema (mkPropSchema hk) (Some (mkPropSchema rk))
        | DefaultHashKey(name,_,pickler,rk) -> mkTableKeySchema (mkKeySchema name pickler) (Some (mkPropSchema rk))
        | DefaultRangeKey(name,_,pickler,hk) -> mkTableKeySchema (mkPropSchema hk) (Some (mkKeySchema name pickler))

type KeyStructure =
    {
        PrimaryKey : PrimaryKeyStructure
        GlobalSecondaryIndices : TableKeySchema []
        LocalSecondaryIndices : TableKeySchema []
        Properties : RecordPropertySchema[]
    }
with
    /// Builds key structure from supplied F# record info
    static member FromRecordInfo (recordInfo : RecordInfo) =
        let hkcaOpt = recordInfo.Type.TryGetAttribute<ConstantHashKeyAttribute> ()
        let rkcaOpt = recordInfo.Type.TryGetAttribute<ConstantRangeKeyAttribute> ()
        let primaryKeyStructure = ref None
        let mkKAS rp = KeyAttributeSchema.Create rp

        let extractKeyType (rp : PropertyMetadata) (attr : Attribute) =
            match attr with
            | :? RangeKeyAttribute -> Some(rp, true, PrimaryKey)
            | :? HashKeyAttribute -> Some(rp, false, PrimaryKey)
            | :? SecondaryHashKeyAttribute as hk -> Some(rp, true, GlobalSecondaryIndex hk.IndexName)
            | :? SecondaryRangeKeyAttribute as rk -> Some(rp, false, GlobalSecondaryIndex rk.IndexName)
            | :? LocalSecondaryIndexAttribute as lsi ->
                let name = defaultArg lsi.IndexName (rp.Name + "Index")
                Some(rp, false, LocalSecondaryIndex name)
            | _ -> None

        let extractKeySchema (kst : KeySchemaType) (attrs : (bool * PropertyMetadata) []) =
            match kst, attrs with
            | PrimaryKey, _ ->
                let setResult (pks : PrimaryKeyStructure) = 
                    structure := Some pks ; TableKeySchema.OfKeyStructure pks

                match hkcaOpt, rkcaOpt, sortedAttrs with
                | Some _, Some _, _ ->
                    "Cannot specify both HashKey and RangeKey constant attributes in record definition."
                    |> invalidArg (string recordInfo.Type)

                | Some hkca, None, [|(false, rk)|] -> 
                    if not <| isValidFieldName hkca.Name then
                        invalidArg hkca.Name "invalid hashkey name; must be alphanumeric and should not begin with a number."

                    if recordInfo.Properties |> Array.exists(fun p -> p.Name = hkca.Name) then
                        invalidArg (string recordInfo.Type) "Default HashKey attribute contains conflicting name."

                    let pickler = Pickler.resolveUntyped hkca.HashKeyType
                    DefaultHashKey(hkca.Name, hkca.HashKey, pickler, rangeKeyP) |> setResult

                | None, Some rkca, [|(true, hk)|] ->
                    if not <| isValidFieldName rkca.Name then
                        invalidArg rkca.Name "invalid rangekey name; must be alphanumeric and should not begin with a number."

                    if recordInfo.Properties |> Array.exists(fun p -> p.Name = rkca.Name) then
                        invalidArg (string recordInfo.Type) "Default RangeKey attribute contains conflicting name."

                    let pickler = Pickler.resolveUntyped rkca.HashKeyType
                    DefaultRangeKey(rkca.Name, rkca.RangeKey, pickler, hk) |> setResult

                | None, None, [|(true, hk)|] -> HashKeyOnly(hk) |> setResult
                | None, None, [|(true, hk), (false, rk)|] -> Combined(hk, rk) |> setResult
                | _ -> invalidArg (string recordInfo.Type) "Invalid combination of HashKey and RangeKey attributes."

            | LocalSecondaryIndex _, [|(false, rk)|] ->
                match !primaryKeyStructure with
                | None -> "Does not specify a HashKey attribute." |> invalidArg (string recordInfo.Type)
                | Some pks -> { TableKeySchema.OfKeyStructure pks with RangeKey = Some rk ; Type = ty }

            | GlobalSecondaryIndex _, [|(true, hk)|] -> { HashKey = mkKAS hk ; RangeKey = None ; Type = ty }
            | GlobalSecondaryIndex _, [|(true, hk) ; (false, rk)|] -> 
                { HashKey = mkKAS hk ; RangeKey = Some (mkKAS rk); Type = ty }
            | GlobalSecondaryIndex id, _ ->
                sprintf "Invalid combination of SecondaryHashKey and SecondaryRangeKey attributes for index name '%s'." id
                |> invalidArg (string recordInfo.Type)

        let attributes =
            recordInfo.Properties
            |> Seq.collect (fun rp -> rp.Attributes |> Seq.choose (extractKeyType rp))
            |> Seq.distinct
            |> Seq.groupBy (fun (_,_,ty) -> ty)
            |> Seq.sort (fun (ty,_) -> match ty with PrimaryKey -> 0 | _ -> 1)
            |> Seq.map (fun (ty, attributes) ->
                let sortedAttrs = 
                    attributes
                    |> Seq.distinctBy (fun (rp,_,_) -> rp)
                    |> Seq.map (fun (rp,isHashKey,_) -> isHashKey, rp)
                    |> Seq.sortBy (fun (isHashKey,_) -> not isHashKey)
                    |> Seq.toArray
                    
                let schema = extractKeySchema ty sortedAttrs
                schema, sortedAttrs)
            |> Seq.map (fun (schema, attrs) ->
                
            
            
            )
//
//
//            )


//        let hkcaOpt = recordInfo.Type.TryGetAttribute<ConstantHashKeyAttribute> ()
//        let rkcaOpt = recordInfo.Type.TryGetAttribute<ConstantRangeKeyAttribute> ()
//
//        if Option.isSome hkcaOpt && Option.isSome rkcaOpt then
//            "Cannot specify both HashKey and RangeKey constant attributes in record definition."
//            |> invalidArg (string recordInfo.Type)
//
//        let localSecondaryIndices =
//            recordInfo.Properties |> Array.choose(fun rp ->
//                match rp.AttributeType with
//                | LocalSecondaryIndex name ->
//                    Some { IndexName = name ; LocalSecondaryRangeKey = KeyAttributeSchema.Create rp }
//                | _ -> None)
//
//        let primaryKey =
//            match recordInfo.Properties |> Array.filter (fun p -> p.IsHashKey) with
//            | [|_|] when Option.isSome hkcaOpt ->
//                invalidArg (string recordInfo.Type) "Cannot attach HashKey attribute to records containing HashKeyConstant attribute."
//
//            | [|hashKeyP|] when Option.isSome rkcaOpt ->
//                let rkca = Option.get rkcaOpt
//                if not <| isValidFieldName rkca.Name then
//                    invalidArg rkca.Name "invalid rangekey name; must be alphanumeric and should not begin with a number."
//
//                if recordInfo.Properties |> Array.exists(fun p -> p.Name = rkca.Name) then
//                    invalidArg (string recordInfo.Type) "Default RangeKey attribute contains conflicting name."
//
//                if recordInfo.Properties |> Array.exists(fun p -> p.IsRangeKey) then
//                    invalidArg (string recordInfo.Type) "Cannot attach RangeKey attribute to records containing RangeKeyConstant attribute."
//
//                let pickler = Pickler.resolveUntyped rkca.HashKeyType
//                DefaultRangeKey(rkca.Name, rkca.RangeKey, pickler, hashKeyP)
//
//            | [|hashKeyP|] ->
//                match recordInfo.Properties |> Array.filter (fun p -> p.IsRangeKey) with
//                | [||] -> HashKeyOnly hashKeyP
//                | [|rangeKeyP|] -> Combined(hashKeyP, rangeKeyP)
//                | _ -> invalidArg (string recordInfo.Type) "Found more than one record fields carrying the RangeKey attribute."
//
//            | [||] when Option.isSome hkcaOpt ->
//                match recordInfo.Properties |> Array.filter (fun p -> p.IsRangeKey) with
//                | [||] -> invalidArg (string recordInfo.Type) "Records carrying the RangeKeyConstant attribute must specify a RangeKey property."
//                | [|rangeKeyP|] -> 
//                    let hkca = Option.get hkcaOpt
//                    if not <| isValidFieldName hkca.Name then
//                        invalidArg hkca.Name "invalid hashkey name; must be alphanumeric and should not begin with a number."
//
//                    if recordInfo.Properties |> Array.exists(fun p -> p.Name = hkca.Name) then
//                        invalidArg (string recordInfo.Type) "Default HashKey attribute contains conflicting name."
//
//                    let pickler = Pickler.resolveUntyped hkca.HashKeyType
//                    DefaultHashKey(hkca.Name, hkca.HashKey, pickler, rangeKeyP)
//
//                | _ -> invalidArg (string recordInfo.Type) "Found more than one record fields carrying the RangeKey attribute."
//
//            | [||] -> invalidArg (string recordInfo.Type) "Found no record fields carrying the HashKey attribute."
//            | _ -> invalidArg (string recordInfo.Type) "Found more than one record fields carrying the HashKey attribute."
//
//        { PrimaryKey = primaryKey ; LocalSecondaryIndices = localSecondaryIndices }

        
//
//
//type TableKeySchema with
//    /// Extracts table key schema record from key structure type
//    static member OfKeyStructure(ks : KeyStructure) : TableKeySchema =
//        { PrimaryKey = PrimaryKeySchema.OfKeyStructure ks.PrimaryKey ;
//            LocalSecondaryIndices = Array.toList ks.LocalSecondaryIndices } 
//
//    /// Extract key schema from DynamoDB table description object
//    static member OfTableDescription (td : TableDescription) : TableKeySchema =
//        let mkKeySchema (kse : KeySchemaElement) =
//            let ad = td.AttributeDefinitions |> Seq.find (fun ad -> ad.AttributeName = kse.AttributeName)
//            { AttributeName = kse.AttributeName ; KeyType = ad.AttributeType }
//
//        let mkLocalSecondaryIndex (lsid : LocalSecondaryIndexDescription) : LocalSecondaryIndexSchema =
//            if lsid.Projection.ProjectionType <> ProjectionType.ALL then
//                sprintf "Table '%s' contains local secondary index of unsupported projection type '%O'"
//                    td.TableName lsid.Projection.ProjectionType
//                |> invalidOp
//
//            {
//                IndexName = lsid.IndexName 
//                LocalSecondaryRangeKey = lsid.KeySchema |> Seq.find (fun ks -> ks.KeyType = KeyType.RANGE) |> mkKeySchema
//            }
//           
//        let primaryKey =
//            { 
//                HashKey = td.KeySchema |> Seq.find (fun ks -> ks.KeyType = KeyType.HASH) |> mkKeySchema
//                RangeKey = td.KeySchema |> Seq.tryPick (fun ks -> if ks.KeyType = KeyType.RANGE then Some(mkKeySchema ks) else None)
//            }
//
//        {
//            PrimaryKey = primaryKey
//            LocalSecondaryIndices = td.LocalSecondaryIndexes |> Seq.map mkLocalSecondaryIndex |> Seq.toList
//        }
//
//    /// Create a CreateTableRequest using supplied key schema
//    member schema.CreateCreateTableRequest (tableName : string, provisionedThroughput : ProvisionedThroughput) =
//
//        let ctr = new CreateTableRequest(TableName = tableName)
//        let addKey kt (ks : KeyAttributeSchema) =
//            ctr.KeySchema.Add <| new KeySchemaElement(ks.AttributeName, kt)
//            ctr.AttributeDefinitions.Add <| new AttributeDefinition(ks.AttributeName, ks.KeyType)
//
//        addKey KeyType.HASH schema.PrimaryKey.HashKey
//        schema.PrimaryKey.RangeKey |> Option.iter (fun rk -> addKey KeyType.RANGE rk)
//        ctr.ProvisionedThroughput <- provisionedThroughput
//        for lsi in schema.LocalSecondaryIndices do
//            let l = new LocalSecondaryIndex()
//            l.IndexName <- lsi.IndexName
//            l.KeySchema.Add <| new KeySchemaElement(schema.PrimaryKey.HashKey.AttributeName, KeyType.HASH)
//            l.KeySchema.Add <| new KeySchemaElement(lsi.LocalSecondaryRangeKey.AttributeName, KeyType.RANGE)
//            l.Projection <- new Projection(ProjectionType = ProjectionType.ALL)
//            ctr.LocalSecondaryIndexes.Add l
//            ctr.AttributeDefinitions.Add <| 
//                new AttributeDefinition(lsi.LocalSecondaryRangeKey.AttributeName, lsi.LocalSecondaryRangeKey.KeyType)
//        ctr