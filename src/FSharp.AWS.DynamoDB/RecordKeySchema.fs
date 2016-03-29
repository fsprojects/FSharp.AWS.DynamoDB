[<AutoOpen>]
module internal FSharp.AWS.DynamoDB.KeySchema

open System
open System.Collections.Generic

open Amazon.DynamoDBv2
open Amazon.DynamoDBv2.Model

open FSharp.AWS.DynamoDB

//
//  Table key schema extractor methods for F# records
//

type KeyType =
    | Hash  = 1
    | Range = 2

/// Describes the key structure of a given F# record
type PrimaryKeyStructure =
    | HashKeyOnly of hashKeyProperty:PropertyMetadata
    | Combined of hashKeyProperty:PropertyMetadata * rangeKeyProperty:PropertyMetadata
    | DefaultHashKey of hkName:string * hkValue:obj * hkPickler:Pickler * rangeKeyProperty:PropertyMetadata
    | DefaultRangeKey of rkName:string * rkValue:obj * rkPickler:Pickler * hashKeyProperty:PropertyMetadata

/// Collection of all key schemata in a table, distinguished by index name.
/// Used for compatibility comparisons
[<Sealed; StructuredFormatDisplay("{StructuredFormatDisplay}")>]
type TableKeySchemata (schemata : TableKeySchema[]) =
    let schemata = schemata |> Array.sortBy (fun s -> s.Type)
    member __.Schemata = schemata
    override __.Equals y = match y with :? TableKeySchemata as kss -> schemata = kss.Schemata | _ -> false
    override __.GetHashCode() = hash schemata
    member private __.StructuredFormatDisplay = sprintf "%A" schemata
    override __.ToString() = __.StructuredFormatDisplay

/// Infered key schema metadata for an F# record
type RecordTableInfo =
    {
        Type : Type
        Pickler : Pickler
        Properties : PropertyMetadata []

        /// Primary Key record structure
        PrimaryKeyStructure : PrimaryKeyStructure
        /// Primary Key schema
        PrimaryKeySchema : TableKeySchema
        /// All key schemata defined in the table
        Schemata : TableKeySchemata
        /// Key schemata grouped by property name
        PropertySchemata : Map<string, (TableKeySchema * KeyType) []>
    }


type PrimaryKeyStructure with
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


type RecordTableInfo with
    /// Builds key structure from supplied F# record info
    static member FromRecordPickler<'T> (pickler : RecordPickler<'T>) =
        let hkcaOpt = typeof<'T>.TryGetAttribute<ConstantHashKeyAttribute> ()
        let rkcaOpt = typeof<'T>.TryGetAttribute<ConstantRangeKeyAttribute> ()
        let mkKAS rp = KeyAttributeSchema.Create rp

        let extractKeyType (rp : PropertyMetadata) (attr : Attribute) =
            match attr with
            | :? HashKeyAttribute -> Some(rp, KeyType.Hash, PrimaryKey)
            | :? RangeKeyAttribute -> Some(rp, KeyType.Range, PrimaryKey)
            | :? SecondaryHashKeyAttribute as hk -> Some(rp, KeyType.Hash, GlobalSecondaryIndex hk.IndexName)
            | :? SecondaryRangeKeyAttribute as rk -> Some(rp, KeyType.Range, GlobalSecondaryIndex rk.IndexName)
            | :? LocalSecondaryIndexAttribute as lsi ->
                let name = defaultArg lsi.IndexName (rp.Name + "Index")
                Some(rp, KeyType.Range, LocalSecondaryIndex name)
            | _ -> None


        let primaryKey = ref None
        let extractKeySchema (kst : KeySchemaType) (attributes : seq<PropertyMetadata * KeyType * KeySchemaType>) =
            let groupedAttrs = 
                attributes
                |> Seq.distinctBy (fun (rp,_,_) -> rp)
                |> Seq.map (fun (rp,kt,_) -> kt, rp)
                |> Seq.sortBy (fun (kt,_) -> kt <> KeyType.Hash)
                |> Seq.toArray

            match kst, groupedAttrs with
            | PrimaryKey, _ ->
                let setResult (pks : PrimaryKeyStructure) = 
                    let ks = TableKeySchema.OfKeyStructure pks
                    primaryKey := Some (pks, ks)
                    ks

                match hkcaOpt, rkcaOpt, groupedAttrs with
                | Some _, Some _, _ ->
                    "Cannot specify both HashKey and RangeKey constant attributes in record definition."
                    |> invalidArg (string typeof<'T>)

                | Some hkca, None, [|(KeyType.Range, rk)|] -> 
                    if not <| isValidFieldName hkca.Name then
                        invalidArg hkca.Name "invalid hashkey name; must be alphanumeric and should not begin with a number."

                    if pickler.Properties |> Array.exists(fun p -> p.Name = hkca.Name) then
                        invalidArg (string typeof<'T>) "Default HashKey attribute contains conflicting name."

                    let pickler = Pickler.resolveUntyped hkca.HashKeyType
                    DefaultHashKey(hkca.Name, hkca.HashKey, pickler, rk) |> setResult

                | None, Some rkca, [|(KeyType.Hash, hk)|] ->
                    if not <| isValidFieldName rkca.Name then
                        invalidArg rkca.Name "invalid rangekey name; must be alphanumeric and should not begin with a number."

                    if pickler.Properties |> Array.exists(fun p -> p.Name = rkca.Name) then
                        invalidArg (string typeof<'T>) "Default RangeKey attribute contains conflicting name."

                    let pickler = Pickler.resolveUntyped rkca.HashKeyType
                    DefaultRangeKey(rkca.Name, rkca.RangeKey, pickler, hk) |> setResult

                | None, None, [|(KeyType.Hash, hk)|] -> HashKeyOnly(hk) |> setResult
                | None, None, [|(KeyType.Hash, hk) ; (KeyType.Range, rk)|] -> Combined(hk, rk) |> setResult
                | _ -> invalidArg (string typeof<'T>) "Invalid combination of HashKey and RangeKey attributes."

            | LocalSecondaryIndex _, [|(KeyType.Range, rk)|] ->
                match !primaryKey with
                | None -> "Does not specify a HashKey attribute." |> invalidArg (string typeof<'T>)
                | Some(_,ks) when Option.isNone ks.RangeKey -> 
                    "LocalSecondaryIndex tables must specify a primary RangeKey" |> invalidArg (string typeof<'T>)

                | Some(_,ks) -> { ks with RangeKey = Some (mkKAS rk) ; Type = kst }

            | LocalSecondaryIndex _, ([||] | [|_|]) -> invalidOp "internal error when infering LocalSecondaryIndex."
            | LocalSecondaryIndex id, _ ->
                sprintf "Specified more than one LocalSecondaryIndex properties for '%s'." id
                |> invalidArg (string typeof<'T>)

            | GlobalSecondaryIndex _, [|(KeyType.Hash, hk)|] -> { HashKey = mkKAS hk ; RangeKey = None ; Type = kst }
            | GlobalSecondaryIndex _, [|(KeyType.Hash, hk) ; (KeyType.Range, rk)|] -> 
                { HashKey = mkKAS hk ; RangeKey = Some (mkKAS rk); Type = kst }
            | GlobalSecondaryIndex id, _ ->
                sprintf "Invalid combination of SecondaryHashKey and SecondaryRangeKey attributes for index name '%s'." id
                |> invalidArg (string typeof<'T>)

        let schemata =
            pickler.Properties
            |> Seq.collect (fun rp -> rp.Attributes |> Seq.choose (extractKeyType rp))
            |> Seq.distinct
            |> Seq.groupBy (fun (_,_,ty) -> ty)
            |> Seq.sortBy (fun (ty,_) -> match ty with PrimaryKey -> 0 | _ -> 1) // ensure PrimaryKey is processed first
            |> Seq.map (fun (ty, attributes) -> extractKeySchema ty attributes)
            |> Seq.toArray

        match !primaryKey with
        | None -> "Does not specify a HashKey attribute." |> invalidArg (string typeof<'T>)
        | Some (pkStruct, pkSchema) ->

        let propSchema =
            schemata
            |> Seq.collect (fun attr -> 
                seq { 
                    yield (attr.HashKey, KeyType.Hash, attr)
                    match attr.RangeKey with Some rk -> yield (rk, KeyType.Range, attr) | None -> () })
            |> Seq.groupBy (fun (a, _, _) -> a.AttributeName)
            |> Seq.map (fun (name, kss) -> 
                let schemata = kss |> Seq.map (fun (_,isHashKey,ks) -> ks,isHashKey) |> Seq.toArray
                name, schemata)
            |> Map.ofSeq

        {
            Type = typeof<'T>
            Pickler = pickler :> Pickler
            Properties = pickler.Properties

            PrimaryKeyStructure = pkStruct
            PrimaryKeySchema = pkSchema
            PropertySchemata = propSchema
            Schemata = new TableKeySchemata(schemata)
        }

    member info.GetPropertySchemata(propName : string) =
        defaultArg (info.PropertySchemata.TryFind propName) [||]

type TableKeySchemata with

    /// Extract key schema from DynamoDB table description object
    static member OfTableDescription (td : TableDescription) : TableKeySchemata =
        let mkKeySchema (kse : KeySchemaElement) =
            let ad = td.AttributeDefinitions |> Seq.find (fun ad -> ad.AttributeName = kse.AttributeName)
            { AttributeName = kse.AttributeName ; KeyType = ad.AttributeType }

        let primaryKey =
            { 
                HashKey = td.KeySchema |> Seq.find (fun ks -> ks.KeyType = KeyType.HASH) |> mkKeySchema
                RangeKey = td.KeySchema |> Seq.tryPick (fun ks -> if ks.KeyType = KeyType.RANGE then Some(mkKeySchema ks) else None)
                Type = PrimaryKey
            }

        let mkGlobalSecondaryIndex (gsid : GlobalSecondaryIndexDescription) : TableKeySchema =
            if gsid.Projection.ProjectionType <> ProjectionType.ALL then
                sprintf "Table '%s' contains global secondary index of unsupported projection type '%O'."
                    td.TableName gsid.Projection.ProjectionType
                |> invalidOp

            {
                HashKey = gsid.KeySchema |> Seq.find (fun ks -> ks.KeyType = KeyType.HASH) |> mkKeySchema
                RangeKey = gsid.KeySchema |> Seq.tryFind (fun ks -> ks.KeyType = KeyType.RANGE) |> Option.map mkKeySchema
                Type = GlobalSecondaryIndex gsid.IndexName
            }

        let mkLocalSecondaryIndex (lsid : LocalSecondaryIndexDescription) : TableKeySchema =
            if lsid.Projection.ProjectionType <> ProjectionType.ALL then
                sprintf "Table '%s' contains local secondary index of unsupported projection type '%O'."
                    td.TableName lsid.Projection.ProjectionType
                |> invalidOp

            {
                HashKey = primaryKey.HashKey
                RangeKey = lsid.KeySchema |> Seq.find (fun ks -> ks.KeyType = KeyType.RANGE) |> mkKeySchema |> Some
                Type = LocalSecondaryIndex lsid.IndexName
            }
           
        new TableKeySchemata(
            [|  yield primaryKey
                yield! td.GlobalSecondaryIndexes |> Seq.map mkGlobalSecondaryIndex
                yield! td.LocalSecondaryIndexes |> Seq.map mkLocalSecondaryIndex |])

    /// Create a CreateTableRequest using supplied key schema
    member schema.CreateCreateTableRequest (tableName : string, provisionedThroughput : ProvisionedThroughput) =
        let ctr = new CreateTableRequest(TableName = tableName)
        let inline mkKSE n t = new KeySchemaElement(n, t)

        ctr.ProvisionedThroughput <- provisionedThroughput

        let keyAttrs = new Dictionary<string, KeyAttributeSchema>()
        for tks in schema.Schemata do
            keyAttrs.[tks.HashKey.AttributeName] <- tks.HashKey
            tks.RangeKey |> Option.iter (fun rk -> keyAttrs.[rk.AttributeName] <- rk)

            match tks.Type with
            | PrimaryKey ->
                ctr.KeySchema.Add <| mkKSE tks.HashKey.AttributeName KeyType.HASH
                tks.RangeKey |> Option.iter (fun rk -> ctr.KeySchema.Add <| mkKSE rk.AttributeName KeyType.RANGE)

            | GlobalSecondaryIndex name ->
                let gsi = new GlobalSecondaryIndex()
                gsi.IndexName <- name
                gsi.KeySchema.Add <| mkKSE tks.HashKey.AttributeName KeyType.HASH
                tks.RangeKey |> Option.iter (fun rk -> gsi.KeySchema.Add <| mkKSE rk.AttributeName KeyType.RANGE)
                gsi.Projection <- new Projection(ProjectionType = ProjectionType.ALL)
                gsi.ProvisionedThroughput <- provisionedThroughput
                ctr.GlobalSecondaryIndexes.Add gsi

            | LocalSecondaryIndex name ->
                let lsi = new LocalSecondaryIndex()
                lsi.IndexName <- name
                lsi.KeySchema.Add <| mkKSE tks.HashKey.AttributeName KeyType.HASH
                tks.RangeKey |> Option.iter (fun rk -> lsi.KeySchema.Add <| mkKSE rk.AttributeName KeyType.RANGE)
                lsi.Projection <- new Projection(ProjectionType = ProjectionType.ALL)
                ctr.LocalSecondaryIndexes.Add lsi

        for attr in keyAttrs.Values do
            let ad = new AttributeDefinition(attr.AttributeName, attr.KeyType)
            ctr.AttributeDefinitions.Add ad

        ctr