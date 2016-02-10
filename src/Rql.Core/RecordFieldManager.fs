namespace Rql.Core

open System.Collections.Concurrent

open Microsoft.FSharp.Quotations

type RecordFieldExtractor<'TRecord> internal () =
    let info = RecordBuilder.mkRecordInfo typeof<'TRecord>

    member __.Type = typeof<'TRecord>

    member __.Properties = info.Properties
    member __.ExtractFields(record : 'TRecord) : RecordFields = RecordBuilder.extractRecordFields info record
    member __.ExtractFields(updateExpr : Expr<'TRecord -> 'TRecord>) : RecordFields = 
        RecordExprs.extractPartialRecordExpression info updateExpr

    member __.ExtractQuery(queryExpr : Expr<'TRecord -> bool>) : QueryExpr =
        RecordExprs.extractQueryExpr info queryExpr

    member __.Rebuild(fields : RecordFields) : 'TRecord = RecordBuilder.rebuildRecord info fields

type RecordFieldExtractor =
    static member Create<'TRecord> () = new RecordFieldExtractor<'TRecord> ()