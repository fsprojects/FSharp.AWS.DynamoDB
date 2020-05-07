open Expecto
open FSharp.AWS.DynamoDB.Tests
open ``Record Generation Tests``

let RecordGenerationTests =
    testList "RecordGenerationTests"
        [
            testCase "Generate correct schema for S Record" ``Generate correct schema for S Record``
            testCase "Generate correct schema for N Record" ``Generate correct schema for N Record``
            testCase "Generate correct schema for B Record" ``Generate correct schema for B Record``
            testCase "Generate correct schema for SN Record" ``Generate correct schema for SN Record``
            testCase "Generate correct schema for NB Record" ``Generate correct schema for NB Record``
            testCase "Generate correct schema for BS Record" ``Generate correct schema for BS Record``
            testCase "Generate correct schema for NS Constant HashKey Record" ``Generate correct schema for NS Constant HashKey Record``
            testCase "Generate correct schema for BS Constant RangeKey Record" ``Generate correct schema for BS Constant RangeKey Record``
            testCase "Generate correct schema for SS String representation Record" ``Generate correct schema for SS String representation Record``
            testCase "Generate correct schema for Custom HashKey Name Record" ``Generate correct schema for Custom HashKey Name Record``
            testCase "Attribute value roundtrip for S Record" ``Attribute value roundtrip for S Record``
            testCase "Attribute value roundtrip for N Record" ``Attribute value roundtrip for N Record``
            testCase "Attribute value roundtrip for B Record" ``Attribute value roundtrip for B Record``
            testCase "Attribute value roundtrip for SN Record" ``Attribute value roundtrip for SN Record``
            testCase "Attribute value roundtrip for NB Record" ``Attribute value roundtrip for NB Record``
            testCase "Attribute value roundtrip for BS Record" ``Attribute value roundtrip for BS Record``
            testCase "Attribute value roundtrip for NS constant HashKey Record" ``Attribute value roundtrip for NS constant HashKey Record``
            testCase "Roundtrip complex record A" ``Roundtrip complex record A``
            testCase "Roundtrip complex record B" ``Roundtrip complex record B``
            testCase "Roundtrip complex record C" ``Roundtrip complex record C``
            testCase "Roundtrip complex record D" ``Roundtrip complex record D``
            testCase "Record lacking key attributes should fail" ``Record lacking key attributes should fail``
            testCase "Record lacking hashkey attribute should fail" ``Record lacking hashkey attribute should fail``
            testCase "Record containing unsupported attribute type should fail" ``Record containing unsupported attribute type should fail``
            testCase "Record containing key field of unsupported type should fail" ``Record containing key field of unsupported type should fail``
            testCase "Record containing multiple HashKey attributes should fail" ``Record containing multiple HashKey attributes should fail``
            testCase "Record containing costant HashKey attribute lacking RangeKey attribute should fail" ``Record containing costant HashKey attribute lacking RangeKey attribute should fail``
            testCase "Record containing costant HashKey attribute with HashKey attribute should fail" ``Record containing costant HashKey attribute with HashKey attribute should fail``
            testCase "Generated picklers should be singletons" ``Generated picklers should be singletons``
            testCase "GSI Simple HashKey" ``GSI Simple HashKey``
            testCase "GSI Simple Combined key" ``GSI Simple Combined key``
            testCase "GSI should fail if supplying RangeKey only" ``GSI should fail if supplying RangeKey only``
            testCase "GSI should fail if invalid key type" ``GSI should fail if invalid key type``
            testCase "Sparse GSI" ``Sparse GSI``
            testCase "GSI should fail with option primary hash key" ``GSI should fail with option primary hash key``
            testCase "LSI Simple" ``LSI Simple``
            testCase "LSI should fail if no RangeKey is specified" ``LSI should fail if no RangeKey is specified``
            testCase "Sparse LSI" ``Sparse LSI``
            testCase "DateTimeOffset pickler encoding should preserve ordering" ``DateTimeOffset pickler encoding should preserve ordering``
            testCase "DateTimeOffset pickler encoding should preserve offsets" ``DateTimeOffset pickler encoding should preserve offsets``
        ]

let simpleTableOperationTestsTableFixture = new TableFixture()
let simpleTableOperationTests = ``Simple Table Operation Tests``(simpleTableOperationTestsTableFixture)
let SimpleTableOperationTests =
    testList "SimpleTableOperationTests"
        [
            testCase "Convert to compatible table" simpleTableOperationTests.``Convert to compatible table``
            testCase "Convert to compatible table 2" simpleTableOperationTests.``Convert to compatible table 2``
            testCase "Simple Put Operation" simpleTableOperationTests.``Simple Put Operation``
            testCase "ContainsKey Operation" simpleTableOperationTests.``ContainsKey Operation``
            testCase "Batch Put Operation" simpleTableOperationTests.``Batch Put Operation``
            testCase "Batch Delete Operation" simpleTableOperationTests.``Batch Delete Operation``
            testCase "Simple Delete Operation" simpleTableOperationTests.``Simple Delete Operation``
            testCase "Idempotent Delete Operation" simpleTableOperationTests.``Idempotent Delete Operation``
        ]

let conditionalExpressionTestsTableFixture = new TableFixture()
let conditionalExpressionTests = ``Conditional Expression Tests``(conditionalExpressionTestsTableFixture)
let ConditionalExpressionTests =
    testList "ConditionalExpressionTests"
        [
            testCase "Item exists precondition" conditionalExpressionTests.``Item exists precondition``
            testCase "Item not exists precondition" conditionalExpressionTests.``Item not exists precondition``
            testCase "String precondition" conditionalExpressionTests.``String precondition``
            testCase "Number precondition" conditionalExpressionTests.``Number precondition``
            testCase "Bool precondition" conditionalExpressionTests.``Bool precondition``
            testCase "Bytes precondition" conditionalExpressionTests.``Bytes precondition``
            testCase "DateTimeOffset precondition" conditionalExpressionTests.``DateTimeOffset precondition``
            testCase "TimeSpan precondition" conditionalExpressionTests.``TimeSpan precondition``
            testCase "Guid precondition" conditionalExpressionTests.``Guid precondition``
            testCase "Guid not equal precondition" conditionalExpressionTests.``Guid not equal precondition``
            testCase "Optional precondition" conditionalExpressionTests.``Optional precondition``
            testCase "Optional-Value precondition" conditionalExpressionTests.``Optional-Value precondition``
            testCase "Ref precondition" conditionalExpressionTests.``Ref precondition``
            testCase "Tuple precondition" conditionalExpressionTests.``Tuple precondition``
            testCase "Record precondition" conditionalExpressionTests.``Record precondition``
            testCase "Nested attribute precondition" conditionalExpressionTests.``Nested attribute precondition``
            testCase "Nested union precondition" conditionalExpressionTests.``Nested union precondition``
            testCase "String-Contains precondition" conditionalExpressionTests.``String-Contains precondition``
            testCase "String-StartsWith precondition" conditionalExpressionTests.``String-StartsWith precondition``
            testCase "String-length precondition" conditionalExpressionTests.``String-length precondition``
            testCase "Array-length precondition" conditionalExpressionTests.``Array-length precondition``
            testCase "Array index precondition" conditionalExpressionTests.``Array index precondition``
            testCase "List-length precondition" conditionalExpressionTests.``List-length precondition``
            testCase "List-isEmpty precondition" conditionalExpressionTests.``List-isEmpty precondition``
            testCase "Set-count precondition" conditionalExpressionTests.``Set-count precondition``
            testCase "Set-contains precondition" conditionalExpressionTests.``Set-contains precondition``
            testCase "Map-count precondition" conditionalExpressionTests.``Map-count precondition``
            testCase "Map-contains precondition" conditionalExpressionTests.``Map-contains precondition``
            testCase "Map Item precondition" conditionalExpressionTests.``Map Item precondition``
            testCase "Map Item parametric precondition" conditionalExpressionTests.``Map Item parametric precondition``
            testCase "Fail on identical comparands" conditionalExpressionTests.``Fail on identical comparands``
            testCase "EXISTS precondition" conditionalExpressionTests.``EXISTS precondition``
            testCase "NOT_EXISTS precondition" conditionalExpressionTests.``NOT_EXISTS precondition``
            testCase "Boolean precondition" conditionalExpressionTests.``Boolean precondition``
            testCase "Simple Query Expression" conditionalExpressionTests.``Simple Query Expression``
            testCase "Simple Query/Filter Expression" conditionalExpressionTests.``Simple Query/Filter Expression``
            testCase "Detect incompatible key conditions" conditionalExpressionTests.``Detect incompatible key conditions``
            testCase "Detect incompatible comparisons" conditionalExpressionTests.``Detect incompatible comparisons``
            testCase "Simple Scan Expression" conditionalExpressionTests.``Simple Scan Expression``
            testCase "Simple Parametric Conditional" conditionalExpressionTests.``Simple Parametric Conditional``
            testCase "Parametric Conditional with optional argument" conditionalExpressionTests.``Parametric Conditional with optional argument``
            testCase "Parametric Conditional with invalid param usage" conditionalExpressionTests.``Parametric Conditional with invalid param usage``
            testCase "Global Secondary index query" conditionalExpressionTests.``Global Secondary index query``
            testCase "Local Secondary index query" conditionalExpressionTests.``Local Secondary index query``
        ]

let updateExpressionTestsTableFixture = new TableFixture()
let updateExpressionTests = ``Update Expression Tests``(updateExpressionTestsTableFixture)
let UpdateExpressionTests =
    testList "UpdateExpressionTests"
        [
            testCase "Attempt to update HashKey" updateExpressionTests.``Attempt to update HashKey``
            testCase "Attempt to update RangeKey" updateExpressionTests.``Attempt to update RangeKey``
            testCase "Returning old value" updateExpressionTests.``Returning old value``
            testCase "Simple update DateTimeOffset" updateExpressionTests.``Simple update DateTimeOffset``
            testCase "Simple update TimeSpan" updateExpressionTests.``Simple update TimeSpan``
            testCase "Simple update Guid" updateExpressionTests.``Simple update Guid``
            testCase "Simple increment update" updateExpressionTests.``Simple increment update``
            testCase "Simple decrement update" updateExpressionTests.``Simple decrement update``
            testCase "Simple update serialized value" updateExpressionTests.``Simple update serialized value``
            testCase "Update using nested record values" updateExpressionTests.``Update using nested record values``
            testCase "Update using nested union values" updateExpressionTests.``Update using nested union values``
            testCase "Update using nested list" updateExpressionTests.``Update using nested list``
            testCase "Update using tuple values" updateExpressionTests.``Update using tuple values``
            testCase "Update optional field to None" updateExpressionTests.``Update optional field to None``
            testCase "Update optional field to Some" updateExpressionTests.``Update optional field to Some``
            testCase "Update list field to non-empty" updateExpressionTests.``Update list field to non-empty``
            testCase "Update list field to empty" updateExpressionTests.``Update list field to empty``
            testCase "Update list with concatenation" updateExpressionTests.``Update list with concatenation``
            testCase "Update list with consing" updateExpressionTests.``Update list with consing``
            testCase "Update using defaultArg combinator (Some)" updateExpressionTests.``Update using defaultArg combinator (Some)``
            testCase "Update using defaultArg combinator (None)" updateExpressionTests.``Update using defaultArg combinator (None)``
            testCase "Update int set with add element" updateExpressionTests.``Update int set with add element``
            testCase "Update int set with remove element" updateExpressionTests.``Update int set with remove element``
            testCase "Update int set with append set" updateExpressionTests.``Update int set with append set``
            testCase "Update int set with remove set" updateExpressionTests.``Update int set with remove set``
            testCase "Update string set with add element" updateExpressionTests.``Update string set with add element``
            testCase "Update string set with remove element" updateExpressionTests.``Update string set with remove element``
            testCase "Update string set with append set" updateExpressionTests.``Update string set with append set``
            testCase "Update byte set with append set" updateExpressionTests.``Update byte set with append set``
            testCase "Update string set with remove set" updateExpressionTests.``Update string set with remove set``
            testCase "Update map with add element" updateExpressionTests.``Update map with add element``
            testCase "Update map with remove element" updateExpressionTests.``Update map with remove element``
            testCase "Update map with remove element on existing" updateExpressionTests.``Update map with remove element on existing``
            testCase "Update map entry with Item access" updateExpressionTests.``Update map entry with Item access``
            testCase "Parametric map Item access" updateExpressionTests.``Parametric map Item access``
            testCase "Parametric map ContainsKey" updateExpressionTests.``Parametric map ContainsKey``
            testCase "Combined update with succesful precondition" updateExpressionTests.``Combined update with succesful precondition``
            testCase "Combined update with failed precondition" updateExpressionTests.``Combined update with failed precondition``
            testCase "SET an attribute" updateExpressionTests.``SET an attribute``
            testCase "SET a union attribute" updateExpressionTests.``SET a union attribute``
            testCase "REMOVE an attribute" updateExpressionTests.``REMOVE an attribute``
            testCase "ADD to set" updateExpressionTests.``ADD to set``
            testCase "DELETE from set" updateExpressionTests.``DELETE from set``
            testCase "Detect overlapping paths" updateExpressionTests.``Detect overlapping paths``
            testCase "Simple Parametric Updater 1" updateExpressionTests.``Simple Parametric Updater 1``
            testCase "Simple Parametric Updater 2" updateExpressionTests.``Simple Parametric Updater 2``
            testCase "Parametric Updater with optional argument" updateExpressionTests.``Parametric Updater with optional argument``
            testCase "Parametric Updater with heterogeneous argument consumption" updateExpressionTests.``Parametric Updater with heterogeneous argument consumption``
            testCase "Parametric Updater with invalid param usage" updateExpressionTests.``Parametric Updater with invalid param usage``
            testCase "Parametric Updater with map add element with constant key" updateExpressionTests.``Parametric Updater with map add element with constant key``
            testCase "Parametric Updater with map add element with parametric key" updateExpressionTests.``Parametric Updater with map add element with parametric key``
            testCase "Parametric Updater with map remove element with parametric key" updateExpressionTests.``Parametric Updater with map remove element with parametric key``
        ]

let projectionExpressionTestsTableFixture = new TableFixture()
let projectionExpressionTests = ``Projection Expression Tests``(projectionExpressionTestsTableFixture)
let ProjectionExpressionTests =
    testList "ProjectionExpressionTests"
        [
            testCase "Should fail on invalid projections" projectionExpressionTests.``Should fail on invalid projections``
            testCase "Should fail on conflicting projections" projectionExpressionTests.``Should fail on conflicting projections``
            testCase "Null value projection" projectionExpressionTests.``Null value projection``
            testCase "Single value projection" projectionExpressionTests.``Single value projection``
            testCase "Map projection" projectionExpressionTests.``Map projection``
            testCase "Option-None projection" projectionExpressionTests.``Option-None projection``
            testCase "Option-Some projection" projectionExpressionTests.``Option-Some projection``
            testCase "Multi-value projection" projectionExpressionTests.``Multi-value projection``
            testCase "Nested value projection 1" projectionExpressionTests.``Nested value projection 1``
            testCase "Nested value projection 2" projectionExpressionTests.``Nested value projection 2``
            testCase "Projected query" projectionExpressionTests.``Projected query``
            testCase "Projected scan" projectionExpressionTests.``Projected scan``
        ]

let sparseGSITestsTableFixture = new TableFixture()
let sparseGSITests = ``Sparse GSI Tests``(sparseGSITestsTableFixture)
let SparseGSITests =
    testList "SparseGSITests"
        [
            testCase "GSI Put Operation" sparseGSITests.``GSI Put Operation``
            testCase "GSI Query Operation (match)" sparseGSITests.``GSI Query Operation (match)``
            testCase "GSI Query Operation (missing)" sparseGSITests.``GSI Query Operation (missing)``
        ]

[<Tests>]
let tests =
    testList ""
        [
            RecordGenerationTests
            SimpleTableOperationTests
            ConditionalExpressionTests
            UpdateExpressionTests
            ProjectionExpressionTests
            SparseGSITests
        ]

[<EntryPoint>]
let main args =
    runTestsWithArgs defaultConfig args tests
