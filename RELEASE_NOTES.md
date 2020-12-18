### 0.8.2-beta
* Replace attribute name validation with something that sticks closer to the [AWS naming rules](https://docs.aws.amazon.com/amazondynamodb/latest/developerguide/HowItWorks.NamingRulesDataTypes.html) - fixes #29
* Update project to net50 & bumped dependencies

### 0.8.1-beta
* Replace deprecated AWS ProfileManager usage
* Bumped test project netcoreapp version to 3.1
* Fixed 'Invalid UpdateExpression' exception for precomputed Map.remove operations (#20)

### 0.8.0-beta
* Move to netstandard2.0.

### 0.7.0-beta
* Add sparse GSI Support.

### 0.6.0-beta
* Preserve original offsets when persisting DateTimeOffset fields.

### 0.5.0-beta
* Move converter generation to TypeShape.
* Target latest unquote release.

### 0.4.1-beta
* Fix packaging issue.

### 0.4.0-beta
* Implement credential helper methods.

### 0.3.1-beta
* Minor bugfixes.

### 0.3.0-beta
* Implement secondary indices.

### 0.2.1-beta
* Projection expressions bugfixes and improvements.

### 0.2.0-beta
* Implement projection expressions.
* Minor API improvements.
* Minor bugfixes.

### 0.1.1-beta
* Expose ProvisionedThroughput type to local namespace.

### 0.1.0-beta
* Rename to FSharp.AWS.DynamoDB.
* Add update provision throughput methods.

### 0.0.25-alpha
* Improvements to Enumeration representations.
* Add checks for comparison compatibility in condition expressions.

### 0.0.24-alpha
* Bugfix.

### 0.0.23-alpha
* Add parametric support in attribute ids and key lookups.

### 0.0.22-alpha
* Add support for Array/List.isEmpty and Option.isSome/isNone in conditional expressions.

### 0.0.21-alpha
* Bugfix.

### 0.0.20-alpha
* Fix floating point parsing issue.

### 0.0.19-alpha
* Bugfixes.

### 0.0.18-alpha
* Fix API issue.

### 0.0.17-alpha
* Improve exception message in case where table item is not found.

### 0.0.16-alpha
* Tweak ConstanHashKey methods in RecordTemplate.

### 0.0.15-alpha
* Implement GetHashKeyCondition method.

### 0.0.14-alpha
* Implement update expression combiners.

### 0.0.13-alpha
* Implement update expression combiners.

### 0.0.12-alpha
* Make scan filter condition optional.

### 0.0.11-alpha
* Implement conditional combinators.

### 0.0.10-alpha
* Add support for condition expressions in delete operations.

### 0.0.9-alpha
* Add support for attribute existential primitives.

### 0.0.8-alpha
* Implement string representation attribute.

### 0.0.7-alpha
* Support list consing in update expressions.

### 0.0.6-alpha
* TableContext API refinements.

### 0.0.5-alpha
* Implement parametric expressions.

### 0.0.4-alpha
* Bugfix.

### 0.0.3-alpha
* Revisions in TableContext API.
* Support MemoryStream field types.
* Implement DefaultRangeKeyAttribute.

### 0.0.2-alpha
* Improve PropertySerializer API.

### 0.0.1-alpha
* Initial release.
