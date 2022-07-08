# Dynamojo
## Because one table is better than more

Dynamojo takes the concept of Dynamodb Single Table design and creates a modeling framework for it. This library is opinionated in the following ways:
- Indexes should be generic. They could mean different things for different types of items. **An index attribute shouldn't imply that it is always a date, color, etc**.
- When using generic indexes the attributes should shadow a human readable attribute. For instance if you have a partition key named "pk" that for items that represent users stores their  userid, then there should also be an attribute named userid.
- When creating models for item types that will be stored in the database the developer should only have to worry about their access patterns in terms of the human readable attributes, not be in the weeds of the index design of the table. Mapping items to indexes should happen in code, not in the table definition itself
- Table and Global Secondary indexes should always define a sortkey. There is no reason not to. It's better to have it in cases where you don't need it than to need it and not have it.

### See test.py for examples

This library is very opinionated about how the table's indexes should be structured. Below is Terraform that shows the
correct way to set up the table. Index keys are never referenced directly when using the table. Rely on IndexMap for that.
Since LSI's can only be created at table creation time, and all indexes cost nothing if not used, we go ahead and create
all of the indexes that AWS will allow us to when the table is created.

```hcl
resource "aws_dynamodb_table" "test_table" {
  name         = "test-dynamojo"
  hash_key     = "pk"
  range_key    = "sk"
  billing_mode = "PAY_PER_REQUEST"

  # LSI attributes
  dynamic "attribute" {
    for_each = range(5)

    content {
      name = "lsi${attribute.value}_sk"
      type = "S"
    }
  }

  # GSI pk attributes
  dynamic "attribute" {
    for_each = range(20)

    content {
      name = "gsi${attribute.value}_pk"
      type = "S"
    }
  }

  # GSI sk attributes
  dynamic "attribute" {
    for_each = range(20)

    content {
      name = "gsi${attribute.value}_sk"
      type = "S"
    }
  }

  attribute {
    name = "pk"
    type = "S"
  }

  attribute {
    name = "sk"
    type = "S"
  }

  # GSI's
  dynamic "global_secondary_index" {
    for_each = range(20)

    content {
      name            = "gsi${global_secondary_index.value}"
      hash_key        = "gsi${global_secondary_index.value}_pk"
      range_key       = "gsi${global_secondary_index.value}_sk"
      projection_type = "ALL"
    }
  }

  # LSI's
  dynamic "local_secondary_index" {
    for_each = range(5)

    content {
      name            = "lsi${local_secondary_index.value}"
      range_key       = "lsi${local_secondary_index.value}_sk"
      projection_type = "ALL"
    }
  }
}
```