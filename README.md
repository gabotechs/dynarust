# Dynarust

[![Coverage Status](https://coveralls.io/repos/github/gabotechs/dynarust/badge.svg?branch=main)](https://coveralls.io/github/gabotechs/dynarust?branch=main)

An opinionated DynamoDB ODM library for Rust that maps `structs`
to native Dynamo items using [serde](https://github.com/serde-rs/serde)
and [serde_json](https://github.com/serde-rs/json).

It only works with tables with a `HASH` key and a `RANGE` key, referred in
the code as `pk` and `sk`. This setup can be useful for a very wide variety
of use cases.

```rust
use serde::{Deserialize, Serialize};
use serde_json::json;

#[derive(Serialize, Deserialize)]
struct Car {
    brand: String,
    model: String,
    horse_power: i64
}

impl dynarust::Resource for Car {
    fn table() -> String { "Cars".into() }
    fn pk_sk(&self) -> (String, String) { (self.brand.clone(), self.model.clone()) }
}

#[tokio::main]
async fn main() -> Result<()> {
    let client = dynarust::Client::aws().await;
    
    let car = Car {
        brand: "Tesla",
        model: "Y",
        horse_power: 347
    };
    
    client.create(&car).await?;
    
    client.update(&car, json!({ "horse_power": 534 })).await?;
    
    let tesla = client.get::<Car>(("Tesla".into(), "Y".into())).await?;
    
    assert_eq!(tesla.unwrap().horse_power, 534)
}
```

# Setup

In order to use this library, the table setup would need to look like this equivalent CFN template:
```yaml
DynamoDBTable:
  Type: AWS::DynamoDB::Table
  Properties:
    TableName: my-table
    AttributeDefinitions:
      - AttributeName: PrimaryKey
        AttributeType: S
      - AttributeName: SecondaryKey
        AttributeType: S
    KeySchema:
      - AttributeName: PrimaryKey
        KeyType: HASH
      - AttributeName: SecondaryKey
        KeyType: RANGE
```

`dynarust` expects that attributes `PrimaryKey` and `SecondaryKey` are declared in the key schema
as `HASH` and `RANGE` values, and that both are of type `S` (string).


# Coming next

- Support for global secondary indexes, with the same API that exists right now for the
primary ones.

# Current Limitations

- Only works with the `HASH` and `RANGE` keys setup.
- It is not compatible with global or local secondary indexes.
- Missing some batch operations, right now only `batch_get` is implemented.
- 

