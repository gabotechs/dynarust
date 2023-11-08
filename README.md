# Dynarust

[![Coverage Status](https://coveralls.io/repos/github/gabotechs/dynarust/badge.svg?branch=main)](https://coveralls.io/github/gabotechs/dynarust?branch=main)

An opinionated DynamoDB ODM library for Rust that maps `structs`
to native Dynamo items using [serde](https://github.com/serde-rs/serde)
and [serde_json](https://github.com/serde-rs/json).

It only works with tables with a `HASH` key and a `RANGE` key, referred in
the code as `pk` and `sk`. This setup can be useful for a very wide variety
of use cases.

# Usage

You will need the following dependencies in your `Cargo.toml` (tweak versions as you need):

```toml
[dependencies]
dynarust = "*"
serde = "*"
tokio = { version = "*", features = ["full"] }
```

```rust
// Spawn a local dynamo instance: `docker run -p 8000:8000 amazon/dynamodb-local`
use dynarust::serde::{Deserialize, Serialize};
use dynarust::serde_json::json;

#[derive(Serialize, Deserialize)]
struct Car {
    brand: String,
    model: String,
    horse_power: i64,
}

impl dynarust::Resource for Car {
    fn table() -> String { "Cars".into() }
    fn pk_sk(&self) -> (String, String) { (self.brand.clone(), self.model.clone()) }
}

#[tokio::main]
async fn main() {
    let client = dynarust::Client::local().await;
    client.create_table::<Car>(None).await.unwrap();

    let car = Car {
        brand: "Tesla".into(),
        model: "Y".into(),
        horse_power: 347,
    };

    client.create(&car).await.unwrap();

    client.update(&car, json!({ "horse_power": 534 })).await.unwrap();

    let tesla = client.get::<Car>(("Tesla".into(), "Y".into())).await.unwrap();

    assert_eq!(tesla.unwrap().horse_power, 534)
}
```

# Transactions

Dynarust's api is focused on transaction ergonomics, and offers a way or doing transactional operations
almost as if they were just normal operations:

```rust
// Spawn a local dynamo: `docker run -p 8000:8000 amazon/dynamodb-local`
use dynarust::serde::{Deserialize, Serialize};
use dynarust::serde_json::json;

#[derive(Serialize, Deserialize)]
struct Car {
    brand: String,
    model: String,
    n_modifications: i64,
}

impl dynarust::Resource for Car {
    fn table() -> String { "Cars".into() }
    fn pk_sk(&self) -> (String, String) { (self.brand.clone(), self.model.clone()) }
}

#[derive(Serialize, Deserialize)]
struct CarModification {
    car_brand: String,
    car_model: String,
    modification: String,
    price: f64,
}

impl dynarust::Resource for CarModification {
    fn table() -> String { "CarModifications".into() }
    fn pk_sk(&self) -> (String, String) {(
        format!("{}-{}", self.car_brand, self.car_model),
        self.modification.clone(),
    )}
}

#[tokio::main]
async fn main() {
    let client = dynarust::Client::local().await;
    client.create_table::<Car>(None).await.unwrap();
    client.create_table::<CarModification>(None).await.unwrap();

    let car = Car {
        brand: "Tesla".into(),
        model: "Y".into(),
        n_modifications: 0,
    };

    client.create(&car).await.unwrap();

    let car_modification = CarModification {
        car_brand: "Tesla".into(),
        car_model: "Y".into(),
        modification: "Loud exhaust".into(),
        price: 14999.95,
    };

    let mut context = dynarust::begin_transaction();
    dynarust::transact_create(&car_modification, &mut context).unwrap();
    dynarust::transact_update(&car, json!({ "n_modifications": car.n_modifications + 1 }), &mut context).unwrap();
    client.execute_transaction(context).await.unwrap();

    let tesla = client.get::<Car>(("Tesla".into(), "Y".into())).await.unwrap();

    assert_eq!(tesla.unwrap().n_modifications, 1)
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
