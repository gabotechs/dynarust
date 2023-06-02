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
    fn table() -> String { "Cars".to_string() }
    fn pk(&self) -> String { self.brand.clone() }
    fn sk(&self) -> String { self.model.clone() }
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
    
    let tesla = client.get::<Car>("Tesla", "Y").await?;
    
    assert_eq!(tesla.unwrap().horse_power, 534)
}
```

