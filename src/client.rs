use std::collections::HashMap;
use std::env;
use std::fmt::{Display, Formatter};

use aws_sdk_dynamodb::model::{AttributeValue, TransactWriteItem};
use serde::Serialize;
use serde_json::{Map, Value};

use crate::DynarustError;

pub(crate) const PK: &str = "PrimaryKey";
pub(crate) const SK: &str = "SecondaryKey";

/// list options for listing resources in dynamo under the same PrimaryKey.
pub struct ListOptions {
    /// Sort key to start from listing. If not provided it will start listing from the beginning.
    pub from: Option<String>,
    /// maximum number of items to list in a single page, default is 25.
    pub limit: i32,
    /// whether to list in ascending order or in descending order, default is false.
    pub sort_desc: bool,
}

impl Default for ListOptions {
    fn default() -> Self {
        Self {
            from: None,
            limit: 25,
            sort_desc: false,
        }
    }
}

/// All the resources that dynarust uses must implement this trait.
pub trait Resource {
    /// DynamoDB's table name for this resource.
    fn table() -> String;
    /// Rules for forming the PrimaryKey and SecondaryKey based on the resource object.
    fn pk_sk(&self) -> (String, String);
}

/// Client that holds the connection to dynamo.
pub struct Client {
    pub(crate) client: aws_sdk_dynamodb::Client,
}

impl Client {
    /// Build a client from AWS config. It will look for the next environment variables:
    /// AWS_ACCESS_KEY_ID
    /// AWS_SECRET_ACCESS_KEY
    /// AWS_REGION
    pub async fn aws() -> Self {
        let cfg = aws_config::from_env().load().await;
        Client {
            client: aws_sdk_dynamodb::Client::new(&cfg),
        }
    }

    /// Connect against a local version of DynamoDB running in port 8000.
    /// A DynamoDB instance can be easily launched with:
    /// docker run -p 8000:8000 amazon/dynamodb-local
    pub async fn local() -> Self {
        env::set_var("AWS_REGION", "us-east-1");
        env::set_var("AWS_ACCESS_KEY_ID", ".");
        env::set_var("AWS_SECRET_ACCESS_KEY", ".");
        let cfg = aws_config::from_env().load().await;
        Client {
            client: aws_sdk_dynamodb::Client::from_conf(
                aws_sdk_dynamodb::config::Builder::from(&cfg)
                    .endpoint_url("http://localhost:8000")
                    .build(),
            ),
        }
    }

    pub(crate) fn resource_as_object<T: Resource + Serialize>(
        resource: &T,
    ) -> Result<Map<String, Value>, DynarustError> {
        serde_json::to_value(resource)
            .map_err(|_| {
                DynarustError::AttributeParseError(
                    "resource cannot be serialized to value".to_string(),
                )
            })?
            .as_object()
            .ok_or_else(|| {
                DynarustError::AttributeParseError(
                    "passed resource did not serialize to object".to_string(),
                )
            })
            .cloned()
    }

    pub(crate) fn value2attr(v: &Value) -> Result<AttributeValue, DynarustError> {
        if let Some(str) = v.as_str() {
            Ok(AttributeValue::S(str.to_string()))
        } else if let Some(int) = v.as_i64() {
            Ok(AttributeValue::N(int.to_string()))
        } else if let Some(bool) = v.as_bool() {
            Ok(AttributeValue::Bool(bool))
        } else if let Some(float) = v.as_f64() {
            Ok(AttributeValue::N(float.to_string()))
        } else if let Some(..) = v.as_null() {
            Ok(AttributeValue::Null(true))
        } else if let Some(arr) = v.as_array() {
            let mut result = vec![];
            for e in arr.iter() {
                result.push(Self::value2attr(e)?);
            }
            Ok(AttributeValue::L(result))
        } else if let Some(obj) = v.as_object() {
            let mut hashmap = HashMap::new();
            for (k, v) in obj.into_iter() {
                hashmap.insert(k.clone(), Self::value2attr(v)?);
            }
            Ok(AttributeValue::M(hashmap))
        } else {
            Err(DynarustError::AttributeParseError(format!(
                "cannot map value {} to a dynamo attribute",
                v
            )))
        }
    }

    pub(crate) fn attr2value(attr: &AttributeValue) -> Result<Value, DynarustError> {
        match attr {
            AttributeValue::S(str) => Ok(Value::from(str.to_string())),
            AttributeValue::N(num) => Ok(Value::Number(num.parse().map_err(|_| {
                DynarustError::AttributeParseError(format!("invalid number {num}"))
            })?)),
            AttributeValue::Null(..) => Ok(Value::Null),
            AttributeValue::Bool(bool) => Ok(Value::from(*bool)),
            AttributeValue::L(arr) => {
                let mut result = vec![];
                for e in arr.iter() {
                    result.push(Self::attr2value(e)?)
                }
                Ok(Value::Array(result))
            }
            AttributeValue::M(hashmap) => {
                let mut map = Map::new();
                for (k, v) in hashmap.iter() {
                    map.insert(k.clone(), Self::attr2value(v)?);
                }
                Ok(Value::Object(map))
            }
            _ => Err(DynarustError::AttributeParseError(format!(
                "Error parsing attribute value {:?}",
                attr
            ))),
        }
    }

    /// Executes a transaction given the transaction context.
    ///
    /// # arguments
    ///
    /// * `transaction_context` - A transaction context initiated by `self.begin_transaction`.
    ///
    /// # Examples
    ///
    /// ```
    /// // This example creates two resources transactionally
    /// async {
    ///     let mut context = dynarust::Client::begin_transaction();
    ///     dynarust::Client::transact_create(&resource_1, &mut context)?;
    ///     dynarust::Client::transact_create(&resource_2, &mut context)?;
    ///     client.execute_transaction(context).await?;
    /// }
    /// ```
    pub async fn execute_transaction(
        &self,
        transaction_context: Vec<TransactWriteItem>,
    ) -> Result<(), DynarustError> {
        let mut builder = self.client.transact_write_items();
        for transaction in transaction_context {
            builder = builder.transact_items(transaction.clone())
        }
        builder.send().await?;
        Ok(())
    }
}

/// Creates a transaction context.
///
/// # Examples
///
/// ```
/// // This example creates two resources transactionally
/// async {
///     let mut context = dynarust::begin_transaction();
///     dynarust::transact_create(&resource_1, &mut context)?;
///     dynarust::transact_create(&resource_2, &mut context)?;
///     client.execute_transaction(context).await?;
/// }
/// ```
pub fn begin_transaction() -> Vec<TransactWriteItem> {
    vec![]
}

/// Dynamo operator for comparing values.
pub enum DynamoOperator {
    /// Equals.
    Eq,
    /// Not Equals.
    NEq,
    /// Greater.
    Gt,
    /// Greater or equal.
    GtEq,
    /// Less than.
    Ls,
    /// Less or equal.
    LsEq,
}

impl Display for DynamoOperator {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        f.write_str(match self {
            DynamoOperator::Eq => "=",
            DynamoOperator::NEq => "<>",
            DynamoOperator::Gt => ">",
            DynamoOperator::GtEq => ">=",
            DynamoOperator::Ls => "<",
            DynamoOperator::LsEq => "<=",
        })
    }
}

#[cfg(test)]
pub(crate) mod tests {
    use lazy_static::lazy_static;
    use rand::distributions::Alphanumeric;
    use rand::Rng;
    use serde::{Deserialize, Serialize};
    use serde_json::json;
    use std::collections::HashMap;

    use crate::{Client, Resource};

    lazy_static! {
        pub(crate) static ref TABLE: String = rand::thread_rng()
            .sample_iter(&Alphanumeric)
            .take(7)
            .map(char::from)
            .collect();
    }
    #[derive(Deserialize, Serialize, Default, Debug, PartialEq, Clone)]
    pub(crate) struct Nested {
        code: i64,
        msg: String,
    }

    #[derive(Deserialize, Serialize, Default, Debug, PartialEq, Clone)]
    pub(crate) struct TestResource {
        pub(crate) pk: String,
        pub(crate) sk: String,
        pub(crate) string: String,
        pub(crate) bool: bool,
        pub(crate) int: i64,
        pub(crate) float: f64,
        pub(crate) nullable: Option<String>,
        pub(crate) string_arr: Vec<String>,
        pub(crate) string_2_string_hashmap: HashMap<String, String>,
        pub(crate) nested: Nested,
    }

    impl Resource for TestResource {
        fn table() -> String {
            TABLE.to_string()
        }

        fn pk_sk(&self) -> (String, String) {
            (self.pk.clone(), self.sk.clone())
        }
    }

    #[tokio::test]
    async fn creates_gets_updates_gets_resource() {
        let client = Client::local().await;
        client.create_table::<TestResource>(None).await.unwrap();

        let resource = TestResource {
            pk: "primary".into(),
            sk: "secondary".into(),
            string: "string".into(),
            bool: true,
            int: 1,
            float: 0.0001,
            nullable: Some("nullable".into()),
            string_arr: vec!["foo".into(), "bar".into()],
            string_2_string_hashmap: HashMap::from([
                ("a".into(), "1".into()),
                ("b".into(), "2".into()),
            ]),
            nested: Nested {
                code: 1,
                msg: "foo".into(),
            },
        };

        client.create(&resource).await.unwrap();

        let getted = client
            .get::<TestResource>(resource.pk_sk())
            .await
            .unwrap()
            .unwrap();

        assert_eq!(resource, getted);

        client
            .update(
                &getted,
                json!({
                    "string": "_string",
                    "bool": false,
                    "int": 2,
                    "float": 0.0002,
                    "nullable": null,
                    "string_arr": ["_foo", "_bar"],
                    "string_2_string_hashmap": {
                        "_a": "2",
                        "_b": "3"
                    },
                    "nested": {
                        "code": 2,
                        "msg": "_foo"
                    }
                }),
            )
            .await
            .unwrap();

        let updated = client
            .get::<TestResource>(resource.pk_sk())
            .await
            .unwrap()
            .unwrap();

        let expected = TestResource {
            pk: "primary".into(),
            sk: "secondary".into(),
            string: "_string".into(),
            bool: false,
            int: 2,
            float: 0.0002,
            nullable: None,
            string_arr: vec!["_foo".into(), "_bar".into()],
            string_2_string_hashmap: HashMap::from([
                ("_a".into(), "2".into()),
                ("_b".into(), "3".into()),
            ]),
            nested: Nested {
                code: 2,
                msg: "_foo".into(),
            },
        };

        assert_eq!(expected, updated)
    }
}
