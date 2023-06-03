use std::env;
use std::fmt::{Display, Formatter};

use aws_sdk_dynamodb::model::{AttributeValue, TransactWriteItem};
use serde::Serialize;
use serde_json::{Map, Value};

use crate::DynarustError;

pub(crate) const PK: &str = "PrimaryKey";
pub(crate) const SK: &str = "SecondaryKey";

pub struct ListOptions {
    pub from: Option<String>,
    pub limit: i32,
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

pub trait Resource {
    fn table() -> String;
    fn pk_sk(&self) -> (String, String);
}

pub struct Client {
    pub(crate) client: aws_sdk_dynamodb::Client,
}

impl Client {
    pub async fn aws() -> Self {
        let cfg = aws_config::from_env().load().await;
        Client {
            client: aws_sdk_dynamodb::Client::new(&cfg),
        }
    }

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
            _ => Err(DynarustError::AttributeParseError(format!(
                "Error parsing attribute value {:?}",
                attr
            ))),
        }
    }

    pub fn begin_transaction() -> Vec<TransactWriteItem> {
        vec![]
    }

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

pub enum DynamoOperator {
    Eq,
    NEq,
    Gt,
    GtEq,
    Ls,
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

    use crate::Resource;

    lazy_static! {
        pub(crate) static ref TABLE: String = rand::thread_rng()
            .sample_iter(&Alphanumeric)
            .take(7)
            .map(char::from)
            .collect();
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
    }

    impl Resource for TestResource {
        fn table() -> String {
            TABLE.to_string()
        }

        fn pk_sk(&self) -> (String, String) {
            (self.pk.clone(), self.sk.clone())
        }
    }
}
