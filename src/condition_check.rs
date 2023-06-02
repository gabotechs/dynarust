use std::collections::HashMap;

use aws_sdk_dynamodb::client::fluent_builders::{DeleteItem, PutItem, UpdateItem};
use aws_sdk_dynamodb::model::{
    condition_check, delete, put, update, AttributeValue, TransactWriteItem,
};
use rand::distributions::Alphanumeric;
use rand::Rng;

use crate::client::{PK, SK};
use crate::{Client, DynamoOperator, Resource};

#[derive(Default)]
pub struct ConditionCheckInfo {
    expression: String,
    names: HashMap<String, String>,
    values: HashMap<String, AttributeValue>,
}

impl ConditionCheckInfo {
    pub fn condition_expression(mut self, input: impl Into<String>) -> Self {
        self.expression = input.into();
        self
    }

    pub fn expression_attribute_names(
        mut self,
        k: impl Into<String>,
        v: impl Into<String>,
    ) -> Self {
        self.names.insert(k.into(), v.into());
        self
    }

    pub fn expression_attribute_values(mut self, k: impl Into<String>, v: AttributeValue) -> Self {
        self.values.insert(k.into(), v);
        self
    }

    pub(crate) fn merge(mut self, others: Vec<ConditionCheckInfo>) -> Self {
        for other in others {
            self.names.extend(other.names);
            self.values.extend(other.values);

            if self.expression.is_empty() {
                self.expression = other.expression;
                continue;
            } else if !self.expression.starts_with('(') || !self.expression.ends_with(')') {
                self.expression = format!("({})", self.expression)
            }
            self.expression += &format!(" and ({})", other.expression);
        }
        self
    }

    pub(crate) fn dump_in_condition_check(
        self,
        mut builder: condition_check::Builder,
    ) -> condition_check::Builder {
        if self.expression.is_empty() {
            return builder;
        }
        builder = builder.condition_expression(&self.expression);
        for (k, v) in self.names {
            builder = builder.expression_attribute_names(k, v);
        }
        for (k, v) in self.values {
            builder = builder.expression_attribute_values(k, v);
        }
        builder
    }

    pub(crate) fn dump_in_put(self, mut builder: put::Builder) -> put::Builder {
        if self.expression.is_empty() {
            return builder;
        }
        builder = builder.condition_expression(&self.expression);
        for (k, v) in self.names {
            builder = builder.expression_attribute_names(k, v);
        }
        for (k, v) in self.values {
            builder = builder.expression_attribute_values(k, v);
        }
        builder
    }

    pub(crate) fn dump_in_put_item(self, mut builder: PutItem) -> PutItem {
        if self.expression.is_empty() {
            return builder;
        }
        builder = builder.condition_expression(&self.expression);
        for (k, v) in self.names {
            builder = builder.expression_attribute_names(k, v);
        }
        for (k, v) in self.values {
            builder = builder.expression_attribute_values(k, v);
        }
        builder
    }

    pub(crate) fn dump_in_update(self, mut builder: update::Builder) -> update::Builder {
        if self.expression.is_empty() {
            return builder;
        }
        builder = builder.condition_expression(&self.expression);
        for (k, v) in self.names {
            builder = builder.expression_attribute_names(k, v);
        }
        for (k, v) in self.values {
            builder = builder.expression_attribute_values(k, v);
        }
        builder
    }

    pub(crate) fn dump_in_update_item(self, mut builder: UpdateItem) -> UpdateItem {
        if self.expression.is_empty() {
            return builder;
        }
        builder = builder.condition_expression(&self.expression);
        for (k, v) in self.names {
            builder = builder.expression_attribute_names(k, v);
        }
        for (k, v) in self.values {
            builder = builder.expression_attribute_values(k, v);
        }
        builder
    }

    pub(crate) fn dump_in_delete(self, mut builder: delete::Builder) -> delete::Builder {
        if self.expression.is_empty() {
            return builder;
        }
        builder = builder.condition_expression(&self.expression);
        for (k, v) in self.names {
            builder = builder.expression_attribute_names(k, v);
        }
        for (k, v) in self.values {
            builder = builder.expression_attribute_values(k, v);
        }
        builder
    }

    pub(crate) fn dump_in_delete_item(self, mut builder: DeleteItem) -> DeleteItem {
        if self.expression.is_empty() {
            return builder;
        }
        builder = builder.condition_expression(&self.expression);
        for (k, v) in self.names {
            builder = builder.expression_attribute_names(k, v);
        }
        for (k, v) in self.values {
            builder = builder.expression_attribute_values(k, v);
        }
        builder
    }
}

impl Client {
    fn seed() -> String {
        rand::thread_rng()
            .sample_iter(&Alphanumeric)
            .take(7)
            .map(char::from)
            .collect()
    }

    pub fn condition_check_exists() -> ConditionCheckInfo {
        ConditionCheckInfo::default()
            .condition_expression("attribute_exists(#pk) and attribute_exists(#sk)")
            .expression_attribute_names("#pk", PK)
            .expression_attribute_names("#sk", SK)
    }

    pub fn condition_check_not_exists() -> ConditionCheckInfo {
        ConditionCheckInfo::default()
            .condition_expression("attribute_not_exists(#pk) and attribute_not_exists(#sk)")
            .expression_attribute_names("#pk", PK)
            .expression_attribute_names("#sk", SK)
    }

    pub fn condition_check_number(
        attr: &str,
        operator: DynamoOperator,
        value: i64,
    ) -> ConditionCheckInfo {
        let key = Client::seed();
        ConditionCheckInfo::default()
            .condition_expression(format!("#{} {} :{}", key, operator, key))
            .expression_attribute_names(format!("#{}", key), attr)
            .expression_attribute_values(format!(":{}", key), AttributeValue::N(value.to_string()))
    }

    pub fn condition_check_string(
        attr: &str,
        operator: DynamoOperator,
        value: &str,
    ) -> ConditionCheckInfo {
        let key = Client::seed();
        ConditionCheckInfo::default()
            .condition_expression(format!("#{} {} :{}", key, operator, key))
            .expression_attribute_names(format!("#{}", key), attr)
            .expression_attribute_values(format!(":{}", key), AttributeValue::S(value.to_string()))
    }

    pub fn transact_condition_check<T: Resource>(
        pk: &str,
        sk: &str,
        info: ConditionCheckInfo,
        transaction_context: &mut Vec<TransactWriteItem>,
    ) {
        let builder = condition_check::Builder::default()
            .table_name(T::table())
            .key(PK, AttributeValue::S(pk.to_string()))
            .key(SK, AttributeValue::S(sk.to_string()));

        let check = info.dump_in_condition_check(builder).build();

        transaction_context.push(TransactWriteItem::builder().condition_check(check).build());
    }
}

#[cfg(test)]
mod tests {
    use crate::client::tests::TestResource;
    use crate::Client;

    #[tokio::test]
    async fn creates_only_if_other_exists() {
        let client = Client::local().await;
        client.create_table::<TestResource>(None).await.unwrap();

        let resource = TestResource {
            pk: "creates_only_if_other_exists".to_string(),
            sk: "1".to_string(),
            ..Default::default()
        };

        let mut context = Client::begin_transaction();
        Client::transact_create(&resource, &mut context).unwrap();
        Client::transact_condition_check::<TestResource>(
            "non",
            "existing",
            Client::condition_check_exists(),
            &mut context,
        );
        let err = client.execute_transaction(context).await.unwrap_err();

        assert_eq!(err.to_string(), "Transaction cancelled, please refer cancellation reasons for specific reasons [None, ConditionalCheckFailed]")
    }
}
