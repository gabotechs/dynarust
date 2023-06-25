use std::collections::HashMap;

use aws_sdk_dynamodb::client::fluent_builders::{DeleteItem, PutItem, UpdateItem};
use aws_sdk_dynamodb::model::{
    condition_check, delete, put, update, AttributeValue, TransactWriteItem,
};
use rand::distributions::Alphanumeric;
use rand::Rng;

use crate::client::{PK, SK};
use crate::{DynamoOperator, Resource};

#[derive(Default)]
pub struct ConditionCheckInfo {
    expression: String,
    names: HashMap<String, String>,
    values: HashMap<String, AttributeValue>,
}

impl ConditionCheckInfo {
    pub(crate) fn condition_expression(mut self, input: impl Into<String>) -> Self {
        self.expression = input.into();
        self
    }

    pub(crate) fn expression_attribute_names(
        mut self,
        k: impl Into<String>,
        v: impl Into<String>,
    ) -> Self {
        self.names.insert(k.into(), v.into());
        self
    }

    pub(crate) fn expression_attribute_values(
        mut self,
        k: impl Into<String>,
        v: AttributeValue,
    ) -> Self {
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

fn seed() -> String {
    rand::thread_rng()
        .sample_iter(&Alphanumeric)
        .take(7)
        .map(char::from)
        .collect()
}

/// Creates a condition check that checks if the referenced resource exists.
/// If the referenced resource exist, the condition check passes, otherwise it fails.
pub fn condition_check_exists() -> ConditionCheckInfo {
    ConditionCheckInfo::default()
        .condition_expression("attribute_exists(#pk) and attribute_exists(#sk)")
        .expression_attribute_names("#pk", PK)
        .expression_attribute_names("#sk", SK)
}

/// Creates a condition check that checks if the referenced resource does not exist.
/// If the referenced resource exist, the condition check fails, otherwise it succeeds.
pub fn condition_check_not_exists() -> ConditionCheckInfo {
    ConditionCheckInfo::default()
        .condition_expression("attribute_not_exists(#pk) and attribute_not_exists(#sk)")
        .expression_attribute_names("#pk", PK)
        .expression_attribute_names("#sk", SK)
}

/// Creates a condition check for checking a number's value.
///
/// # arguments
/// * `attr` - The field in the resource that should be checked.
/// * `operator` - The operator for comparing the field to the value.
/// * `value` - The numeric value.
pub fn condition_check_number(
    attr: &str,
    operator: DynamoOperator,
    value: i64,
) -> ConditionCheckInfo {
    let key = seed();
    ConditionCheckInfo::default()
        .condition_expression(format!("#{} {} :{}", key, operator, key))
        .expression_attribute_names(format!("#{}", key), attr)
        .expression_attribute_values(format!(":{}", key), AttributeValue::N(value.to_string()))
}

/// Creates a condition check for checking a string's value.
///
/// # arguments
/// * `attr` - The field in the resource that should be checked.
/// * `operator` - The operator for comparing the field to the value.
/// * `value` - The string value.
pub fn condition_check_string(
    attr: &str,
    operator: DynamoOperator,
    value: &str,
) -> ConditionCheckInfo {
    let key = seed();
    ConditionCheckInfo::default()
        .condition_expression(format!("#{} {} :{}", key, operator, key))
        .expression_attribute_names(format!("#{}", key), attr)
        .expression_attribute_values(format!(":{}", key), AttributeValue::S(value.to_string()))
}

/// Takes a Condition check and adds it as a standalone check to a transaction.
/// Useful for when a condition check must be made in a transaction but any of previous the items
/// in the transaction refer to the item that wants to be checked.
///
/// # arguments
/// * `pk_sk` - The pk and sk pair for identifying the resource.
/// * `info` - The condition check itself.
/// * `transaction_context` - the transaction context to which the condition check will be added.
///
/// # example
///
/// ```
/// async {
///     let mut context = dynarust::begin_transaction();
///     dynarust::transact_create(&resource, &mut context)?;
///     dynarust::transact_condition_check::<TestResource>(
///         ("non".into(), "existing".into()),
///         dynarust::condition_check_not_exists(),
///         &mut context,
///     );
///     let err = client.execute_transaction(context).await?;
/// }
/// ```
pub fn transact_condition_check<T: Resource>(
    (pk, sk): (String, String),
    info: ConditionCheckInfo,
    transaction_context: &mut Vec<TransactWriteItem>,
) {
    let builder = condition_check::Builder::default()
        .table_name(T::table())
        .key(PK, AttributeValue::S(pk))
        .key(SK, AttributeValue::S(sk));

    let check = info.dump_in_condition_check(builder).build();

    transaction_context.push(TransactWriteItem::builder().condition_check(check).build());
}

#[cfg(test)]
mod tests {
    use crate::client::tests::TestResource;
    use crate::condition_check::{condition_check_exists, transact_condition_check};
    use crate::create::transact_create;
    use crate::{begin_transaction, Client};

    #[tokio::test]
    async fn creates_only_if_other_exists() {
        let client = Client::local().await;
        client.create_table::<TestResource>(None).await.unwrap();

        let resource = TestResource {
            pk: "creates_only_if_other_exists".to_string(),
            sk: "1".to_string(),
            ..Default::default()
        };

        let mut context = begin_transaction();
        transact_create(&resource, &mut context).unwrap();
        transact_condition_check::<TestResource>(
            ("non".into(), "existing".into()),
            condition_check_exists(),
            &mut context,
        );
        let err = client.execute_transaction(context).await.unwrap_err();

        assert_eq!(err.to_string(), "Transaction cancelled, please refer cancellation reasons for specific reasons [None, ConditionalCheckFailed]")
    }
}
