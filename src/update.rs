use std::collections::HashMap;

use aws_sdk_dynamodb::model::{update, AttributeValue, TransactWriteItem};
use serde::de::DeserializeOwned;
use serde::Serialize;
use serde_json::Value;

use crate::client::{PK, SK};
use crate::condition_check::{condition_check_exists, ConditionCheckInfo};
use crate::{Client, DynarustError, Resource};

impl Client {
    /// Updates a resource. It returns an error if the resource does not exist.
    ///
    /// # arguments
    ///
    /// * `resource` - the resource that will get updated.
    /// * `request` - serde Object with the info for updating the request. If the requests tries
    ///   to update the resource in a way that deserializing it will no longer be compatible with
    ///   type T, it fails at runtime.
    ///
    /// # examples
    ///
    /// ```
    /// use serde_json::json;
    /// async {
    ///    let updated = client
    ///       .update(
    ///           &person,
    ///           json!({ "name": "John", "age":  28 }),
    ///       )
    ///       .await?;
    /// }
    /// ```
    pub async fn update<T: Resource + Serialize + DeserializeOwned>(
        &self,
        resource: &T,
        request: Value,
    ) -> Result<T, DynarustError> {
        self.update_with_checks(resource, request, vec![]).await
    }

    /// Updates a resource with additional condition checks. It returns an error if the resource does not exist.
    ///
    /// # arguments
    ///
    /// * `resource` - the resource that will get updated.
    /// * `request` - serde Object with the info for updating the request. If the requests tries
    ///   to update the resource in a way that deserializing it will no longer be compatible with
    ///   type T, it fails at runtime.
    /// * `condition_checks` - The condition checks that will be added to the transaction item.
    ///
    /// # examples
    ///
    /// ```
    /// use serde_json::json;
    /// async {
    ///    let updated = client
    ///       .update(
    ///           &person,
    ///           json!({ "name": "John", "age":  28 }),
    ///           vec![dynarust::condition_check_number("age", dynarust::DynamoOperator::Gt, 21)],
    ///       )
    ///       .await?;
    /// }
    /// ```
    pub async fn update_with_checks<T: Resource + Serialize + DeserializeOwned>(
        &self,
        resource: &T,
        request: Value,
        condition_checks: Vec<ConditionCheckInfo>,
    ) -> Result<T, DynarustError> {
        let mut object = Self::resource_as_object(resource)?;

        let request: HashMap<String, Value> = serde_json::from_value(request)?;

        for (k, new_v) in request.iter() {
            object[k] = new_v.clone()
        }
        let updated: T = serde_json::from_value(Value::Object(object))?;

        if request.is_empty() {
            return Ok(updated);
        }

        if updated.pk_sk() != resource.pk_sk() {
            return Err(DynarustError::InvalidRequestError(
                "Cannot update PK and/or SK".into(),
            ));
        }

        let condition_check = condition_check_exists().merge(condition_checks);

        let (pk, sk) = resource.pk_sk();
        let mut builder = self
            .client
            .update_item()
            .table_name(T::table())
            .key(PK, AttributeValue::S(pk))
            .key(SK, AttributeValue::S(sk));

        let mut update_expression = "set ".to_string();
        let request_len = request.len();
        for (i, (k, v)) in request.into_iter().enumerate() {
            let name = format!("#updateAttr{}", i);
            let value = format!(":updateAttr{}", i);
            update_expression += &format!("{} = {}", name, value);
            if i < request_len - 1 {
                update_expression += ", "
            }
            builder = builder.expression_attribute_names(name, k);
            builder = builder.expression_attribute_values(value, Self::value2attr(&v)?);
        }

        builder = condition_check.dump_in_update_item(builder);

        builder.update_expression(update_expression).send().await?;

        Ok(updated)
    }
}

/// Adds an update operation to the transaction context.
///
/// # arguments
///
/// * `resource` - the resource that will get updated.
/// * `request` - serde Object with the info for updating the request. If the requests tries
///   to update the resource in a way that deserializing it will no longer be compatible with
///   type T, it fails at runtime.
/// * `transaction_context` - The transaction context to which the create operation will be added.
///
/// # examples
///
/// ```
/// use serde_json::json;
/// async {
///     let mut context = dynarust::begin_transaction();
///     client.transact_update(
///         &person,
///         json!({ "name": "John", "age":  28 }),
///         &mut context
///     )?;
///     client.execute_transaction(context).await?;
/// }
/// ```
pub fn transact_update<T: Resource + Serialize + DeserializeOwned>(
    resource: &T,
    request: Value,
    transaction_context: &mut Vec<TransactWriteItem>,
) -> Result<T, DynarustError> {
    transact_update_with_checks(resource, request, vec![], transaction_context)
}

/// Adds an update operation to the transaction context, with additional condition checks
///
/// # arguments
///
/// * `resource` - the resource that will get updated.
/// * `request` - serde Object with the info for updating the request. If the requests tries
///   to update the resource in a way that deserializing it will no longer be compatible with
///   type T, it fails at runtime.
/// * `condition_checks` - The condition checks that will be added to the transaction item.
/// * `transaction_context` - The transaction context to which the create operation will be added.
///
/// # examples
///
/// ```
/// use serde_json::json;
/// async {
///     let mut context = dynarust::begin_transaction();
///     client.transact_update(
///         &person,
///         json!({ "name": "John", "age":  28 }),
///         vec![dynarust::condition_check_number("age", dynarust::DynamoOperator::Gt, 21)],
///         &mut context
///     )?;
///     client.execute_transaction(context).await?;
/// }
/// ```
pub fn transact_update_with_checks<T: Resource + Serialize + DeserializeOwned>(
    resource: &T,
    request: Value,
    condition_checks: Vec<ConditionCheckInfo>,
    transaction_context: &mut Vec<TransactWriteItem>,
) -> Result<T, DynarustError> {
    let mut object = Client::resource_as_object(resource)?;

    let request: HashMap<String, Value> = serde_json::from_value(request)?;

    for (k, new_v) in request.iter() {
        object[k] = new_v.clone()
    }
    let updated: T = serde_json::from_value(Value::Object(object))?;

    if request.is_empty() {
        return Ok(updated);
    }

    if updated.pk_sk() != resource.pk_sk() {
        return Err(DynarustError::InvalidRequestError(
            "Cannot update PK and/or SK".into(),
        ));
    }

    let condition_check = condition_check_exists().merge(condition_checks);

    let (pk, sk) = resource.pk_sk();
    let mut builder = update::Builder::default()
        .table_name(T::table())
        .key(PK, AttributeValue::S(pk))
        .key(SK, AttributeValue::S(sk));

    let mut update_expression = "set ".to_string();
    let request_len = request.len();
    for (i, (k, v)) in request.into_iter().enumerate() {
        let name = format!("#updateAttr{}", i);
        let value = format!(":updateAttr{}", i);
        update_expression += &format!("{} = {}", name, value);
        if i < request_len - 1 {
            update_expression += ", "
        }
        builder = builder.expression_attribute_names(name, k);
        builder = builder.expression_attribute_values(value, Client::value2attr(&v)?);
    }

    builder = condition_check.dump_in_update(builder);

    let update = builder.update_expression(update_expression).build();
    transaction_context.push(TransactWriteItem::builder().update(update).build());

    Ok(updated)
}

#[cfg(test)]
mod tests {
    use serde_json::json;

    use crate::client::tests::TestResource;
    use crate::condition_check::condition_check_number;
    use crate::create::transact_create;
    use crate::update::transact_update;
    use crate::{begin_transaction, Client, DynamoOperator, Resource};

    #[tokio::test]
    async fn creates_updates_gets_resource() {
        let client = Client::local().await;
        client.create_table::<TestResource>(None).await.unwrap();
        let resource = TestResource {
            pk: "creates_updates_gets_resource".to_string(),
            sk: "1".to_string(),
            string: "asda".to_string(),
            ..Default::default()
        };
        client.create(&resource).await.unwrap();

        let updated = client
            .update(
                &resource,
                json!({
                    "string": "updated",
                    "string_arr": vec!["foo".to_string()]
                }),
            )
            .await
            .unwrap();

        let retrieved = client.get::<TestResource>(resource.pk_sk()).await.unwrap();
        assert_eq!(retrieved, Some(updated))
    }

    #[tokio::test]
    async fn updates_null_field() {
        let client = Client::local().await;
        client.create_table::<TestResource>(None).await.unwrap();
        let resource = TestResource {
            pk: "updates_null_field".to_string(),
            sk: "1".to_string(),
            ..Default::default()
        };
        client.create(&resource).await.unwrap();

        let updated = client
            .update(
                &resource,
                json!({
                    "nullable": "updated"
                }),
            )
            .await
            .unwrap();

        let retrieved = client.get::<TestResource>(resource.pk_sk()).await.unwrap();
        assert_eq!(retrieved, Some(updated))
    }

    #[tokio::test]
    async fn creates_updates_conditional_check_fails() {
        let client = Client::local().await;
        client.create_table::<TestResource>(None).await.unwrap();
        let resource = TestResource {
            pk: "creates_updates_conditional_check_fails".to_string(),
            sk: "1".to_string(),
            string: "asda".to_string(),
            int: 0,
            ..Default::default()
        };
        client.create(&resource).await.unwrap();

        client
            .update_with_checks(
                &resource,
                json!({
                    "int": 1
                }),
                vec![condition_check_number("int", DynamoOperator::NEq, 1)],
            )
            .await
            .unwrap();

        let err = client
            .update_with_checks(
                &resource,
                json!({
                    "int": 2
                }),
                vec![condition_check_number("int", DynamoOperator::NEq, 1)],
            )
            .await
            .unwrap_err();

        assert_eq!(err.to_string(), "The conditional request failed")
    }

    #[tokio::test]
    async fn creates_and_updates_resources_transactionally() {
        let client = Client::local().await;
        client.create_table::<TestResource>(None).await.unwrap();

        let resource_1 = TestResource {
            pk: "creates_and_updates_resources_transactionally".to_string(),
            sk: "1".to_string(),
            ..Default::default()
        };

        client.create(&resource_1).await.unwrap();

        let resource_2 = TestResource {
            pk: "creates_and_updates_resources_transactionally".to_string(),
            sk: "2".to_string(),
            ..Default::default()
        };

        let mut context = begin_transaction();
        let updated_resource_1 = transact_update(
            &resource_1,
            json!({
                "string": "updated"
            }),
            &mut context,
        )
        .unwrap();
        transact_create(&resource_2, &mut context).unwrap();
        client.execute_transaction(context).await.unwrap();

        let retrieved_1 = client
            .get::<TestResource>(updated_resource_1.pk_sk())
            .await
            .unwrap();
        assert_eq!(retrieved_1, Some(updated_resource_1));

        let retrieved_2 = client
            .get::<TestResource>(resource_2.pk_sk())
            .await
            .unwrap();
        assert_eq!(retrieved_2, Some(resource_2))
    }
}
