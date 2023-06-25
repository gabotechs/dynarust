use aws_sdk_dynamodb::model::{delete, AttributeValue, TransactWriteItem};

use crate::client::{PK, SK};
use crate::condition_check::ConditionCheckInfo;
use crate::{Client, DynarustError, Resource};

/// Adds a delete operation to a transaction context.
///
/// # arguments
/// * `pk_sk` - The pk and sk pair for identifying the resource.
/// * `transaction_context` - The transaction context to which the delete operation will be added.
pub fn transact_delete<T: Resource>(
    pk_sk: (String, String),
    transaction_context: &mut Vec<TransactWriteItem>,
) {
    transact_delete_with_checks::<T>(pk_sk, vec![], transaction_context)
}

/// Adds a delete operation to a transaction context, with some conditional checks referencing
/// the resource that is being deleted.
///
/// # arguments
/// * `pk_sk` - The pk and sk pair for identifying the resource.
/// * `condition_checks` - The condition checks that will be added to the transaction item.
/// * `transaction_context` - The transaction context to which the delete operation will be added.
pub fn transact_delete_with_checks<T: Resource>(
    (pk, sk): (String, String),
    condition_checks: Vec<ConditionCheckInfo>,
    transaction_context: &mut Vec<TransactWriteItem>,
) {
    let mut delete = delete::Builder::default()
        .table_name(T::table())
        .key(PK, AttributeValue::S(pk))
        .key(SK, AttributeValue::S(sk));

    delete = ConditionCheckInfo::default()
        .merge(condition_checks)
        .dump_in_delete(delete);

    transaction_context.push(TransactWriteItem::builder().delete(delete.build()).build());
}

impl Client {
    /// Deletes a resource.
    ///
    /// # arguments
    ///
    /// * `pk_sk` - Pk and sk pair for identifying the resource that will get deleted.
    pub async fn delete<T: Resource>(&self, pk_sk: (String, String)) -> Result<(), DynarustError> {
        self.delete_with_checks::<T>(pk_sk, vec![]).await
    }

    /// Deletes a resource with additional condition checks.
    ///
    /// # arguments
    ///
    /// * `pk_sk` - Pk and sk pair for identifying the resource that will get deleted.
    /// * `condition_checks` - The condition checks that will be added to the transaction item.
    pub async fn delete_with_checks<T: Resource>(
        &self,
        (pk, sk): (String, String),
        condition_checks: Vec<ConditionCheckInfo>,
    ) -> Result<(), DynarustError> {
        let mut builder = self
            .client
            .delete_item()
            .table_name(T::table())
            .key(PK, AttributeValue::S(pk))
            .key(SK, AttributeValue::S(sk));

        builder = ConditionCheckInfo::default()
            .merge(condition_checks)
            .dump_in_delete_item(builder);

        builder.send().await?;

        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use crate::client::tests::TestResource;
    use crate::condition_check::condition_check_number;
    use crate::{Client, DynamoOperator, Resource};

    #[tokio::test]
    async fn creates_deletes_gets_resource() {
        let client = Client::local().await;
        client.create_table::<TestResource>(None).await.unwrap();
        let resource = TestResource {
            pk: "creates_deletes_gets_resource".to_string(),
            sk: "1".to_string(),
            ..Default::default()
        };
        client.create(&resource).await.unwrap();

        let defined = client.get::<TestResource>(resource.pk_sk()).await.unwrap();

        assert_ne!(defined, None);

        client
            .delete::<TestResource>(resource.pk_sk())
            .await
            .unwrap();

        let undefined = client.get::<TestResource>(resource.pk_sk()).await.unwrap();

        assert_eq!(undefined, None)
    }

    #[tokio::test]
    async fn cannot_delete_if_conditional_check_fails() {
        let client = Client::local().await;
        client.create_table::<TestResource>(None).await.unwrap();
        let resource = TestResource {
            pk: "cannot_delete_if_conditional_check_fails".to_string(),
            sk: "1".to_string(),
            int: 1,
            ..Default::default()
        };
        client.create(&resource).await.unwrap();

        let err = client
            .delete_with_checks::<TestResource>(
                resource.pk_sk(),
                vec![condition_check_number("int", DynamoOperator::Gt, 1)],
            )
            .await
            .unwrap_err();

        assert_eq!(err.to_string(), "The conditional request failed")
    }
}
