use aws_sdk_dynamodb::model::{delete, AttributeValue, TransactWriteItem};

use crate::client::{PK, SK};
use crate::condition_check::ConditionCheckInfo;
use crate::{Client, DynarustError, Resource};

impl Client {
    pub fn transact_delete<T: Resource>(
        pk: String,
        sk: String,
        transaction_context: &mut Vec<TransactWriteItem>,
    ) {
        Self::transact_delete_with_checks::<T>(pk, sk, vec![], transaction_context)
    }

    pub fn transact_delete_with_checks<T: Resource>(
        pk: String,
        sk: String,
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

    pub async fn delete<T: Resource>(&self, pk: String, sk: String) -> Result<(), DynarustError> {
        self.delete_with_checks::<T>(pk, sk, vec![]).await
    }

    pub async fn delete_with_checks<T: Resource>(
        &self,
        pk: String,
        sk: String,
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

        let defined = client
            .get::<TestResource>(resource.pk(), resource.sk())
            .await
            .unwrap();

        assert_ne!(defined, None);

        client
            .delete::<TestResource>(resource.pk(), resource.sk())
            .await
            .unwrap();

        let undefined = client
            .get::<TestResource>(resource.pk(), resource.sk())
            .await
            .unwrap();

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
                resource.pk(),
                resource.sk(),
                vec![Client::condition_check_number("int", DynamoOperator::Gt, 1)],
            )
            .await
            .unwrap_err();

        assert_eq!(err.to_string(), "The conditional request failed")
    }
}
