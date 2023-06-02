use aws_sdk_dynamodb::model::{delete, AttributeValue, TransactWriteItem};

use crate::condition_check::ConditionCheckInfo;
use crate::dao::{PK, SK};
use crate::{Dao, DynarustError, Resource};

impl Dao {
    pub fn transact_delete<T: Resource>(
        pk: &str,
        sk: &str,
        transaction_context: &mut Vec<TransactWriteItem>,
    ) {
        Self::transact_delete_with_checks::<T>(pk, sk, vec![], transaction_context)
    }

    pub fn transact_delete_with_checks<T: Resource>(
        pk: &str,
        sk: &str,
        condition_checks: Vec<ConditionCheckInfo>,
        transaction_context: &mut Vec<TransactWriteItem>,
    ) {
        let mut delete = delete::Builder::default()
            .table_name(T::table())
            .key(PK, AttributeValue::S(pk.to_string()))
            .key(SK, AttributeValue::S(sk.to_string()));

        delete = ConditionCheckInfo::default()
            .merge(condition_checks)
            .dump_in_delete(delete);

        transaction_context.push(TransactWriteItem::builder().delete(delete.build()).build());
    }

    pub async fn delete<T: Resource>(&self, pk: &str, sk: &str) -> Result<(), DynarustError> {
        self.delete_with_checks::<T>(pk, sk, vec![]).await
    }

    pub async fn delete_with_checks<T: Resource>(
        &self,
        pk: &str,
        sk: &str,
        condition_checks: Vec<ConditionCheckInfo>,
    ) -> Result<(), DynarustError> {
        let mut builder = self
            .client
            .delete_item()
            .table_name(T::table())
            .key(PK, AttributeValue::S(pk.to_string()))
            .key(SK, AttributeValue::S(sk.to_string()));

        builder = ConditionCheckInfo::default()
            .merge(condition_checks)
            .dump_in_delete_item(builder);

        builder.send().await?;

        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use crate::dao::tests::TestResource;
    use crate::{Dao, DynamoOperator, Resource};

    #[tokio::test]
    async fn creates_deletes_gets_resource() {
        let dao = Dao::local().await;
        dao.create_table::<TestResource>(None).await.unwrap();
        let resource = TestResource {
            pk: "creates_deletes_gets_resource".to_string(),
            sk: "1".to_string(),
            ..Default::default()
        };
        dao.create(&resource).await.unwrap();

        let defined = dao
            .get::<TestResource>(&resource.pk(), &resource.sk())
            .await
            .unwrap();

        assert_ne!(defined, None);

        dao.delete::<TestResource>(&resource.pk(), &resource.sk())
            .await
            .unwrap();

        let undefined = dao
            .get::<TestResource>(&resource.pk(), &resource.sk())
            .await
            .unwrap();

        assert_eq!(undefined, None)
    }

    #[tokio::test]
    async fn cannot_delete_if_conditional_check_fails() {
        let dao = Dao::local().await;
        dao.create_table::<TestResource>(None).await.unwrap();
        let resource = TestResource {
            pk: "cannot_delete_if_conditional_check_fails".to_string(),
            sk: "1".to_string(),
            int: 1,
            ..Default::default()
        };
        dao.create(&resource).await.unwrap();

        let err = dao
            .delete_with_checks::<TestResource>(
                &resource.pk(),
                &resource.sk(),
                vec![Dao::condition_check_number("int", DynamoOperator::Gt, 1)],
            )
            .await
            .unwrap_err();

        assert_eq!(err.to_string(), "The conditional request failed")
    }
}
