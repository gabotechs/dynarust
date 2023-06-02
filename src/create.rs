use aws_sdk_dynamodb::model::{put, AttributeValue, TransactWriteItem};
use serde::Serialize;

use crate::condition_check::ConditionCheckInfo;
use crate::{Dao, DynarustError, Resource};

impl Dao {
    pub fn transact_create<'a, T: Resource + Serialize>(
        resource: &'a T,
        transaction_context: &mut Vec<TransactWriteItem>,
    ) -> Result<&'a T, DynarustError> {
        Self::transact_create_with_checks(resource, vec![], transaction_context)
    }

    pub fn transact_create_with_checks<'a, T: Resource + Serialize>(
        resource: &'a T,
        condition_checks: Vec<ConditionCheckInfo>,
        transaction_context: &mut Vec<TransactWriteItem>,
    ) -> Result<&'a T, DynarustError> {
        let object = Self::resource_as_object(resource)?;

        let mut builder = put::Builder::default().table_name(T::table());

        for (k, v) in object {
            builder = builder.item(k, Self::value2attr(&v)?)
        }

        let condition_checks = Self::condition_check_not_exists().merge(condition_checks);

        let mut put = builder
            .item(crate::dao::PK, AttributeValue::S(resource.pk()))
            .item(crate::dao::SK, AttributeValue::S(resource.sk()));

        put = condition_checks.dump_in_put(put);

        transaction_context.push(TransactWriteItem::builder().put(put.build()).build());
        Ok(resource)
    }

    pub async fn create<'a, T: Resource + Serialize>(
        &self,
        resource: &'a T,
    ) -> Result<&'a T, DynarustError> {
        self.create_with_checks(resource, vec![]).await
    }

    pub async fn create_with_checks<'a, T: Resource + Serialize>(
        &self,
        resource: &'a T,
        condition_checks: Vec<ConditionCheckInfo>,
    ) -> Result<&'a T, DynarustError> {
        let object = Self::resource_as_object(resource)?;

        let mut builder = self.client.put_item().table_name(T::table());

        for (k, v) in object {
            builder = builder.item(k, Self::value2attr(&v)?)
        }

        let condition_checks = Self::condition_check_not_exists().merge(condition_checks);

        builder = condition_checks.dump_in_put_item(builder);

        builder
            .item(crate::dao::PK, AttributeValue::S(resource.pk()))
            .item(crate::dao::SK, AttributeValue::S(resource.sk()))
            .send()
            .await?;

        Ok(resource)
    }

    /// creates the resource even if it previously existed, replacing the original
    pub async fn force_create<'a, T: Resource + Serialize>(
        &self,
        resource: &'a T,
    ) -> Result<&'a T, DynarustError> {
        let object = Self::resource_as_object(resource)?;

        let mut builder = self.client.put_item().table_name(T::table());
        for (k, v) in object {
            builder = builder.item(k, Self::value2attr(&v)?)
        }
        builder
            .item(crate::dao::PK, AttributeValue::S(resource.pk()))
            .item(crate::dao::SK, AttributeValue::S(resource.sk()))
            .send()
            .await?;

        Ok(resource)
    }
}

#[cfg(test)]
mod tests {
    use crate::dao::tests::TestResource;
    use crate::{Dao, Resource};

    #[tokio::test]
    async fn is_able_to_create_table() {
        let dao = Dao::local().await;
        dao.create_table::<TestResource>(None).await.unwrap();
    }

    #[tokio::test]
    async fn supports_nullable_values() {
        let dao = Dao::local().await;
        dao.create_table::<TestResource>(None).await.unwrap();

        let resource_1 = TestResource {
            pk: "supports_nullable_values".to_string(),
            sk: "1".to_string(),
            nullable: Some("This is something".to_string()),
            ..Default::default()
        };

        let resource_2 = TestResource {
            pk: "supports_nullable_values".to_string(),
            sk: "2".to_string(),
            ..Default::default()
        };

        let created_1 = dao.create(&resource_1).await.unwrap();
        let created_2 = dao.create(&resource_2).await.unwrap();
        assert_eq!(created_1.nullable, resource_1.nullable);
        assert_eq!(created_2.nullable, resource_2.nullable)
    }

    #[tokio::test]
    async fn supports_array_values() {
        let dao = Dao::local().await;
        dao.create_table::<TestResource>(None).await.unwrap();

        let resource_1 = TestResource {
            pk: "supports_array_values".to_string(),
            sk: "1".to_string(),
            string_arr: vec!["foo".to_string(), "bar".to_string()],
            ..Default::default()
        };

        let created_1 = dao.create(&resource_1).await.unwrap();
        assert_eq!(created_1.string_arr, resource_1.string_arr);
    }

    #[tokio::test]
    async fn creates_two_resources_transactionally() {
        let dao = Dao::local().await;
        dao.create_table::<TestResource>(None).await.unwrap();

        let resource_1 = TestResource {
            pk: "creates_two_resources_transactionally".to_string(),
            sk: "1".to_string(),
            ..Default::default()
        };

        let resource_2 = TestResource {
            pk: "creates_two_resources_transactionally".to_string(),
            sk: "2".to_string(),
            ..Default::default()
        };

        let mut context = Dao::begin_transaction();
        Dao::transact_create(&resource_1, &mut context).unwrap();
        Dao::transact_create(&resource_2, &mut context).unwrap();
        dao.execute_transaction(context).await.unwrap();

        let retrieved_1 = dao
            .get::<TestResource>(&resource_1.pk(), &resource_1.sk())
            .await
            .unwrap();
        assert_eq!(retrieved_1, Some(resource_1));

        let retrieved_2 = dao
            .get::<TestResource>(&resource_2.pk(), &resource_2.sk())
            .await
            .unwrap();
        assert_eq!(retrieved_2, Some(resource_2))
    }
}
