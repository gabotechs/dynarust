use std::collections::HashMap;

use aws_sdk_dynamodb::model::{AttributeValue, KeysAndAttributes};
use serde::de::DeserializeOwned;
use serde_json::Value;

use crate::client::{PK, SK};
use crate::{Client, DynarustError, Resource};

impl Client {
    /// Retrieves a resource. If the resource does not exist returns Option::None.
    ///
    /// # arguments
    /// * `pk_sk` - Pk and sk pair for identifying the resource
    pub async fn get<T: Resource + DeserializeOwned>(
        &self,
        (pk, sk): (String, String),
    ) -> Result<Option<T>, DynarustError> {
        let result = self
            .client
            .get_item()
            .table_name(T::table())
            .key(PK, AttributeValue::S(pk))
            .key(SK, AttributeValue::S(sk))
            .send()
            .await?;

        if let Some(item) = result.item() {
            let mut object = Value::Object(serde_json::Map::new());
            for (k, v) in item {
                object[k] = Self::attr2value(v)?
            }
            let t: T = serde_json::from_value(object)?;
            Ok(Some(t))
        } else {
            Ok(None)
        }
    }

    /// Retrieves multiple resource in the same operation. If one of the resources do not exist
    /// it will not be present in the resulting HashMap.
    ///
    /// # arguments
    /// * `items` - Array of pk and sk pairs identifying the resource that will be retrieved.
    pub async fn batch_get<T: Resource + DeserializeOwned>(
        &self,
        items: Vec<(String, String)>,
    ) -> Result<HashMap<(String, String), T>, DynarustError> {
        let mut builder = KeysAndAttributes::builder();

        for (pk, sk) in items {
            builder = builder.keys(HashMap::from([
                (PK.to_string(), AttributeValue::S(pk)),
                (SK.to_string(), AttributeValue::S(sk)),
            ]))
        }

        let result = self
            .client
            .batch_get_item()
            .request_items(T::table(), builder.build())
            .send()
            .await?;

        let mut resources = HashMap::new();

        if let Some(responses) = result.responses() {
            let responses = responses.get(&T::table()).ok_or_else(|| {
                DynarustError::UnexpectedError(
                    "Table was not returned in that batch items response".to_string(),
                )
            })?;

            for item in responses {
                let mut object = Value::Object(serde_json::Map::new());
                for (k, v) in item {
                    object[k] = Self::attr2value(v)?
                }
                let t: T = serde_json::from_value(object)?;
                resources.insert(t.pk_sk(), t);
            }
        } else {
            return Ok(HashMap::new());
        }

        Ok(resources)
    }
}

#[cfg(test)]
mod tests {
    use crate::client::tests::TestResource;
    use crate::{Client, Resource};

    #[tokio::test]
    async fn creates_and_gets_resource() {
        let client = Client::local().await;
        client.create_table::<TestResource>(None).await.unwrap();
        let resource = TestResource {
            pk: "creates_and_gets_resource".to_string(),
            sk: "1".to_string(),
            string: "asda".to_string(),
            ..Default::default()
        };

        client.create(&resource).await.unwrap();
        let retrieved = client.get::<TestResource>(resource.pk_sk()).await.unwrap();
        assert_eq!(retrieved, Some(resource))
    }

    #[tokio::test]
    async fn creates_and_batch_gets_resource() {
        let client = Client::local().await;
        client.create_table::<TestResource>(None).await.unwrap();

        let pk = "creates_and_batch_gets_resource".to_string();

        for i in 0..3 {
            let resource = TestResource {
                pk: pk.clone(),
                sk: i.to_string(),
                int: i,
                ..Default::default()
            };
            client.create(&resource).await.unwrap();
        }

        let retrieved = client
            .batch_get::<TestResource>(vec![
                (pk.clone(), 0.to_string()),
                (pk.clone(), 1.to_string()),
                (pk.clone(), 2.to_string()),
            ])
            .await
            .unwrap();
        assert_eq!(retrieved.len(), 3);
        assert_eq!(retrieved[&(pk.clone(), "0".to_string())].int, 0);
        assert_eq!(retrieved[&(pk.clone(), "1".to_string())].int, 1);
        assert_eq!(retrieved[&(pk.clone(), "2".to_string())].int, 2);
    }

    #[tokio::test]
    async fn batch_gets_empty() {
        let client = Client::local().await;
        client.create_table::<TestResource>(None).await.unwrap();

        let err = client.batch_get::<TestResource>(vec![]).await.unwrap_err();

        assert!(err
            .to_string()
            .starts_with("The list of keys in RequestItems for BatchGetItem is required"))
    }
}
