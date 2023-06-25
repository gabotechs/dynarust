use aws_sdk_dynamodb::model::AttributeValue;
use serde::de::DeserializeOwned;
use serde_json::Value;

use crate::client::{PK, SK};
use crate::{Client, DynarustError, ListOptions, Resource};

impl Client {
    /// List all the resources under the same pk.
    ///
    /// # arguments
    ///
    /// * `pk` - Primary Key under which the listed resources live.
    /// * `options` - optional pagination options.
    ///
    /// # example
    ///
    /// ```
    /// use serde::{Deserialize, Serialize};
    /// use dynarust::ListOptions;
    ///
    /// #[derive(Serialize, Deserialize)]
    /// struct Event {
    ///     id: String,
    ///     timestamp: i64
    /// }
    ///
    /// impl dynarust::Resource for Event {
    ///     fn table() -> String { "Events".into() }
    ///     fn pk_sk(&self) -> (String, String) { (self.id.into(), self.timestamp.to_string()) }
    /// }
    ///
    /// async {
    ///     let client = dynarust::Client::local().await;
    ///     let result = client.list(
    ///         "client-events".into(),
    ///         &ListOptions {
    ///              from: Some("16794003059".into()),
    ///              limit: 100,
    ///              sort_desc: true
    ///         }
    ///     ).await?;
    ///     assert_eq!(result.len(), 100)
    /// }
    /// ```
    pub async fn list<T: Resource + DeserializeOwned>(
        &self,
        pk: String,
        options: &ListOptions,
    ) -> Result<Vec<T>, DynarustError> {
        let scan_index_forward = !options.sort_desc;
        let limit = options.limit;
        let operator = match scan_index_forward {
            true => ">",
            false => "<",
        };
        let sk = match &options.from {
            Some(sk) => sk,
            None => match scan_index_forward {
                true => "+++++++++",   // hehehe
                false => "zzzzzzzzzz", // hohoho
            },
        };

        let result = self
            .client
            .query()
            .table_name(T::table())
            .key_condition_expression(format!("#pk = :pk and #sk {} :sk", operator))
            .expression_attribute_names("#pk", PK)
            .expression_attribute_names("#sk", SK)
            .expression_attribute_values(":pk", AttributeValue::S(pk.to_string()))
            .expression_attribute_values(":sk", AttributeValue::S(sk.to_string()))
            .limit(limit)
            .scan_index_forward(scan_index_forward)
            .send()
            .await?;

        let mut results = vec![];

        if let Some(items) = result.items() {
            for item in items {
                let mut object = Value::Object(serde_json::Map::new());
                for (k, v) in item {
                    object[k] = Self::attr2value(v)?
                }
                let t: T = serde_json::from_value(object)?;
                results.push(t)
            }
        }

        Ok(results)
    }
}

#[cfg(test)]
mod tests {
    use crate::client::tests::TestResource;
    use crate::{Client, ListOptions, Resource};

    #[tokio::test]
    async fn creates_lists_resources() {
        let client = Client::local().await;
        client.create_table::<TestResource>(None).await.unwrap();

        let mut expected = vec![];

        let pk = "creates_lists_resources";
        for i in 0..10 {
            let resource = TestResource {
                pk: pk.to_string(),
                sk: i.to_string(),
                int: i,
                ..Default::default()
            };
            client.create(&resource).await.unwrap();
            expected.push(resource);
        }

        let asc_results = client
            .list::<TestResource>(
                pk.to_string(),
                &ListOptions {
                    limit: 3,
                    ..Default::default()
                },
            )
            .await
            .unwrap();

        assert_eq!(asc_results[0], expected[0]);
        assert_eq!(asc_results[1], expected[1]);
        assert_eq!(asc_results[2], expected[2]);

        let desc_results = client
            .list::<TestResource>(
                pk.to_string(),
                &ListOptions {
                    limit: 3,
                    sort_desc: true,
                    ..Default::default()
                },
            )
            .await
            .unwrap();

        assert_eq!(desc_results[0], expected[9]);
        assert_eq!(desc_results[1], expected[8]);
        assert_eq!(desc_results[2], expected[7]);

        let desc_results_offset = client
            .list::<TestResource>(
                pk.to_string(),
                &ListOptions {
                    limit: 3,
                    sort_desc: true,
                    from: Some(desc_results[2].pk_sk().1),
                },
            )
            .await
            .unwrap();

        assert_eq!(desc_results_offset[0], expected[6]);
        assert_eq!(desc_results_offset[1], expected[5]);
        assert_eq!(desc_results_offset[2], expected[4]);
    }
}
