use aws_sdk_dynamodb::model::AttributeValue;
use serde::de::DeserializeOwned;
use serde_json::Value;

use crate::dao::{PK, SK};
use crate::{Dao, DynarustError, ListOptions, Resource};

impl Dao {
    pub async fn list<T: Resource + DeserializeOwned>(
        &self,
        pk: &str,
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
    use crate::dao::tests::TestResource;
    use crate::{Dao, ListOptions, Resource};

    #[tokio::test]
    async fn creates_lists_resources() {
        let dao = Dao::local().await;
        dao.create_table::<TestResource>(None).await.unwrap();

        let mut expected = vec![];

        let pk = "creates_lists_resources";
        for i in 0..10 {
            let resource = TestResource {
                pk: pk.to_string(),
                sk: i.to_string(),
                int: i,
                ..Default::default()
            };
            dao.create(&resource).await.unwrap();
            expected.push(resource);
        }

        let asc_results = dao
            .list::<TestResource>(
                pk,
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

        let desc_results = dao
            .list::<TestResource>(
                pk,
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

        let desc_results_offset = dao
            .list::<TestResource>(
                pk,
                &ListOptions {
                    limit: 3,
                    sort_desc: true,
                    from: Some(desc_results[2].sk()),
                },
            )
            .await
            .unwrap();

        assert_eq!(desc_results_offset[0], expected[6]);
        assert_eq!(desc_results_offset[1], expected[5]);
        assert_eq!(desc_results_offset[2], expected[4]);
    }
}
