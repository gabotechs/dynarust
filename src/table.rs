use crate::{Client, DynarustError, Resource, PK, SK};
use aws_sdk_dynamodb::model::{
    AttributeDefinition, KeySchemaElement, KeyType, ProvisionedThroughput, ScalarAttributeType,
};

#[derive(Debug, Clone)]
pub struct CreateTableOptions {
    pub read_capacity: i64,
    pub write_capacity: i64,
}

impl Default for CreateTableOptions {
    fn default() -> Self {
        Self {
            read_capacity: 5,
            write_capacity: 5,
        }
    }
}

pub fn create_sam_resource<T: Resource>(maybe_options: Option<CreateTableOptions>) -> String {
    let options = maybe_options.unwrap_or_default();
    let read_capacity = options.read_capacity;
    let write_capacity = options.write_capacity;
    let table_name = T::table();
    format!(
        "\
{table_name}DynamoDBTable:
  Type: AWS::DynamoDB::Table
  Properties:
    TableName: {table_name}
    AttributeDefinitions:
      - AttributeName: {PK}
        AttributeType: S
      - AttributeName: {SK}
        AttributeType: S
    KeySchema:
      - AttributeName: {PK}
        KeyType: HASH
      - AttributeName: {SK}
        KeyType: RANGE
    ProvisionedThroughput:
      ReadCapacityUnits: {read_capacity}
      WriteCapacityUnits: {write_capacity}
"
    )
}

impl Client {
    /// Creates a table in dynamo with the appropriate configuration for resource T
    pub async fn create_table<T: Resource>(
        &self,
        options: Option<CreateTableOptions>,
    ) -> Result<(), DynarustError> {
        let options = options.unwrap_or_default();
        let pk = AttributeDefinition::builder()
            .attribute_name(PK)
            .attribute_type(ScalarAttributeType::S)
            .build();

        let sk = AttributeDefinition::builder()
            .attribute_name(SK)
            .attribute_type(ScalarAttributeType::S)
            .build();

        let ks_pk = KeySchemaElement::builder()
            .attribute_name(PK)
            .key_type(KeyType::Hash)
            .build();

        let ks_sk = KeySchemaElement::builder()
            .attribute_name(SK)
            .key_type(KeyType::Range)
            .build();

        let pt = ProvisionedThroughput::builder()
            .read_capacity_units(options.read_capacity)
            .write_capacity_units(options.write_capacity)
            .build();

        let result = self
            .client
            .create_table()
            .table_name(T::table())
            .attribute_definitions(pk)
            .attribute_definitions(sk)
            .key_schema(ks_pk)
            .key_schema(ks_sk)
            .provisioned_throughput(pt)
            .send()
            .await;

        if let Err(err) = result {
            let err: DynarustError = err.into();
            if let DynarustError::TableAlreadyExistsError(_) = err {
                Ok(())
            } else {
                Err(err)
            }
        } else {
            Ok(())
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::client::tests::TestResource;

    #[tokio::test]
    async fn test_no_connection_to_dynamo() {
        let client = Client::local_on_port(12345).await;
        let err = client
            .create_table::<TestResource>(Some(CreateTableOptions {
                read_capacity: 0,
                write_capacity: 0,
            }))
            .await
            .unwrap_err();
        assert_eq!(
            format!("{err}"),
            "Connection error: could not connect to dynamo"
        )
    }
}
