use std::fmt::Debug;

use aws_sdk_dynamodb::error::{
    BatchGetItemError, CreateTableError, DeleteItemError, GetItemError, PutItemError, QueryError,
    TransactWriteItemsError, UpdateItemError,
};
use aws_sdk_dynamodb::types::SdkError;
use thiserror::Error;

#[derive(Error, Debug)]
pub enum DynarustError {
    #[error("Connection error: could not connect to dynamo")]
    ConnectionError(String),

    #[error("Table already exists error: {0}")]
    TableAlreadyExistsError(String),

    #[error("Unexpected error: {0}")]
    UnexpectedError(String),

    #[error("Invalid request: {0}")]
    InvalidRequestError(String),

    #[error("Attribute parse error: {0}")]
    AttributeParseError(String),

    #[error("Attribute serialize error: {0}")]
    AttributeSerializeError(String),

    #[error("Error while deserializing resource: {0}")]
    ResourceDeserializeError(#[from] serde_json::Error),

    #[error("{0}")]
    DynamoError(String),
}

macro_rules! impl_dynamo_error {
    ($t: ty) => {
        impl From<SdkError<$t>> for DynarustError {
            fn from(value: SdkError<$t>) -> Self {
                if let SdkError::DispatchFailure(_) = value {
                    return DynarustError::ConnectionError("".to_string());
                };
                let service_error = value.into_service_error();
                DynarustError::DynamoError(
                    service_error
                        .message()
                        .unwrap_or("unknown error")
                        .to_string(),
                )
            }
        }
    };
}

impl_dynamo_error!(BatchGetItemError);
impl_dynamo_error!(GetItemError);
impl_dynamo_error!(PutItemError);
impl_dynamo_error!(TransactWriteItemsError);
impl_dynamo_error!(QueryError);
impl_dynamo_error!(UpdateItemError);
impl_dynamo_error!(DeleteItemError);

impl From<SdkError<CreateTableError>> for DynarustError {
    fn from(value: SdkError<CreateTableError>) -> Self {
        if let SdkError::DispatchFailure(_) = value {
            return DynarustError::ConnectionError("".to_string());
        };
        let service_error = value.into_service_error();
        let message = service_error
            .message()
            .unwrap_or("unknown error")
            .to_string();
        if service_error.is_resource_in_use_exception() {
            DynarustError::TableAlreadyExistsError(message)
        } else {
            DynarustError::DynamoError(message)
        }
    }
}
