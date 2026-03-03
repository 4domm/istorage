use axum::{
    http::StatusCode,
    response::{IntoResponse, Response},
    Json,
};
use serde::Serialize;
use thiserror::Error;

#[derive(Error, Debug)]
pub enum ApiError {
    #[error("validation error: {0}")]
    Validation(String),
    #[error("metadata service error: {0}")]
    Metadata(#[from] reqwest::Error),
    #[error("chunk service error: {0}")]
    Chunk(#[from] ChunkServiceError),
    #[error("object not found: {bucket}/{key}")]
    NotFound { bucket: String, key: String },
    #[error("internal error: {0}")]
    Internal(#[from] anyhow::Error),
}

#[derive(Error, Debug)]
pub enum ChunkServiceError {
    #[error("request failed: {0}")]
    Request(#[from] reqwest::Error),
    #[error("chunk not found: {0}")]
    NotFound(String),
}

#[derive(Serialize)]
struct ErrorBody {
    error: String,
}

impl IntoResponse for ApiError {
    fn into_response(self) -> Response {
        let (status, message) = match &self {
            ApiError::Validation(_) => (StatusCode::BAD_REQUEST, self.to_string()),
            ApiError::NotFound { .. } => (StatusCode::NOT_FOUND, self.to_string()),
            ApiError::Metadata(_) | ApiError::Chunk(_) => {
                (StatusCode::BAD_GATEWAY, "upstream service error".into())
            }
            ApiError::Internal(_) => (StatusCode::INTERNAL_SERVER_ERROR, "internal error".into()),
        };
        (status, Json(ErrorBody { error: message })).into_response()
    }
}

impl IntoResponse for ChunkServiceError {
    fn into_response(self) -> Response {
        let status = match &self {
            ChunkServiceError::NotFound(_) => StatusCode::NOT_FOUND,
            ChunkServiceError::Request(_) => StatusCode::BAD_GATEWAY,
        };
        (status, Json(ErrorBody { error: self.to_string() })).into_response()
    }
}
