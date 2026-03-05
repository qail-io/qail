use super::ApiError;
use axum::{
    Json,
    response::{IntoResponse, Response},
};

impl IntoResponse for ApiError {
    fn into_response(self) -> Response {
        let status = self.status_code();
        let is_rate_limited = self.code == "RATE_LIMITED";
        let is_pool_backpressure = self.code == "POOL_BACKPRESSURE";
        let request_id = self.request_id.clone();
        let message = self.message.clone();
        let mut response = (status, Json(self)).into_response();

        if is_rate_limited && let Ok(v) = "1".parse() {
            response.headers_mut().insert("retry-after", v);
        }

        if is_pool_backpressure {
            let (scope, reason, retry_after_secs) =
                crate::db_backpressure::backpressure_response_metadata(&message);

            if let Ok(v) = retry_after_secs.to_string().parse() {
                response.headers_mut().insert("retry-after", v);
            }
            if let Ok(v) = scope.parse() {
                response
                    .headers_mut()
                    .insert("x-qail-backpressure-scope", v);
            }
            if let Ok(v) = reason.parse() {
                response
                    .headers_mut()
                    .insert("x-qail-backpressure-reason", v);
            }
        }

        if let Some(ref id) = request_id
            && let Ok(v) = id.parse()
        {
            response.headers_mut().insert("x-request-id", v);
        }

        response
    }
}
