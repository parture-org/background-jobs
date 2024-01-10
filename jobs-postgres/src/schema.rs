// @generated automatically by Diesel CLI.

pub mod sql_types {
    #[derive(diesel::query_builder::QueryId, diesel::sql_types::SqlType)]
    #[diesel(postgres_type(name = "backoff_strategy"))]
    pub struct BackoffStrategy;

    #[derive(diesel::query_builder::QueryId, diesel::sql_types::SqlType)]
    #[diesel(postgres_type(name = "job_status"))]
    pub struct JobStatus;

    #[derive(diesel::query_builder::QueryId, diesel::sql_types::SqlType)]
    #[diesel(postgres_type(name = "retry_strategy"))]
    pub struct RetryStrategy;
}

diesel::table! {
    use diesel::sql_types::*;
    use super::sql_types::RetryStrategy;
    use super::sql_types::BackoffStrategy;
    use super::sql_types::JobStatus;

    job_queue (id) {
        id -> Uuid,
        name -> Text,
        queue -> Text,
        args -> Jsonb,
        retry_count -> Int4,
        max_retries -> Int4,
        retry -> RetryStrategy,
        backoff_multiplier -> Int4,
        backoff -> BackoffStrategy,
        next_queue -> Timestamp,
        heartbeat_interval -> Interval,
        runner_id -> Nullable<Uuid>,
        status -> JobStatus,
        heartbeat -> Nullable<Timestamp>,
    }
}

diesel::table! {
    refinery_schema_history (version) {
        version -> Int4,
        #[max_length = 255]
        name -> Nullable<Varchar>,
        #[max_length = 255]
        applied_on -> Nullable<Varchar>,
        #[max_length = 255]
        checksum -> Nullable<Varchar>,
    }
}

diesel::allow_tables_to_appear_in_same_query!(
    job_queue,
    refinery_schema_history,
);
