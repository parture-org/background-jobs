use barrel::backend::Pg;
use barrel::functions::AutogenFunction;
use barrel::{types, Migration};

pub(crate) fn migration() -> String {
    let mut m = Migration::new();

    m.inject_custom("CREATE TYPE job_status AS ENUM ('new', 'running');");
    m.inject_custom("CREATE TYPE retry_strategy AS ENUM ('infinite', 'count');");
    m.inject_custom("CREATE TYPE backoff_strategy AS ENUM ('linear', 'exponential');");

    m.create_table("job_queue", |t| {
        t.inject_custom(r#""id" UUID PRIMARY KEY DEFAULT gen_random_uuid() NOT NULL UNIQUE"#);

        t.add_column("name", types::text().size(128).nullable(false));
        t.add_column("queue", types::text().size(128).nullable(false));
        t.add_column("args", types::custom("jsonb").nullable(false));
        t.add_column("retry_count", types::integer().nullable(false));
        t.add_column("max_retries", types::integer().nullable(false));
        t.add_column("retry", types::custom("retry_strategy").nullable(false));
        t.add_column("backoff_multiplier", types::integer().nullable(false));
        t.add_column("backoff", types::custom("backoff_strategy").nullable(false));
        t.add_column("next_queue", types::datetime().nullable(false));
        t.add_column(
            "heartbeat_interval",
            types::custom("INTERVAL").nullable(false),
        );
        t.add_column(
            "runner_id",
            types::uuid().nullable(true).indexed(false).unique(false),
        );
        t.add_column(
            "status",
            types::custom("job_status").nullable(false).default("new"),
        );
        t.add_column("heartbeat", types::datetime().nullable(true));

        t.add_index("heartbeat_index", types::index(["heartbeat"]));
        t.add_index("next_queue_index", types::index(["next_queue"]));
    });

    m.inject_custom(
        r#"
CREATE OR REPLACE FUNCTION queue_status_notify()
	RETURNS trigger AS
$$
BEGIN
	PERFORM pg_notify('queue_status_channel', NEW.id::text || ' ' || NEW.queue::text);
	RETURN NEW;
END;
$$ LANGUAGE plpgsql;
    "#
        .trim(),
    );

    m.inject_custom(
        r#"
CREATE TRIGGER queue_status
	AFTER INSERT OR UPDATE OF status
	ON job_queue
	FOR EACH ROW
EXECUTE PROCEDURE queue_status_notify();
    "#
        .trim(),
    );

    m.make::<Pg>().to_string()
}
