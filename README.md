# loggerson

It reads your access logs and does something.

Goals: Idempotent log file reading, small sqlite database.

## TODO:

-

## Notes:

-   SQLite is really slow if you call idempotent upserts in transaction such as: `INSERT INTO ... ON CONFLICT DO UPDATE SET id=id RETURNING id`. It's far better to avoid those and cache the conflicts before inserts.
