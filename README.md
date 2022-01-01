# loggerson

It reads your access logs and does something.

Goals: Idempotent log file reading, small sqlite database.

## Queries

All users by duration:

```sql
select (MAX(timestamp) - MIN(timestamp))/(3600*24) as duration, COUNT(*) as cnt, ua.value from entrys e, users u, useragents ua where e.user_id = u.id AND u.useragent_id = ua.id GROUP BY u.id ORDER BY duration DESC
```

## TODO:

-   Ability to clear old user hashes (so they become impossible to reverse even
    in theory)

## Notes:

-   SQLite is really slow if you call idempotent upserts in transaction such as:
    `INSERT INTO ... ON CONFLICT DO UPDATE SET id=id RETURNING id`. It's far
    better to avoid those and cache the conflicts before inserts.

-   User hash is string, it takes less space as binary, but is it worth it?

-   Weird thought, since hashing just IP is not proper anonymization, because
    you can reverse it with just rainbow tables. But how about using bad hasher,
    like md5 and hashing IP+Useragent in same hash? This means there is
    inifinite amount of collisions (because it's broken has), thus the hash is
    GDPR safe?

-
