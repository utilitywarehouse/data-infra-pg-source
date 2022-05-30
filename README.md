# data-infra-pg-source

Reads the configured postgres table, converts and validates each row against the configured data product schema, creates and pushes the data product parquest schema to google storage.
Runs on a cron schedule.

See [manifests](manifests/base) for configuration details. 

| Data Points type | Postgres type |
|---------------|---------------|
| BOOLEAN | boolean |
| INT | bigint |
| DOUBLE | double precision |
| DECIMAL | numeric(18,2) |
| STRING | text |
| UUID  | text |
| DATE | date |
| TIMESTAMP | timestamp | 
| BYTE_ARRAY | bytea |
| ARRAY | jsonb |
| OBJECT | jsonb |
