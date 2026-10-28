# PostgreSQL storage

The generator can store finished conversations in PostgreSQL instead of `output/output.jsonl`.
This is optional: with `database.enabled: false` (the default) the generator writes JSONL files and
never connects to a database.

The application connects to an existing database but does not create its table. Create the role,
the database and the table once, by hand, as described below.

## 1. Create the role and the database

On Linux, run these as the `postgres` system user. With a Homebrew install on macOS, your own user
is the superuser, so leave out `sudo -u postgres`.

```sh
sudo -u postgres createuser -P generator    # prompts for the new role's password
sudo -u postgres createdb -O generator -E UTF8 -T template0 generator
```

## 2. Create the table

Connect as `generator` over TCP with its password, the same way the application connects. A
successful login here also confirms the credentials you will put in the configuration.

```sh
psql -h localhost -U generator -d generator
```

```sql
CREATE TABLE IF NOT EXISTS generated_conversations (
    id SERIAL PRIMARY KEY,
    task_id TEXT UNIQUE NOT NULL,
    conversation_data JSONB NOT NULL,
    api_slot_idx INTEGER,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);
```

The `UNIQUE` constraint on `task_id` also indexes it, so the table needs no other index. Running
the statement again changes nothing; it only prints a notice that the table exists.

## 3. Configure the generator

Set the `database` section of `config/config.yml`, or the same fields in the configuration editor:

```yaml
database:
  enabled: true
  host: localhost
  port: 5432
  dbname: generator
  user: generator
  password: '<the password from step 1>'
  pool_size: 10
```

`psycopg2-binary`, from `requirements.txt`, is the driver.

## How the generator uses the table

- At startup it opens a connection pool. The dashboard's Overview tab shows whether PostgreSQL is
  connected.
- Each finished conversation becomes one row: `conversation_data` holds the record that would
  otherwise be a line of `output.jsonl`. A row whose `task_id` already exists is skipped.
- While the database is enabled, nothing is written to `output.jsonl`. If an insert fails, the
  error is logged to `output/log.txt` and that conversation is not saved anywhere.
- **Export DB → JSONL** writes every row to a JSONL file, oldest first. **Clear Database** empties
  the table.
