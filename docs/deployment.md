# Configuration and deployment

Configuration precedence is defaults, YAML, then environment variables. The
default file `/etc/seath/config.yaml` is optional. If `CONFIG_PATH` is explicitly
set, that file must exist and parse. Invalid numeric environment values fail
startup. Restart the single service instance to apply configuration changes.

Keep passwords out of YAML and Git. Inject `PG_DSN` from a secret manager. The
`NATS_ADDRESS`, `NATS_USERNAME`, `NATS_PASSWORD`, `TOPIC`, `TABLE`, and `SAMPLE_RATE`
settings apply to the headless command. Console environments and topic mappings
are database-owned; normal startup never imports or overwrites them.

Deploy one replica with `Recreate`. Mount non-secret settings as a ConfigMap;
update a pod annotation or image version when changing it to trigger a rollout.
Expose `/healthz` and `/readyz` for health checks. Allow long-lived SSE responses.
Back up your database privately: it includes descriptors, mappings, recording
data, account hashes, and environment credentials. Do not package it with source.

## SQL console and secrets

The SQL console runs one statement at a time in a read-only transaction. To let
viewers use it, give it a separate login that can read recording tables and
nothing else. Run this once as a database admin (`seath` is the role in
`PG_DSN`):

```sql
CREATE ROLE seath_reader LOGIN PASSWORD '...';
GRANT CONNECT ON DATABASE seath TO seath_reader;
```

Then set `QUERY_PG_DSN` to that login. Seath grants it `SELECT` on each
recording table. At startup, Seath checks the reader and refuses to start if
the reader is the `PG_DSN` role, belongs to any other role (including built-in
`pg_*` roles), is a superuser or has `BYPASSRLS`/`CREATEROLE`, or has any
privilege on `recorder_presets`, `recorder_users`, `recorder_audit` or the other
internal tables. Do not grant the reader anything else. Without
`QUERY_PG_DSN`, only admins can use the SQL console. Viewers can still browse
rows and export CSV; the table page tells them the SQL console is admin-only.

Recording table names must be lower-case letters, digits and underscores
(max 63, not starting with a digit), and must not be one of Seath's own
tables or an existing table that no recording owns. The console folds what you
type to lower case; the API rejects other names with a 400.

Set `SEATH_SECRET_KEY` (from `openssl rand -base64 32`) to encrypt NATS
environment passwords and tokens in the database. On the next start, Seath
encrypts any values that are still plaintext. Keep the key in your secret
manager and back it up: without it, Seath cannot read the stored credentials
and refuses to start. Key rotation is not automated yet. To change the key,
stop Seath, clear the stored credentials with
`UPDATE recorder_presets SET password = '', token = ''`, start Seath with the
new key, then enter the credentials again in Registry. Recordings keep their
environments by name, so they are not affected.

## Explicit instance bootstrap

Create an admin with `seath user add <name>` (password on stdin). Generate the
synthetic example bundle with:

```sh
protoc -I examples --include_imports --descriptor_set_out=examples/events.protoset examples/event.proto
python3 tools/bootstrap.py configs/bootstrap.example.json --url http://127.0.0.1:8080
```

Supply `SEATH_USERNAME` and `SEATH_PASSWORD` through the environment. The command
prints planned counts by default. Add `--apply --activate` for a fresh instance.
For private environments, keep your manifest outside the public repository;
reference credentials using `username_env`, `password_env`, and `token_env`.

Existing module checksums, environment URLs, and topic mappings are checked for
conflicts. Existing credentials are left untouched. Preset creation is atomic
and add-only (`If-None-Match: *`); mappings use the add-only API. An identical
retry skips existing definitions. Errors stop subsequent steps; earlier staged
modules or successful additions remain and are safe to retry. Activation is
explicit and never deletes or rewrites recordings: existing jobs retain their
pinned proto snapshots and only future recordings use the newly active version.
Descriptor validation occurs on the server during import. Bootstrap is an
operator action; do not run it on every application restart.

## Proto snapshot rollout and rollback

Deploy the snapshot-aware image before activating a new module version. Its
first startup additively creates `proto_name` and `proto_module_ids`, then
backfills every legacy recording from the still-active catalogue before serving
requests. Activation is refused if any in-memory recording lacks that snapshot.

For a rollback window, do not create recordings from a newly activated schema
until the image is accepted. If the image must be rolled back during that
window, reactivate the previous module versions first, then restore the previous
image. Once a recording has been created from a new schema, the previous image
is not a safe rollback target because it does not understand pinned snapshots;
roll forward with the snapshot-aware image instead.

## Build and CI

`docker build -t seath:local .` builds without private dependencies. Its build
context allows only application code. The Go binary uses `-trimpath` and
`-buildvcs=false`, excluding checkout paths and private commit metadata.

GitLab CI accepts `GO_PROXY` and `APK_REPOSITORIES` for optional private mirrors.
Optional release variables are `PRIMARY_REGISTRY`, `PRIMARY_IMAGE`,
`PRIMARY_REGISTRY_USER`, `PRIMARY_REGISTRY_PASSWORD`, and the equivalent
`SECONDARY_*` variables. Passwords must be masked CI secrets. A version tag
releases to the primary destination if configured; secondary release is manual.
The public GitHub workflow only tests/builds and has read-only repository access.

## Publication boundary

Package approved committed files from a clean checkout, never the entire working
directory. Keep deployment manifests, descriptors, captures, screenshots from
real instances, agent state and credentials outside the public repository.
Publish only a newly reviewed clean history; private development history and
old binaries are not publication inputs. Run both content and secret scans on
the exact source tree, archive and binary intended for release. Ownership and
license authorization must be established before publishing.
