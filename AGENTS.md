# Kestra MongoDB Plugin

## What

- Provides plugin components under `io.kestra.plugin.mongodb`.
- Includes classes such as `InsertOne`, `MongoDbConnection`, `Delete`, `MongoDbService`.

## Why

- What user problem does this solve? Teams need to query and manipulate MongoDB collections from orchestrated workflows instead of relying on manual console work, ad hoc scripts, or disconnected schedulers.
- Why would a team adopt this plugin in a workflow? It keeps MongoDB steps in the same Kestra flow as upstream preparation, approvals, retries, notifications, and downstream systems.
- What operational/business outcome does it enable? It reduces manual handoffs and fragmented tooling while improving reliability, traceability, and delivery speed for processes that depend on MongoDB.

## How

### Architecture

Single-module plugin. Source packages under `io.kestra.plugin`:

- `mongodb`

Infrastructure dependencies: tests start MongoDB with Testcontainers (`mongo:8.0`), so no Docker Compose service is needed.

### Key Plugin Classes

- `io.kestra.plugin.mongodb.Aggregate`
- `io.kestra.plugin.mongodb.Bulk`
- `io.kestra.plugin.mongodb.Delete`
- `io.kestra.plugin.mongodb.Find`
- `io.kestra.plugin.mongodb.InsertOne`
- `io.kestra.plugin.mongodb.Load`
- `io.kestra.plugin.mongodb.Trigger`
- `io.kestra.plugin.mongodb.Update`

### Project Structure

```
plugin-mongodb/
├── src/main/java/io/kestra/plugin/mongodb/
├── src/test/java/io/kestra/plugin/mongodb/
├── build.gradle
└── README.md
```

## Local rules

Follow the Kestra plugin contribution guidelines. These are the rules that sent the Data Studio plugin back for another review:

- Rendered values use the `r` prefix (`rFilter`), never `renderedFilter`, and must not shadow the field they come from.
- A bound on a `Property` is `Property<@Min(1) Integer>`. `@Min` on the `Property` field fails validation.
- A new property with a fixed set of values is an enum. A fallback such as `UNKNOWN` is output-only and is rejected on input.
- Required, primary properties use group `main`. `source` is for where data comes from. A safety opt-in uses `reliability`.
- Timestamps in outputs are `Instant`. Descriptions match the real behavior, including flags whose API meaning is exclusive.
- Large results use `STORE`. `FETCH_ONE` returns one row and does not also return the full list. An output that grows with the input has a documented cap; `size` keeps the full count.
- An HTTP client sets a connect timeout and a read idle timeout. Decode error bodies as text. An empty token body fails before it is parsed.
- Plugin how-to docs start with `# How to use the … plugin`, then `## Authentication` (secrets and plugin defaults, with links), `## Tasks`, and `## Triggers`.
- One happy-path test per task. Also cover a failure, a bound of `0`, and one Pebble render, including a list. Do not chain several tasks in one test method.
- Secrets use `secret = true` and `@ToString.Exclude`. Log with `runContext.logger()`. Serialize with `JacksonMapper`. Examples are `full = true` and use `{{ secret() }}`.
- Every property has `@Schema` and a property group. Use the official icon. Do not invent a logo.
- A comment states only why, in one sentence. Do not restate the code.
- Change only the code the issue needs. Do not close the issue until the pull request merges. Do not commit secrets or edit the Gradle wrapper.

`JacksonMapper.ofJson()` omits null map entries. `MongoDbService.toDocument` must keep them, because `{deletedAt: null}` would otherwise become `{}` and `Delete` would match every document. Nulls inside arrays already survive. Kestra core can also drop a null while serializing the task before the worker; that half is a core report, not a change in this repository.

## References

- https://kestra.io/docs/plugin-developer-guide
- https://kestra.io/docs/plugin-developer-guide/contribution-guidelines
