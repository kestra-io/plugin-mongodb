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

Infrastructure dependencies (Docker Compose services):

- `mongo`

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

## References

- https://kestra.io/docs/plugin-developer-guide
- https://kestra.io/docs/plugin-developer-guide/contribution-guidelines
