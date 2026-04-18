# Kestra MongoDB Plugin

## What

- Provides plugin components under `io.kestra.plugin.mongodb`.
- Includes classes such as `InsertOne`, `MongoDbConnection`, `Delete`, `MongoDbService`.

## Why

- This plugin integrates Kestra with MongoDB.
- It provides tasks that query and manipulate MongoDB collections.

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
