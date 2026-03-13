# Kestra MongoDB Plugin

## What

Access and manipulate MongoDB data within Kestra workflows. Exposes 8 plugin components (tasks, triggers, and/or conditions).

## Why

Enables Kestra workflows to interact with MongoDB, allowing orchestration of MongoDB-based operations as part of data pipelines and automation workflows.

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

### Important Commands

```bash
# Build the plugin
./gradlew shadowJar

# Run tests
./gradlew test

# Build without tests
./gradlew shadowJar -x test
```

### Configuration

All tasks and triggers accept standard Kestra plugin properties. Credentials should use
`{{ secret('SECRET_NAME') }}` — never hardcode real values.

## Agents

**IMPORTANT:** This is a Kestra plugin repository (prefixed by `plugin-`, `storage-`, or `secret-`). You **MUST** delegate all coding tasks to the `kestra-plugin-developer` agent. Do NOT implement code changes directly — always use this agent.
