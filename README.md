# spring-scylla-jpa

[![Status](https://img.shields.io/badge/status-alpha-orange)](https://github.com/saivnct/spring-scylla-jpa)
[![Java](https://img.shields.io/badge/java-17%2B-blue)](https://github.com/saivnct/spring-scylla-jpa)
[![Maven](https://img.shields.io/badge/build-maven-informational)](https://maven.apache.org/)
[![License](https://img.shields.io/github/license/saivnct/spring-scylla-jpa)](https://github.com/saivnct/spring-scylla-jpa/blob/main/LICENSE)
[![Stars](https://img.shields.io/github/stars/saivnct/spring-scylla-jpa?style=social)](https://github.com/saivnct/spring-scylla-jpa/stargazers)
[![Issues](https://img.shields.io/github/issues/saivnct/spring-scylla-jpa)](https://github.com/saivnct/spring-scylla-jpa/issues)
[![Last Commit](https://img.shields.io/github/last-commit/saivnct/spring-scylla-jpa)](https://github.com/saivnct/spring-scylla-jpa/commits/main)

A Spring-oriented data access library for ScyllaDB that combines:
- low-level control from the Scylla Java Driver,
- Spring-style mapping and conversion,
- repository-style CRUD/paging APIs,
- schema lifecycle automation for tables, indexes, and UDTs.

The project is built on top of the [Scylla Java Driver for Scylla and Apache Cassandra](https://github.com/scylladb/java-driver).

## Table of contents
- [Why this library](#why-this-library)
- [Key capabilities](#key-capabilities)
- [How it compares](#how-it-compares)
- [Architecture](#architecture)
- [Version compatibility](#version-compatibility)
- [Maven dependency](#maven-dependency)
- [Quick start](#quick-start)
- [Schema lifecycle](#schema-lifecycle)
- [Async programming model](#async-programming-model)
- [Integration testing (Testcontainers)](#integration-testing-testcontainers)
- [Build from source](#build-from-source)
- [Roadmap](#roadmap)
- [Contributing](#contributing)
- [License](#license)

## Why this library

`spring-scylla-jpa` targets teams that want ScyllaDB performance with a safer and more productive Java/Spring developer experience.

Core goals:
- reduce boilerplate in session setup and entity mapping,
- reduce runtime mapping errors through metadata-driven conversion,
- provide a consistent repository abstraction for sync and async access,
- support controlled schema initialization during application lifecycle.

## Key capabilities

- Scylla Java Driver integration (including shard-aware driver capabilities).
- Mapping context for table entities, tuple types, and user-defined types (UDTs).
- Automatic schema actions:
  - create table(s)
  - create index(es)
  - create UDT(s)
- Repository abstraction with:
  - single/bulk save
  - conditional update (`IF EXISTS`)
  - partition-key and primary-key reads
  - delete/count operations
  - async variants (`CompletionStage`)
- Centralized execution API (`ScyllaTemplate`) for statement execution and mapping.
- Exception translation into Spring `DataAccessException` hierarchy.

## How it compares

| Area | spring-scylla-jpa | Spring Data Cassandra |
|---|---|---|
| Primary optimization target | Scylla-first runtime behavior and mapping workflow | Broad Cassandra ecosystem support |
| Driver foundation | Scylla Java Driver | Apache Cassandra Java Driver |
| Repository style | Generic repository classes (`SimpleScyllaRepository`, paging variant) | Spring Data repository interfaces and query derivation |
| Schema lifecycle integration | Built-in schema action orchestration in configuration/factory beans | Supported via Spring Data Cassandra schema APIs and templates |
| Mapping coverage | Tables, tuple types, UDTs with custom converters | Rich mapping model with Spring Data ecosystem integration |

Note: choose based on operational target. If your primary runtime is ScyllaDB and you want a Scylla-focused abstraction layer, this project aims to reduce integration friction.

## Architecture

- `com.giangbb.scylla.config`
  - session configuration and factory beans
  - schema/keyspace lifecycle wiring
- `com.giangbb.scylla.core.mapping`
  - entity metadata model and verification
- `com.giangbb.scylla.core.convert`
  - object <-> row/UDT/tuple conversion
- `com.giangbb.scylla.core`
  - template and schema create/drop orchestration
- `com.giangbb.scylla.repository`
  - generic repository implementations

## Version compatibility

| Component | Recommended baseline |
|---|---|
| Java | 17+ |
| Spring Boot | 3.2.x |
| Scylla Java Driver | 4.17.0.0 |
| spring-scylla-jpa | 0.2.0 |

Notes:
- Keep Java runtime and build toolchain aligned (`mvn -version` should report Java 17+).
- Pin `spring-scylla-jpa` and Scylla driver versions in production environments.

## Maven dependency

```xml
<dependency>
  <groupId>com.giangbb</groupId>
  <artifactId>scylla-jpa</artifactId>
  <version>0.2.0</version>
</dependency>
```

## Quick start

### 1) Configure Scylla session + mapping

```java
import com.giangbb.scylla.config.AbstractScyllaConfiguration;
import com.giangbb.scylla.config.SchemaAction;
import org.springframework.context.annotation.Configuration;
import org.springframework.lang.NonNull;

@Configuration
public class ScyllaConfiguration extends AbstractScyllaConfiguration {

    @Override
    protected String getKeyspaceName() {
        return "springdemo";
    }

    @Override
    @NonNull
    protected String getLocalDataCenter() {
        return "datacenter1";
    }

    @Override
    protected String getContactPoints() {
        return "127.0.0.1";
    }

    @Override
    public SchemaAction getSchemaAction() {
        return SchemaAction.CREATE_IF_NOT_EXISTS;
    }

    @Override
    public String[] getEntityBasePackages() {
        return new String[] { "com.example.demo.entity" };
    }
}
```

### 2) Define an entity

```java
import com.datastax.oss.driver.api.mapper.annotations.ClusteringColumn;
import com.datastax.oss.driver.api.mapper.annotations.PartitionKey;
import com.giangbb.scylla.core.mapping.Indexed;
import com.giangbb.scylla.core.mapping.Table;

import java.util.UUID;

@Table
public class User {

    @PartitionKey
    private UUID id;

    @ClusteringColumn
    private int userAge;

    @Indexed
    private String userName;

    public User() {}

    public User(UUID id, int userAge, String userName) {
        this.id = id;
        this.userAge = userAge;
        this.userName = userName;
    }
}
```

### 3) Create a repository

```java
import com.giangbb.scylla.core.ScyllaTemplate;
import com.giangbb.scylla.repository.SimpleScyllaRepository;
import org.springframework.stereotype.Component;

@Component
public class UserRepository extends SimpleScyllaRepository<User> {

    public UserRepository(ScyllaTemplate scyllaTemplate) {
        super(User.class, scyllaTemplate);
    }
}
```

### 4) Use repository APIs

```java
UUID id = UUID.randomUUID();
userRepository.save(new User(id, 20, "alice"));

Map<CqlIdentifier, Object> pk = new LinkedHashMap<>();
pk.put(CqlIdentifier.fromCql("id"), id);
pk.put(CqlIdentifier.fromCql("userage"), 20);

User user = userRepository.findByPrimaryKey(pk);
long count = userRepository.countAll();
```

## Schema lifecycle

Schema behavior is controlled by `SchemaAction` in your configuration.

Typical options:
- `NONE`: no schema changes on startup.
- `CREATE`: create mapped schema objects.
- `CREATE_IF_NOT_EXISTS`: safe create with `IF NOT EXISTS`.
- `RECREATE`: drop/recreate mapped schema.
- `RECREATE_DROP_UNUSED`: drop/recreate and remove unused mapped schema.

## Async programming model

Most repository operations expose async counterparts returning `CompletionStage<T>`, enabling non-blocking integration with reactive pipelines or custom executors.

Examples:
- `saveAsync(...)`
- `findByPrimaryKeyAsync(...)`
- `findAllAsync()`
- `countAllAsync()`

## Integration testing (Testcontainers)

Below is a copy-paste friendly integration test template using JUnit 5 + Testcontainers + Spring context bootstrapping.

### Test dependencies

```xml
<dependency>
  <groupId>org.testcontainers</groupId>
  <artifactId>scylla</artifactId>
  <version>1.21.3</version>
  <scope>test</scope>
</dependency>
<dependency>
  <groupId>org.junit.jupiter</groupId>
  <artifactId>junit-jupiter</artifactId>
  <scope>test</scope>
</dependency>
```

### Example integration test

```java
import com.datastax.oss.driver.api.core.CqlIdentifier;
import com.giangbb.scylla.config.SchemaAction;
import com.giangbb.scylla.config.AbstractScyllaConfiguration;
import com.giangbb.scylla.core.ScyllaTemplate;
import com.giangbb.scylla.core.cql.keyspace.CreateKeyspaceSpecification;
import com.giangbb.scylla.core.mapping.Table;
import com.giangbb.scylla.repository.SimpleScyllaRepository;
import com.datastax.oss.driver.api.mapper.annotations.ClusteringColumn;
import com.datastax.oss.driver.api.mapper.annotations.PartitionKey;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.Test;
import org.springframework.context.annotation.AnnotationConfigApplicationContext;
import org.springframework.context.annotation.Configuration;
import org.springframework.stereotype.Component;
import org.testcontainers.containers.ScyllaDBContainer;
import org.testcontainers.utility.DockerImageName;

import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;

import static org.junit.jupiter.api.Assertions.assertNotNull;

class UserRepositoryIT {

    private static final ScyllaDBContainer<?> SCYLLA =
            new ScyllaDBContainer<>(DockerImageName.parse("scylladb/scylla:5.4"));

    static {
        SCYLLA.start();
    }

    @AfterAll
    static void shutdown() {
        SCYLLA.stop();
    }

    @Test
    void saveAndReadByPrimaryKey() {
        try (AnnotationConfigApplicationContext context = new AnnotationConfigApplicationContext()) {
            context.register(TestScyllaConfiguration.class, UserRepository.class);
            context.refresh();

            UserRepository repository = context.getBean(UserRepository.class);

            UUID id = UUID.randomUUID();
            repository.save(new User(id, 20, "alice"));

            Map<CqlIdentifier, Object> primaryKey = new LinkedHashMap<>();
            primaryKey.put(CqlIdentifier.fromCql("id"), id);
            primaryKey.put(CqlIdentifier.fromCql("age"), 20);

            User loaded = repository.findByPrimaryKey(primaryKey);
            assertNotNull(loaded);
        }
    }

    @Configuration
    static class TestScyllaConfiguration extends AbstractScyllaConfiguration {

        @Override
        protected String getKeyspaceName() {
            return "testks";
        }

        @Override
        protected String getLocalDataCenter() {
            return "datacenter1";
        }

        @Override
        protected String getContactPoints() {
            return SCYLLA.getHost();
        }

        @Override
        protected int getPort() {
            return SCYLLA.getMappedPort(9042);
        }

        @Override
        protected List<CreateKeyspaceSpecification> getKeyspaceCreations() {
            return List.of(CreateKeyspaceSpecification.createKeyspace("testks").ifNotExists().withSimpleReplication(1));
        }

        @Override
        public SchemaAction getSchemaAction() {
            return SchemaAction.CREATE_IF_NOT_EXISTS;
        }

        @Override
        protected Set<Class<?>> getInitialEntitySet() {
            return Set.of(User.class);
        }
    }

    @Component
    static class UserRepository extends SimpleScyllaRepository<User> {
        UserRepository(ScyllaTemplate template) {
            super(User.class, template);
        }
    }

    @Table
    static class User {
        @PartitionKey
        private UUID id;
        @ClusteringColumn
        private int age;
        private String name;

        User() {}

        User(UUID id, int age, String name) {
            this.id = id;
            this.age = age;
            this.name = name;
        }
    }
}
```

Tip: for larger suites, reuse a singleton container and isolate test data by keyspace/table naming conventions.

## Build from source

```bash
mvn clean install
```

## Roadmap

- Stabilize startup/shutdown lifecycle behavior around schema refresh and keyspace actions.
- Expand automated tests:
  - mapping/conversion unit tests
  - repository integration tests against Scylla
  - schema action lifecycle tests
- Add richer query APIs (criteria/predicate-based extensions).
- Improve production hardening docs:
  - consistency-level strategy
  - timeout/retry guidance
  - migration/versioning playbook
- Prepare release packaging and publishing workflow improvements.

## Contributing

Contributions are welcome.

Recommended contribution flow:
1. Open an issue describing the use case, bug, or proposal.
2. Add/update tests for behavior changes.
3. Keep API changes backward-compatible where possible.
4. Submit a focused pull request with implementation notes.

## License

Apache License 2.0
