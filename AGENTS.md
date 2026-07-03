# Spring Data Couchbase — Agent Guide

## Build & Common Commands

```bash
# Build (skip tests)
./mvnw clean package -DskipTests

# Run unit tests only (files matching *Test.java / *Tests.java, excludes *IntegrationTests.java)
./mvnw test

# Run integration tests (requires a Couchbase cluster — see below)
./mvnw verify

# Run a single unit test class
./mvnw test -Dtest=QueryCriteriaTests

# Run a single integration test class
./mvnw verify -Dit.test=CouchbaseTemplateKeyValueIntegrationTests

# Compile only
./mvnw compile
```

## Test Infrastructure

Tests split into two categories controlled by Maven plugins:
- **Unit tests** (`maven-surefire-plugin`): `**/*Test.java`, `**/*Tests.java`
- **Integration tests** (`maven-failsafe-plugin`): `**/*IntegrationTests.java`

Integration tests require a Couchbase cluster. The cluster type is configured in `src/test/resources/integration.properties`:

| `cluster.type` | Description |
|---|---|
| `mocked` (default) | Uses `CouchbaseMock` in-process — no real Couchbase needed |
| `unmanaged` | Connects to a running Couchbase server |

For local development against a real cluster, create `src/test/resources/integration.local.properties` (git-ignored) overriding `cluster.type=unmanaged` and `cluster.unmanaged.seed=<host>:<port>`.

The `ClusterInvocationProvider` JUnit 5 extension bootstraps the cluster before integration tests run. All integration test classes extend `ClusterAwareIntegrationTests` (or `CollectionAwareIntegrationTests` for collection-scoped tests). Use `@IgnoreWhen` to conditionally skip tests based on cluster capabilities/type.

## Architecture Overview

### Package Structure (`org.springframework.data.couchbase`)

| Package | Purpose |
|---|---|
| `config` | `AbstractCouchbaseConfiguration` — the main Spring `@Configuration` base class users extend to configure everything (connection, converter, template beans) |
| `core` | `CouchbaseTemplate` / `ReactiveCouchbaseTemplate` — central operation classes; fluent operation API |
| `core.convert` | `MappingCouchbaseConverter`, custom converters, `JacksonTranslationService` (JSON serialization) |
| `core.mapping` | Persistent entity/property model, annotations (`@Document`, `@Field`, `@Expiry`, `@Durability`), event callbacks |
| `core.mapping.id` | ID generation strategies (`@GeneratedValue`, `@IdPrefix`, `@IdSuffix`) |
| `core.query` | `Query`, `QueryCriteria`, N1QL expression building |
| `core.index` | Index annotations (`@QueryIndexed`, `@CompositeQueryIndex`) and auto-index creator |
| `repository` | `CouchbaseRepository` / `ReactiveCouchbaseRepository` interfaces and annotations (`@Query`, `@ScanConsistency`, `@Scope`, `@Collection`) |
| `repository.support` | Factory classes, `SimpleCouchbaseRepository`, `SimpleReactiveCouchbaseRepository` |
| `repository.query` | Query derivation from method names (`PartTree*`) and `@Query` string parsing (`StringBased*`) — both imperative and reactive |
| `transaction` | `CouchbaseCallbackTransactionManager`, `CouchbaseTransactionalOperator`, interceptors |
| `cache` | `CouchbaseCacheManager` / `CouchbaseCache` — Spring Cache abstraction implementation |

### Dual Imperative/Reactive Model

Every major operation has both a blocking and a reactive variant:
- `CouchbaseTemplate` wraps `ReactiveCouchbaseTemplate` — the reactive template is the canonical implementation.
- Operation interfaces follow the pattern `ExecutableXxxByIdOperation` (blocking) / `ReactiveXxxByIdOperation` (reactive), with `*Support` classes as implementations (e.g., `ExecutableInsertByIdOperationSupport`).
- The `core/support/` package contains fine-grained `With*` builder interfaces (e.g., `WithExpiry`, `WithDurability`, `InCollection`) that compose into the fluent API.

### Repository Query Execution Flow

1. `CouchbaseRepositoryFactory` / `ReactiveCouchbaseRepositoryFactory` creates repository proxies.
2. For derived queries: `CouchbaseQueryMethod` → `N1qlQueryCreator` builds a `Query` from the `PartTree`.
3. For `@Query` string queries: `StringBasedCouchbaseQuery` / `StringBasedN1qlQueryParser` handles N1QL template substitution.
4. Execution delegates to `CouchbaseTemplate.findByQuery()` which runs N1QL (SQL++) via the SDK.
5. Analytics queries follow the same path using `ExecutableFindByAnalyticsOperation`.

### Entity Mapping

- `MappingCouchbaseConverter` converts between Java objects and `CouchbaseDocument` (a `Map`-backed structure).
- Type information is stored in a configurable key (default `_class`, overridable via `AbstractCouchbaseConfiguration.typeKey()`).
- Custom type mapping: extend `DefaultCouchbaseTypeMapper`, override `typeKey()`, and register a custom `MappingCouchbaseConverter` bean.
- Field encryption uses `CryptoConverter` + `couchbase-encryption` SDK dependency.

### Test Domain Model

`src/test/java/.../domain/` contains shared test entities (`User`, `Airport`, `Airline`, `Person`, etc.) and their repositories. `Config` extends `AbstractCouchbaseConfiguration` and is the shared Spring context configuration for most integration tests.
