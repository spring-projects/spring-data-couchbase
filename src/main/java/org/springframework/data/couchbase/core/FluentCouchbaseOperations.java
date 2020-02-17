package org.springframework.data.couchbase.core;

public interface FluentCouchbaseOperations extends
  ExecutableUpsertByIdOperation,
  ExecutableFindByIdOperation,
  ExecutableFindByQueryOperation,
  ExecutableExistsByIdOperation,
  ExecutableRemoveByIdOperation,
  ExecutableRemoveByQueryOperation {
}
