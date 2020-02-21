package org.springframework.data.couchbase.core;

public interface FluentCouchbaseOperations extends
  ExecutableUpsertByIdOperation,
  ExecutableInsertByIdOperation,
  ExecutableReplaceByIdOperation,
  ExecutableFindByIdOperation,
  ExecutableFindFromReplicasByIdOperation,
  ExecutableFindByQueryOperation,
  ExecutableFindByAnalyticsOperation,
  ExecutableExistsByIdOperation,
  ExecutableRemoveByIdOperation,
  ExecutableRemoveByQueryOperation {
}
