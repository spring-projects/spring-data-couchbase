package org.springframework.data.couchbase.repository.auditing;

import org.springframework.data.couchbase.repository.CouchbaseRepository;

public interface AuditedRepository extends CouchbaseRepository<AuditedItem, String> {

}
