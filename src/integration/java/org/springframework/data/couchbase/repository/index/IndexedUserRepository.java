package org.springframework.data.couchbase.repository.index;

import org.springframework.data.couchbase.core.query.N1qlPrimaryIndexed;
import org.springframework.data.couchbase.core.query.N1qlSecondaryIndexed;
import org.springframework.data.couchbase.core.query.ViewIndexed;
import org.springframework.data.couchbase.repository.CouchbaseRepository;
import org.springframework.data.couchbase.repository.User;

@N1qlPrimaryIndexed
@N1qlSecondaryIndexed(indexName = IndexedRepositoryIT.SECONDARY)
@ViewIndexed(designDoc = IndexedRepositoryIT.VIEW_DOC, viewName = IndexedRepositoryIT.VIEW_NAME)
public interface IndexedUserRepository extends CouchbaseRepository<User, String> {
}
