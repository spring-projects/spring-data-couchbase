package org.springframework.data.couchbase.repository.index;

import java.util.List;

import org.springframework.data.couchbase.core.query.N1qlSecondaryIndexed;
import org.springframework.data.couchbase.core.query.ViewIndexed;
import org.springframework.data.couchbase.repository.CouchbaseRepository;
import org.springframework.data.couchbase.repository.User;

@N1qlSecondaryIndexed(indexName = IndexedRepositoryIntegrationTests.IGNORED_SECONDARY)
@ViewIndexed(designDoc = IndexedRepositoryIntegrationTests.VIEW_DOC, viewName = IndexedRepositoryIntegrationTests.IGNORED_VIEW_NAME)
public interface AnotherIndexedUserRepository extends CouchbaseRepository<User, String> {

  public List<User> findByAge(int age);
}
