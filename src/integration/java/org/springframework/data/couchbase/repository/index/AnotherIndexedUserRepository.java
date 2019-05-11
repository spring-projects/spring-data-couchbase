package org.springframework.data.couchbase.repository.index;

import java.util.List;

import org.springframework.data.couchbase.core.query.N1qlSecondaryIndexed;
import org.springframework.data.couchbase.core.query.ViewIndexed;
import org.springframework.data.couchbase.repository.CouchbaseRepository;
import org.springframework.data.couchbase.repository.User;

@N1qlSecondaryIndexed(indexName = IndexedRepositoryIT.IGNORED_SECONDARY)
@ViewIndexed(designDoc = IndexedRepositoryIT.VIEW_DOC, viewName = IndexedRepositoryIT.IGNORED_VIEW_NAME)
public interface AnotherIndexedUserRepository extends CouchbaseRepository<User, String> {

  public List<User> findByAge(int age);
}
