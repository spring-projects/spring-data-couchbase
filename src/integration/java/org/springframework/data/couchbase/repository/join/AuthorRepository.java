package org.springframework.data.couchbase.repository.join;

import org.springframework.data.couchbase.core.query.N1qlPrimaryIndexed;
import org.springframework.data.couchbase.repository.CouchbaseRepository;

@N1qlPrimaryIndexed
public interface AuthorRepository extends CouchbaseRepository<Author, String> {
}
