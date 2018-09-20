package org.springframework.data.couchbase.repository.join;

import org.springframework.data.couchbase.core.query.N1qlSecondaryIndexed;
import org.springframework.data.couchbase.repository.CouchbaseRepository;

@N1qlSecondaryIndexed(indexName = "bookIndex")
interface BookRepository extends CouchbaseRepository<Book, String> {
}