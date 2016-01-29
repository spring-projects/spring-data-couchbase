package org.springframework.data.couchbase.repository.index;

import org.springframework.data.annotation.Id;
import org.springframework.data.couchbase.core.mapping.Document;
import org.springframework.data.couchbase.core.query.ViewIndexed;
import org.springframework.data.couchbase.repository.CouchbaseRepository;

@ViewIndexed(designDoc = "foo")
public interface IndexedFooRepository extends CouchbaseRepository<IndexedFooRepository.Foo, String> {

  @Document
  final class Foo {
    @Id
    private String id;

    private String value1;

    private int value2;

    public Foo(String id, String value1, int value2) {
      this.id = id;
      this.value1 = value1;
      this.value2 = value2;
    }
  }
}
