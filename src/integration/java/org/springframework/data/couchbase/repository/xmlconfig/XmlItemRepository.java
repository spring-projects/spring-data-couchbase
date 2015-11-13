package org.springframework.data.couchbase.repository.xmlconfig;

import org.springframework.data.couchbase.repository.CouchbaseRepository;
import org.springframework.data.couchbase.repository.Item;

public interface XmlItemRepository extends CouchbaseRepository<Item, String> {
}
