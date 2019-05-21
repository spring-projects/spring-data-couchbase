package org.springframework.data.couchbase.repository;

import java.util.List;

import org.springframework.data.couchbase.core.query.N1qlPrimaryIndexed;
import org.springframework.data.repository.CrudRepository;

@N1qlPrimaryIndexed
public interface ItemRepository extends CrudRepository<Item, String> {

  List<Object> findAllByDescriptionNotNull();
}
