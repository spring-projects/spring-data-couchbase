package org.springframework.data.couchbase.repository;

import java.util.List;

import org.springframework.data.repository.CrudRepository;

public interface ItemRepository extends CrudRepository<Item, String> {

  List<Object> findAllByDescriptionNotNull();
}
