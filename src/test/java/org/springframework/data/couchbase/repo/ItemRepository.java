package org.springframework.data.couchbase.repo;

import java.util.List;

import org.springframework.data.couchbase.core.query.N1qlPrimaryIndexed;
import org.springframework.data.couchbase.repository.Item;
import org.springframework.data.repository.CrudRepository;
import org.springframework.stereotype.Repository;

@Repository
@N1qlPrimaryIndexed
public interface ItemRepository extends CrudRepository<Item, String> {

  List<Object> findAllByDescriptionNotNull();
}
