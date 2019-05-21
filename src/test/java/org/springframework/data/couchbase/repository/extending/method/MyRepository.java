package org.springframework.data.couchbase.repository.extending.method;

import org.springframework.data.couchbase.core.query.N1qlPrimaryIndexed;
import org.springframework.data.couchbase.core.query.ViewIndexed;
import org.springframework.data.repository.CrudRepository;

@N1qlPrimaryIndexed
@ViewIndexed(designDoc = "myItem", viewName = "all")
public interface MyRepository extends CrudRepository<MyItem, String>, MyRepositoryCustom {
}
