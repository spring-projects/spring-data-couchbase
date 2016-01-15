package org.springframework.data.couchbase.repository.spel;

import java.util.List;

import org.springframework.data.couchbase.core.query.N1qlPrimaryIndexed;
import org.springframework.data.couchbase.core.query.Query;
import org.springframework.data.couchbase.repository.User;
import org.springframework.data.repository.CrudRepository;
import org.springframework.stereotype.Repository;

@N1qlPrimaryIndexed
public interface SpelRepository extends CrudRepository<User, String> {

  @Query("#{#n1ql.selectEntity} WHERE #{#n1ql.filter} AND username = \"#{oneCustomer}\"")
  List<User> findCustomUsers();
}
