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

  //notice how the SpEL syntax #{[0]} considers the first method argument,
  //and N1QL placeholder resolution will still consider every method argument as a placeholder value.
  //thus N1QL placeholder used is $2, to match criteriaValue
  @Query("#{#n1ql.selectEntity} WHERE #{#n1ql.filter} AND #{[0]} = $2")
  List<User> findUserWithDynamicCriteria(String criteriaField, Object criteriaValue);
}
