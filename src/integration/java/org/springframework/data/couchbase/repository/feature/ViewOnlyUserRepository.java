package org.springframework.data.couchbase.repository.feature;

import org.springframework.data.couchbase.repository.User;
import org.springframework.data.repository.CrudRepository;

public interface ViewOnlyUserRepository extends CrudRepository<User, String> {
}
