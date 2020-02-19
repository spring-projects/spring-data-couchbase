package org.springframework.data.couchbase.repository;

import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Configuration;
import org.springframework.data.couchbase.config.AbstractCouchbaseConfiguration;
import org.springframework.data.couchbase.domain.User;
import org.springframework.data.couchbase.domain.UserRepository;
import org.springframework.data.couchbase.repository.config.EnableCouchbaseRepositories;
import org.springframework.data.couchbase.util.ClusterAwareIntegrationTest;
import org.springframework.test.context.junit.jupiter.SpringJUnitConfig;

import java.util.Optional;
import java.util.UUID;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

@SpringJUnitConfig(CouchbaseRepositoryKeyValueIntegrationTest.Config.class)
public class CouchbaseRepositoryKeyValueIntegrationTest extends ClusterAwareIntegrationTest {

  @Autowired
  UserRepository userRepository;

  @Test
  void saveAndFindById() {
    User user = new User(UUID.randomUUID().toString(), "f", "l");

    assertFalse(userRepository.existsById(user.getId()));

    userRepository.save(user);

    Optional<User> found = userRepository.findById(user.getId());
    assertTrue(found.isPresent());
    found.ifPresent(u -> assertEquals(user, u));

    assertTrue(userRepository.existsById(user.getId()));
  }

  @Configuration
  @EnableCouchbaseRepositories("org.springframework.data.couchbase")
  static class Config extends AbstractCouchbaseConfiguration {

    @Override
    public String getConnectionString() {
      return connectionString();
    }

    @Override
    public String getUserName() {
      return config().adminUsername();
    }

    @Override
    public String getPassword() {
      return config().adminPassword();
    }

    @Override
    public String getBucketName() {
      return bucketName();
    }

  }

}
