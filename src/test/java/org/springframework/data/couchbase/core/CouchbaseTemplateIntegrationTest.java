package org.springframework.data.couchbase.core;

import com.couchbase.client.java.Cluster;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.dao.DataRetrievalFailureException;
import org.springframework.dao.DuplicateKeyException;
import org.springframework.data.couchbase.CouchbaseClientFactory;
import org.springframework.data.couchbase.SimpleCouchbaseClientFactory;
import org.springframework.data.couchbase.core.convert.CouchbaseConverter;
import org.springframework.data.couchbase.core.convert.MappingCouchbaseConverter;
import org.springframework.data.couchbase.domain.User;

import java.util.UUID;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class CouchbaseTemplateIntegrationTest {

  private CouchbaseTemplate couchbaseTemplate;

  @BeforeEach
  void beforeEach() {
    Cluster cluster = Cluster.connect("127.0.0.1", "Administrator", "password");
    CouchbaseClientFactory clientFactory = new SimpleCouchbaseClientFactory(cluster, "travel-sample");
    CouchbaseConverter couchbaseConverter = new MappingCouchbaseConverter();
    couchbaseTemplate = new CouchbaseTemplate(clientFactory, couchbaseConverter);
  }

  @Test
  void upsertAndFindById() {
    User user = new User(UUID.randomUUID().toString(), "firstname", "lastname");
    User modified = couchbaseTemplate.upsertById(User.class).one(user);
    assertEquals(user, modified);

    User found = couchbaseTemplate.findById(User.class).one(user.getId());
    assertEquals(user, found);
  }

  @Test
  void findDocWhichDoesNotExist() {
    assertThrows(
      DataRetrievalFailureException.class,
      () -> couchbaseTemplate.findById(User.class).one(UUID.randomUUID().toString())
    );
  }

  @Test
  void upsertAndReplaceById() {
    User user = new User(UUID.randomUUID().toString(), "firstname", "lastname");
    User modified = couchbaseTemplate.upsertById(User.class).one(user);
    assertEquals(user, modified);

    User toReplace = new User(modified.getId(), "some other", "lastname");
    couchbaseTemplate.replaceById(User.class).one(toReplace);

    User loaded = couchbaseTemplate.findById(User.class).one(toReplace.getId());
    assertEquals("some other", loaded.getFirstname());
  }

  @Test
  void upsertAndRemoveById() {
    User user = new User(UUID.randomUUID().toString(), "firstname", "lastname");
    User modified = couchbaseTemplate.upsertById(User.class).one(user);
    assertEquals(user, modified);

    RemoveResult removeResult = couchbaseTemplate.removeById().one(user.getId());
    assertEquals(user.getId(), removeResult.getId());
    assertTrue(removeResult.getCas() != 0);
    assertTrue(removeResult.getMutationToken().isPresent());

    assertThrows(
      DataRetrievalFailureException.class,
      () -> couchbaseTemplate.findById(User.class).one(user.getId())
    );
  }

  @Test
  void insertById() {
    User user = new User(UUID.randomUUID().toString(), "firstname", "lastname");
    User inserted = couchbaseTemplate.insertById(User.class).one(user);
    assertEquals(user, inserted);

    assertThrows(DuplicateKeyException.class, () -> couchbaseTemplate.insertById(User.class).one(user));
  }

  @Test
  void existsById() {
    String id = UUID.randomUUID().toString();
    assertFalse(couchbaseTemplate.existsById().one(id));

    User user = new User(id, "firstname", "lastname");
    User inserted = couchbaseTemplate.insertById(User.class).one(user);
    assertEquals(user, inserted);

    assertTrue(couchbaseTemplate.existsById().one(id));
  }

  // test find by query
  // test find by analytics
  // test remove by query

}
