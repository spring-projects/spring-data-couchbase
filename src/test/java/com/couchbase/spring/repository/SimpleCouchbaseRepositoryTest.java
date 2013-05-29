package com.couchbase.spring.repository;

import com.couchbase.spring.TestApplicationConfig;
import com.couchbase.spring.core.CouchbaseTemplate;
import com.couchbase.spring.repository.support.CouchbaseRepositoryFactory;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.data.repository.core.support.RepositoryFactorySupport;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.junit4.SpringJUnit4ClassRunner;

import static junit.framework.Assert.assertNull;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

@RunWith(SpringJUnit4ClassRunner.class)
@ContextConfiguration(classes = TestApplicationConfig.class)
public class SimpleCouchbaseRepositoryTest {

  @Autowired
  private CouchbaseTemplate template;

  @Test
  public void simpleCrud() {
    String key = "my_unique_user_key";
    RepositoryFactorySupport factory = new CouchbaseRepositoryFactory(template);
    UserRepository repository = factory.getRepository(UserRepository.class);
    User instance = new User(key, "foobar");
    repository.save(instance);

    User found = repository.findOne(key);
    assertEquals(instance.getKey(), found.getKey());
    assertEquals(instance.getUsername(), found.getUsername());

    assertTrue(repository.exists(key));
    repository.delete(found);

    assertNull(repository.findOne(key));
    assertFalse(repository.exists(key));
  }

}
