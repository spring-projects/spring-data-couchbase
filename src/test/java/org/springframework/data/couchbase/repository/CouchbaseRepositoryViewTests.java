package org.springframework.data.couchbase.repository;

import com.couchbase.client.CouchbaseClient;
import com.couchbase.client.protocol.views.Query;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.data.couchbase.TestApplicationConfig;
import org.springframework.data.couchbase.core.CouchbaseTemplate;
import org.springframework.data.couchbase.repository.support.CouchbaseRepositoryFactory;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.TestExecutionListeners;
import org.springframework.test.context.junit4.SpringJUnit4ClassRunner;

import java.util.Iterator;

import static com.couchbase.client.protocol.views.Stale.FALSE;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.MatcherAssert.assertThat;

@RunWith(SpringJUnit4ClassRunner.class)
@ContextConfiguration(classes = TestApplicationConfig.class)
@TestExecutionListeners(CouchbaseRepositoryViewListener.class)
public class CouchbaseRepositoryViewTests {

  @Autowired
  private CouchbaseClient client;

  @Autowired
  private CouchbaseTemplate template;

  private UserRepositoryCustom repository;

  @Before
  public void setup() throws Exception {
    repository = new CouchbaseRepositoryFactory(template).getRepository(UserRepositoryCustom.class);
  }

  @Test
  public void shouldFindAllWithCustomView() {
    client.query(client.getView("user", "customFindAllView"), new Query().setStale(FALSE));
    Iterable<User> allUsers = repository.findAll();
    int i = 0;
    for (final User allUser : allUsers) {
      i++;
    }
    assertThat(i, is(100));
  }

  @Test
  public void shouldCountWithCustomView() {
    client.query(client.getView("user", "customCountView"), new Query().setStale(FALSE));
    final long value = repository.count();
    assertThat(value, is(100L));
  }

}
