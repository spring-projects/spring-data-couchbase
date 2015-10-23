package org.springframework.data.couchbase.repository;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import java.util.ArrayList;
import java.util.List;

import com.couchbase.client.java.Bucket;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.data.couchbase.IntegrationTestApplicationConfig;
import org.springframework.data.couchbase.repository.config.RepositoryOperationsMapping;
import org.springframework.data.couchbase.repository.support.CouchbaseRepositoryFactory;
import org.springframework.data.couchbase.repository.support.IndexManager;
import org.springframework.data.domain.Page;
import org.springframework.data.domain.PageRequest;
import org.springframework.data.domain.Slice;
import org.springframework.data.repository.core.support.RepositoryFactorySupport;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.TestExecutionListeners;
import org.springframework.test.context.junit4.SpringJUnit4ClassRunner;

@RunWith(SpringJUnit4ClassRunner.class)
@ContextConfiguration(classes = IntegrationTestApplicationConfig.class)
@TestExecutionListeners(SimpleCouchbaseRepositoryListener.class)
public class PageAndSliceTests {

  @Autowired
  private Bucket client;

  @Autowired
  private RepositoryOperationsMapping operationsMapping;

  @Autowired
  private IndexManager indexManager;

  private UserRepository repository;

  @Before
  public void setup() throws Exception {
    RepositoryFactorySupport factory = new CouchbaseRepositoryFactory(operationsMapping, indexManager);
    repository = factory.getRepository(UserRepository.class);
  }
  @Test
  public void shouldPageThroughResults() {
    Page<User> page1 = repository.findByAgeGreaterThan(9, new PageRequest(0, 40)); //there are 90 matching users
    Page<User> page2 = repository.findByAgeGreaterThan(9, page1.nextPageable());
    Page<User> page3 = repository.findByAgeGreaterThan(9, page2.nextPageable());

    assertEquals(90, page1.getTotalElements());
    assertEquals(3, page1.getTotalPages());
    assertTrue(page1.hasContent());
    assertTrue(page1.hasNext());
    assertEquals(40, page1.getNumberOfElements());

    assertTrue(page2.hasContent());
    assertTrue(page2.hasNext());
    assertEquals(40, page2.getNumberOfElements());

    assertTrue(page3.hasContent());
    assertFalse(page3.hasNext());
    assertEquals(10, page3.getNumberOfElements());
  }

  @Test(expected = IllegalArgumentException.class)
  public void shouldThrowWhenPageableIsNullInPageQuery() {
    repository.findByAgeGreaterThan(9, null);
  }

  @Test
  public void shouldSliceThroughResults() {
    int count = 0;
    List<User> allMatching = new ArrayList<User>(10);
    Slice<User> slice = repository.findByAgeLessThan(9, new PageRequest(0, 3)); //9 matching users (ages 0-8)
    allMatching.addAll(slice.getContent());
    while(slice.hasNext()) {
      slice = repository.findByAgeLessThan(9, slice.nextPageable());
      allMatching.addAll(slice.getContent());
      assertEquals(3, slice.getContent().size());
    }
    assertEquals(9, allMatching.size());
  }

  @Test(expected = IllegalArgumentException.class)
  public void shouldThrowWhenPageableIsNullSliceQuery() {
    repository.findByAgeLessThan(9, null);
  }
}
