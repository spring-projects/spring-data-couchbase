package org.springframework.data.couchbase.repository;

import static org.assertj.core.api.Assertions.assertThat;
import static org.springframework.data.couchbase.CouchbaseTestHelper.getRepositoryWithRetry;

import java.util.ArrayList;
import java.util.List;

import com.couchbase.client.java.Bucket;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.data.couchbase.ContainerResourceRunner;
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

@RunWith(ContainerResourceRunner.class)
@ContextConfiguration(classes = IntegrationTestApplicationConfig.class)
@TestExecutionListeners(SimpleCouchbaseRepositoryListener.class)
public class PageAndSliceIntegrationTests {

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
    repository = getRepositoryWithRetry(factory, UserRepository.class);
  }
  @Test
  public void shouldPageThroughResults() {
    Page<User> page1 = repository.findByAgeGreaterThan(9, PageRequest.of(0, 40)); //there are 90 matching users
    Page<User> page2 = repository.findByAgeGreaterThan(9, page1.nextPageable());
    Page<User> page3 = repository.findByAgeGreaterThan(9, page2.nextPageable());

    assertThat(page1.getTotalElements()).isEqualTo(90);
    assertThat(page1.getTotalPages()).isEqualTo(3);
    assertThat(page1.hasContent()).isTrue();
    assertThat(page1.hasNext()).isTrue();
    assertThat(page1.getNumberOfElements()).isEqualTo(40);

    assertThat(page2.hasContent()).isTrue();
    assertThat(page2.hasNext()).isTrue();
    assertThat(page2.getNumberOfElements()).isEqualTo(40);

    assertThat(page3.hasContent()).isTrue();
    assertThat(page3.hasNext()).isFalse();
    assertThat(page3.getNumberOfElements()).isEqualTo(10);
  }

  @Test(expected = UnsupportedOperationException.class)
  public void shouldThrowWhenPageableIsNullInPageQuery() {
    repository.findByAgeGreaterThan(9, null);
  }

  @Test
  public void shouldSliceThroughResults() {
    int count = 0;
    List<User> allMatching = new ArrayList<User>(10);
    Slice<User> slice = repository.findByAgeLessThan(9, PageRequest.of(0, 3)); //9 matching users (ages 0-8)
    allMatching.addAll(slice.getContent());
    while(slice.hasNext()) {
      slice = repository.findByAgeLessThan(9, slice.nextPageable());
      allMatching.addAll(slice.getContent());
      assertThat(slice.getContent().size()).isEqualTo(3);
    }
    assertThat(allMatching.size()).isEqualTo(9);
  }

  @Test(expected = UnsupportedOperationException.class)
  public void shouldThrowWhenPageableIsNullSliceQuery() {
    repository.findByAgeLessThan(9, null);
  }
}
