package org.springframework.data.couchbase.repository.wiring;

import static org.junit.Assert.*;
import static org.mockito.Mockito.*;

import java.util.Arrays;
import java.util.List;
import java.util.Optional;

import com.couchbase.client.java.cluster.ClusterInfo;
import com.couchbase.client.java.util.features.CouchbaseFeature;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.runner.RunWith;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.data.annotation.Id;
import org.springframework.data.couchbase.config.AbstractCouchbaseConfiguration;
import org.springframework.data.couchbase.core.CouchbaseOperations;
import org.springframework.data.couchbase.core.CouchbaseTemplate;
import org.springframework.data.couchbase.repository.CouchbaseRepository;
import org.springframework.data.couchbase.repository.config.EnableCouchbaseRepositories;
import org.springframework.data.couchbase.repository.config.RepositoryOperationsMapping;
import org.springframework.data.couchbase.repository.support.IndexManager;
import org.springframework.stereotype.Repository;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.junit4.SpringJUnit4ClassRunner;

/**
 * This test case demonstrates (with a bit of mocking) that the framework will take the
 * {@link RepositoryOperationsMapping} into account in its wiring of repositories with
 * underlying {@link CouchbaseOperations} beans.
 *
 * @author Simon Baslé
 * @author Mark Paluch
 */
@SuppressWarnings("SpringJavaAutowiringInspection")
@RunWith(SpringJUnit4ClassRunner.class)
@ContextConfiguration
public class RepositoryTemplateWiringTests {

  private static CouchbaseOperations mockOpsA;
  private static CouchbaseOperations mockOpsB;
  private static CouchbaseOperations mockOpsC;

  @BeforeClass
  public static void initMocks() {
    ClusterInfo info = mock(ClusterInfo.class);
    when(info.checkAvailable(any(CouchbaseFeature.class))).thenReturn(true);

    mockOpsA = mock(CouchbaseOperations.class);
    when(mockOpsA.getCouchbaseClusterInfo()).thenReturn(info);
    when(mockOpsA.exists(any(String.class))).thenReturn(true);

    mockOpsB = mock(CouchbaseOperations.class);
    when(mockOpsB.getCouchbaseClusterInfo()).thenReturn(info);
    when(mockOpsB.exists(any(String.class))).thenReturn(false);

    mockOpsC = spy(new CouchbaseTemplate(info, null));
    Misc cValue = new Misc();
    cValue.id = "mock";
    cValue.random = true;
    doReturn(cValue).when(mockOpsC).findById(any(String.class), any(Class.class));
  }

  @Autowired
  BucketARepository repositoryA;

  @Autowired
  BucketBRepository repositoryB;

  @Autowired
  BucketCRepository repositoryC;

  @Configuration
  @EnableCouchbaseRepositories(basePackageClasses = RepositoryTemplateWiringTests.class, considerNestedRepositories = true)
  static class Config extends AbstractCouchbaseConfiguration {

    @Override
    protected List<String> getBootstrapHosts() {
      return Arrays.asList("127.0.0.1");
    }

    @Override
    protected String getBucketName() {
      return "default";
    }

    @Override
    protected String getBucketPassword() {
      return "";
    }

    @Bean
    public CouchbaseOperations templateA() {
      return mockOpsA;
    }

    @Bean
    public CouchbaseOperations templateB() {
      return mockOpsB;
    }

    @Bean
    public CouchbaseOperations templateC() {
      return mockOpsC;
    }

    //this is for dev so it is ok to auto-create indexes
    @Override
    public IndexManager indexManager() {
      return new IndexManager();
    }

    @Override
    public void configureRepositoryOperationsMapping(RepositoryOperationsMapping base) {
      base.setDefault(templateC())
          .map(BucketBRepository.class, templateB())
          .mapEntity(Item.class, templateA());
    }
  }

  @Test
  public void testRepositoriesAreInstanciatedWithCorrectTemplates() {
    assertNotNull(repositoryA);
    assertNotNull(repositoryB);
    assertNotNull(repositoryC);

    boolean existA = repositoryA.exists("testA");
    boolean existB = repositoryB.exists("testB");
    Optional<Misc> valueC = repositoryC.findOne("toto");

    assertTrue(existA);
    assertFalse(existB);
    assertTrue(valueC.isPresent());
    valueC.ifPresent(actual -> {
      assertEquals("mock", actual.id);
      assertEquals(true, actual.random);
    });

    verify(mockOpsA).exists("testA");
    verify(mockOpsB).exists("testB");
    verify(mockOpsC).findById(any(String.class), eq(Misc.class));
  }

  private static class Item {
    @Id
    public String id;

    public String value;
  }

  private static class Misc {
    @Id
    public String id;

    public boolean random;
  }

  @Repository
  static interface BucketARepository extends CouchbaseRepository<Item, String> {}

  @Repository
  static interface BucketBRepository extends CouchbaseRepository<Item, String> {}

  @Repository
  static interface BucketCRepository extends CouchbaseRepository<Misc, String> {}

}
