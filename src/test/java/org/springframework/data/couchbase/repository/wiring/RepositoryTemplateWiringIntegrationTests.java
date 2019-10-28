package org.springframework.data.couchbase.repository.wiring;

import static org.junit.Assert.*;
import static org.mockito.Mockito.*;

import java.util.Arrays;
import java.util.List;
import java.util.Optional;

import com.couchbase.client.java.env.ClusterEnvironment;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.runner.RunWith;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.data.annotation.Id;

import org.springframework.data.couchbase.ContainerResourceRunner;
import org.springframework.data.couchbase.config.AbstractCouchbaseConfiguration;
import org.springframework.data.couchbase.core.CouchbaseOperations;
import org.springframework.data.couchbase.core.CouchbaseTemplate;
import org.springframework.data.couchbase.core.convert.MappingCouchbaseConverter;
import org.springframework.data.couchbase.core.mapping.CouchbaseMappingContext;
import org.springframework.data.couchbase.repository.CouchbaseRepository;
import org.springframework.data.couchbase.repository.config.EnableCouchbaseRepositories;
import org.springframework.data.couchbase.repository.config.RepositoryOperationsMapping;
import org.springframework.stereotype.Repository;
import org.springframework.test.context.ContextConfiguration;

/**
 * This test case demonstrates (with a bit of mocking) that the framework will take the
 * {@link RepositoryOperationsMapping} into account in its wiring of repositories with
 * underlying {@link CouchbaseOperations} beans.
 *
 * @author Simon Baslé
 * @author Mark Paluch
 */
@SuppressWarnings("SpringJavaAutowiringInspection")
@RunWith(ContainerResourceRunner.class)
@ContextConfiguration
public class RepositoryTemplateWiringIntegrationTests {

  private static CouchbaseOperations mockOpsA;
  private static CouchbaseOperations mockOpsB;
  private static CouchbaseOperations mockOpsC;

  @BeforeClass
  public static void initMocks() {

    mockOpsA = mock(CouchbaseOperations.class);
    when(mockOpsA.exists(any(String.class))).thenReturn(true);
    when(mockOpsA.getConverter()).thenReturn(new MappingCouchbaseConverter(new CouchbaseMappingContext()));


    mockOpsB = mock(CouchbaseOperations.class);
    when(mockOpsB.exists(any(String.class))).thenReturn(false);
    when(mockOpsB.getConverter()).thenReturn(new MappingCouchbaseConverter(new CouchbaseMappingContext()));

    mockOpsC = spy(new CouchbaseTemplate(null, null));
    Misc cValue = new Misc();
    cValue.id = "mock";
    cValue.random = true;
    when(mockOpsC.getConverter()).thenReturn(new MappingCouchbaseConverter(new CouchbaseMappingContext()));
    doReturn(cValue).when(mockOpsC).findById(any(String.class), any(Class.class));
  }

  @Autowired
  BucketARepository repositoryA;

  @Autowired
  BucketBRepository repositoryB;

  @Autowired
  BucketCRepository repositoryC;

  @Configuration
  @EnableCouchbaseRepositories(basePackageClasses = RepositoryTemplateWiringIntegrationTests.class, considerNestedRepositories = true)
  static class Config extends AbstractCouchbaseConfiguration {

    @Override
    protected List<String> getBootstrapHosts() {
      return Arrays.asList("127.0.0.1");
    }

    @Override
    protected String getBucketName() {
      return "protected";
    }

    @Override
    protected String getPassword() {
      return "password";
    }

    @Override
    public ClusterEnvironment couchbaseEnvironment() {
      return ClusterEnvironment.builder().build();
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

/*    boolean existA = repositoryA.existsById("testA");
    boolean existB = repositoryB.existsById("testB");
    Optional<Misc> valueC = repositoryC.findById("toto");

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
  */}

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
