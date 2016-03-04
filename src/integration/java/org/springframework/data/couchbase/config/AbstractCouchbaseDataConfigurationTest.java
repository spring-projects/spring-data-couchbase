package org.springframework.data.couchbase.config;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertSame;

import com.couchbase.client.java.Bucket;
import com.couchbase.client.java.Cluster;
import com.couchbase.client.java.CouchbaseCluster;
import com.couchbase.client.java.cluster.ClusterInfo;
import com.couchbase.client.java.document.JsonDocument;
import com.couchbase.client.java.env.CouchbaseEnvironment;
import com.couchbase.client.java.env.DefaultCouchbaseEnvironment;
import org.junit.Test;
import org.junit.runner.RunWith;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.data.annotation.Id;
import org.springframework.data.couchbase.repository.CouchbaseRepository;
import org.springframework.data.couchbase.repository.config.EnableCouchbaseRepositories;
import org.springframework.stereotype.Repository;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.junit4.SpringJUnit4ClassRunner;

/**
 * This test case demonstrates that the {@link AbstractCouchbaseDataConfiguration} can take its SDK beans
 * from a sibling {@link Configuration}.
 *
 * @author Simon Baslé
 */
@SuppressWarnings("SpringJavaAutowiringInspection")
@RunWith(SpringJUnit4ClassRunner.class)
@ContextConfiguration
public class AbstractCouchbaseDataConfigurationTest {

  @Autowired
  ItemRepository repository;

  @Autowired
  Bucket client;

  @Configuration
  static class SdkConfig {

    private static final String IP = "127.0.0.1";
    private static final String BUCKET_NAME = "default";
    private static final String BUCKET_PASSWORD = "";

    public static Bucket bucket;

    @Bean
    public Cluster couchbaseCluster() {
      return CouchbaseCluster.create(IP);
    }

    @Bean
    public ClusterInfo couchbaseClusterInfo() {
      return couchbaseCluster().clusterManager(BUCKET_NAME, BUCKET_PASSWORD).info();
    }

    @Bean
    public Bucket couchbaseBucket() {
      Bucket b = couchbaseCluster().openBucket(BUCKET_NAME, BUCKET_PASSWORD);
      bucket = b;
      return b;
    }
  }

  @Configuration
  @EnableCouchbaseRepositories(basePackageClasses = AbstractCouchbaseDataConfigurationTest.class, considerNestedRepositories = true)
  static class Config extends AbstractCouchbaseDataConfiguration {

    @Autowired
    Cluster c;

    @Autowired
    ClusterInfo ci;

    @Autowired
    Bucket b;

    @Override
    public CouchbaseEnvironment couchbaseEnvironment() {
      return DefaultCouchbaseEnvironment.create();
    }

    @Override
    public Cluster couchbaseCluster() {
      return c;
    }

    @Override
    public ClusterInfo couchbaseClusterInfo() {
      return ci;
    }

    @Override
    public Bucket couchbaseClient() {
      return b;
    }
  }

  @Test
  public void testInjectedBucketIsFromAdditionalConfig() {
    assertSame(client, SdkConfig.bucket);
  }

  @Test
  public void testTemplateIsUsable() {
    String key = "simpleConfigTest";
    assertNotNull(repository);

    Item item = new Item();
    item.id = key;
    item.value = "Test if the SimpleCouchbaseConfiguration can correctly get Bucket/Cluster/etc... beans injected";

    repository.save(item);
    JsonDocument testDoc = client.get(key);

    assertNotNull(testDoc);
    assertNotNull(testDoc.content());
    assertEquals(item.value, testDoc.content().getString("value"));
  }

  private static class Item {
    @Id
    public String id;

    public String value;
  }

  @Repository
  interface ItemRepository extends CouchbaseRepository<Item, String> {}
}
