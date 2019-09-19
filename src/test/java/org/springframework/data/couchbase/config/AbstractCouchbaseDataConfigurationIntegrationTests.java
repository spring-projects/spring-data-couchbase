package org.springframework.data.couchbase.config;

import static org.junit.Assert.*;

import com.couchbase.client.java.Bucket;
import com.couchbase.client.java.Cluster;
import com.couchbase.client.java.ClusterOptions;
import com.couchbase.client.java.Collection;

import com.couchbase.client.java.env.ClusterEnvironment;
import com.couchbase.client.java.json.JsonObject;
import org.junit.Test;
import org.junit.runner.RunWith;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.data.annotation.Id;
import org.springframework.data.couchbase.ContainerResourceRunner;
import org.springframework.data.couchbase.repository.CouchbaseRepository;
import org.springframework.data.couchbase.repository.config.EnableCouchbaseRepositories;
import org.springframework.stereotype.Repository;
import org.springframework.test.context.ContextConfiguration;

/**
 * This test case demonstrates that the {@link AbstractCouchbaseDataConfiguration} can take its SDK beans
 * from a sibling {@link Configuration}.
 *
 * Tests DATACOUCH-279
 *
 * @author Simon Baslé
 */
@SuppressWarnings("SpringJavaAutowiringInspection")
@RunWith(ContainerResourceRunner.class)
@ContextConfiguration
public class AbstractCouchbaseDataConfigurationIntegrationTests {

  @Autowired
  ItemRepository repository;

  @Autowired
  Collection client;

  @Configuration
  static class SdkConfig {

    private static final String IP = "127.0.0.1";
    private static final String BUCKET_NAME = "protected";
    private static final String BUCKET_PASSWORD = "password";

    public static Bucket bucket;
    public static Collection collection;

    @Bean
    public Cluster couchbaseCluster() {
      return Cluster.connect(IP, getOptions());
    }

    @Bean Collection couchbaseCollection() {
      collection = couchbaseBucket().defaultCollection();
      return collection;
    }

    @Bean
    public Bucket couchbaseBucket() {
      bucket = couchbaseCluster().bucket(BUCKET_NAME);
      return bucket;
    }

    @Bean
    public ClusterOptions getOptions() { return ClusterOptions.clusterOptions(BUCKET_NAME, BUCKET_PASSWORD).environment(couchbaseEnv()); }

    @Bean
    public ClusterEnvironment couchbaseEnv() {
      return ClusterEnvironment.builder().build();
    }
  }

  @Configuration
  @EnableCouchbaseRepositories(basePackageClasses = AbstractCouchbaseDataConfigurationIntegrationTests.class, considerNestedRepositories = true)
  static class Config extends AbstractCouchbaseDataConfiguration {

    @Autowired
    Cluster cluster;

    @Autowired
    Collection collection;

    @Autowired
    ClusterEnvironment env;

    @Override
    protected CouchbaseConfigurer couchbaseConfigurer() {
      return new TestCouchbaseConfigurer(cluster, collection);
    }
  }

  @Test
  public void testInjectedBucketIsFromAdditionalConfig() {
    assertSame(client, SdkConfig.collection);
  }

  @Test
  public void testTemplateIsUsable() {
    String key = "simpleConfigTest";
    assertNotNull(repository);

    Item item = new Item();
    item.id = key;
    item.value = "Test if the SimpleCouchbaseConfiguration can correctly get Bucket/Cluster/etc... beans injected";

    repository.save(item);
    Item repoItem = repository.findById(key).get();
    System.out.println("XXXXXXXX " + repoItem.value);
    JsonObject testItem = client.get(key).contentAsObject();

    assertNotNull(testItem);
    System.out.println("XXXXXXXXXX " + testItem.toString() + "XXXXXXX " + item.value);
    assertEquals(item.value, testItem.getString("value"));
  }

  private static class Item {
    @Id
    public String id;

    public String value;
  }

  private static class TestCouchbaseConfigurer implements CouchbaseConfigurer {

    private Cluster cluster;
    private Collection collection;

    public TestCouchbaseConfigurer(Cluster cluster, Collection collection) {
      this.cluster = cluster;
      this.collection = collection;
    }

    @Override
    public Cluster couchbaseCluster() throws Exception {
      return this.cluster;
    }

    @Override
    public Collection couchbaseClient() throws Exception {
      return this.collection;
    }

  }

  @Repository
  interface ItemRepository extends CouchbaseRepository<Item, String> {}
}
