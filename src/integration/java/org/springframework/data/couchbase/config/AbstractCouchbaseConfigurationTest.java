package org.springframework.data.couchbase.config;

import static org.junit.Assert.*;

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
import org.springframework.data.couchbase.core.CouchbaseTemplate;
import org.springframework.data.couchbase.core.WriteResultChecking;
import org.springframework.data.couchbase.repository.CouchbaseRepository;
import org.springframework.data.couchbase.repository.config.EnableCouchbaseRepositories;
import org.springframework.stereotype.Repository;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.junit4.SpringJUnit4ClassRunner;

/**
 * This test case demonstrates that the {@link AbstractCouchbaseConfiguration} can take its SDK beans
 * from a sibling {@link Configuration}.
 *
 * @author Simon Baslé
 */
@SuppressWarnings("SpringJavaAutowiringInspection")
@RunWith(SpringJUnit4ClassRunner.class)
@ContextConfiguration
public class AbstractCouchbaseConfigurationTest {

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
      return CouchbaseCluster.create(couchbaseEnv(), IP);
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

    @Bean
    public CouchbaseEnvironment couchbaseEnv() {
      return DefaultCouchbaseEnvironment.create();
    }
  }

  @Configuration
  @EnableCouchbaseRepositories(basePackageClasses = AbstractCouchbaseConfigurationTest.class, considerNestedRepositories = true)
  abstract static class Config extends AbstractCouchbaseConfiguration {

    @Autowired
    Cluster c;

    @Autowired
    ClusterInfo ci;

    @Autowired
    Bucket b;

    @Autowired
    CouchbaseEnvironment e;

    //TODO maybe create the bucket if doesn't exist

    @Override
    protected CouchbaseEnvironment getEnvironment() {
      return DefaultCouchbaseEnvironment.builder()
              .connectTimeout(10000)
              .kvTimeout(10000)
              .queryTimeout(10000)
              .viewTimeout(10000)
              .build();
    }

    @Override
    public CouchbaseTemplate couchbaseTemplate() throws Exception {
      CouchbaseTemplate template = super.couchbaseTemplate();
      template.setWriteResultChecking(WriteResultChecking.LOG);
      return template;
    }

    @Override
    protected CouchbaseConfigurer couchbaseConfigurer() {
      return new TestCouchbaseConfigurer(e, c, ci, b);
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

  private static class TestCouchbaseConfigurer implements CouchbaseConfigurer {

    private CouchbaseEnvironment env;
    private Cluster cluster;
    private ClusterInfo info;
    private Bucket bucket;

    public TestCouchbaseConfigurer(CouchbaseEnvironment env, Cluster cluster, ClusterInfo info, Bucket bucket) {
      this.env = env;
      this.cluster = cluster;
      this.info = info;
      this.bucket = bucket;
    }

    @Override
    public CouchbaseEnvironment couchbaseEnvironment() throws Exception {
      return this.env;
    }

    @Override
    public Cluster couchbaseCluster() throws Exception {
      return this.cluster;
    }

    @Override
    public ClusterInfo couchbaseClusterInfo() throws Exception {
      return this.info;
    }

    @Override
    public Bucket couchbaseClient() throws Exception {
      return this.bucket;
    }
  }

  @Repository
  interface ItemRepository extends CouchbaseRepository<Item, String> {}
}
