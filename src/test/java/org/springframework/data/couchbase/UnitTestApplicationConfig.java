package org.springframework.data.couchbase;

import java.util.Collections;
import java.util.List;

import com.couchbase.client.java.Collection;
import org.mockito.Mockito;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.data.couchbase.config.AbstractCouchbaseConfiguration;
import org.springframework.data.couchbase.core.CouchbaseTemplate;
import org.springframework.data.couchbase.core.WriteResultChecking;
import org.springframework.data.couchbase.core.query.Consistency;
import org.springframework.data.couchbase.repository.support.IndexManager;

import com.couchbase.client.java.Cluster;

@Configuration
public class UnitTestApplicationConfig extends AbstractCouchbaseConfiguration {

  @Bean
  public String couchbaseAdminUser() {
    return "someLogin";
  }

  @Bean
  public String couchbaseAdminPassword() {
    return "somePassword";
  }

  @Override
  protected List<String> getBootstrapHosts() {
    return Collections.singletonList("192.1.2.3");
  }

  @Override
  protected String getBucketName() {
    return "someBucket";
  }

  @Override
  protected String getBucketPassword() {
    return "someBucketPassword";
  }

  @Override
  public Cluster couchbaseCluster() throws Exception {
    return Mockito.mock(Cluster.class);
  }

  @Override
  public Collection couchbaseClient() throws Exception {
    return Mockito.mock(Collection.class);
  }

  @Override
  public CouchbaseTemplate couchbaseTemplate() throws Exception {
    CouchbaseTemplate template = super.couchbaseTemplate();
    template.setWriteResultChecking(WriteResultChecking.LOG);
    return template;
  }

  //this is for dev so it is ok to auto-create indexes
  @Override
  public IndexManager indexManager(Cluster cluster) {
    return new IndexManager(cluster);
  }

  @Override
  protected Consistency getDefaultConsistency() {
    return Consistency.READ_YOUR_OWN_WRITES;
  }
}
