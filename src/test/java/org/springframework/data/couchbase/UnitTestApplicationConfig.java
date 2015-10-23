package org.springframework.data.couchbase;

import static org.mockito.Mockito.when;

import java.util.Collections;
import java.util.List;

import com.couchbase.client.java.Bucket;
import com.couchbase.client.java.Cluster;
import com.couchbase.client.java.CouchbaseBucket;
import com.couchbase.client.java.CouchbaseCluster;
import com.couchbase.client.java.cluster.ClusterInfo;
import com.couchbase.client.java.cluster.DefaultClusterInfo;
import com.couchbase.client.java.util.features.CouchbaseFeature;
import com.couchbase.client.java.util.features.Version;
import org.mockito.Mockito;

import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.data.couchbase.config.AbstractCouchbaseConfiguration;
import org.springframework.data.couchbase.core.CouchbaseTemplate;
import org.springframework.data.couchbase.core.WriteResultChecking;
import org.springframework.data.couchbase.core.query.Consistency;

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
    return Mockito.mock(CouchbaseCluster.class);
  }

  @Override
  public ClusterInfo couchbaseClusterInfo() {
    DefaultClusterInfo mock = Mockito.mock(DefaultClusterInfo.class);
    when(mock.checkAvailable(CouchbaseFeature.N1QL)).thenReturn(true);
    when(mock.getMinVersion()).thenReturn(new Version(4, 0, 0));
    return mock;
  }

  @Override
  public Bucket couchbaseClient() throws Exception {
    return Mockito.mock(CouchbaseBucket.class);
  }

  @Override
  public CouchbaseTemplate couchbaseTemplate() throws Exception {
    CouchbaseTemplate template = super.couchbaseTemplate();
    template.setWriteResultChecking(WriteResultChecking.LOG);
    return template;
  }

  @Override
  protected Consistency getDefaultConsistency() {
    return Consistency.READ_YOUR_OWN_WRITES;
  }
}
