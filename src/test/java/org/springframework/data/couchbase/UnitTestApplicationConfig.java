package org.springframework.data.couchbase;

import java.util.Collections;
import java.util.List;

import com.couchbase.client.java.Collection;
import com.couchbase.client.java.ReactiveCluster;
import com.couchbase.client.java.ReactiveCollection;
import com.couchbase.client.java.kv.InsertOptions;
import com.couchbase.client.java.kv.MutationResult;
import com.couchbase.client.java.kv.ReplaceOptions;
import com.couchbase.client.java.kv.UpsertOptions;
import org.mockito.Mockito;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.data.couchbase.config.AbstractCouchbaseConfiguration;
import org.springframework.data.couchbase.core.CouchbaseTemplate;
import org.springframework.data.couchbase.core.WriteResultChecking;
import org.springframework.data.couchbase.core.query.Consistency;
import org.springframework.data.couchbase.repository.support.IndexManager;

import com.couchbase.client.java.Cluster;
import reactor.core.publisher.Mono;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

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
  protected String getPassword() {
    return "someBucketPassword";
  }

  @Override
  public Cluster couchbaseCluster() throws Exception {
    Cluster mockedCluster = mock(Cluster.class);
    ReactiveCluster mockedReactiveCluster = mock(ReactiveCluster.class);
    when(mockedCluster.reactive()).thenReturn(mockedReactiveCluster);
    return mockedCluster;
  }

  @Override
  public Collection couchbaseClient() throws Exception {
    MutationResult mockedMutationResult = mock(MutationResult.class);
    Collection mockedCollection = mock(Collection.class);
    ReactiveCollection mockedReactiveCollection = mock(ReactiveCollection.class);
    when(mockedReactiveCollection.insert(anyString(), any(), any(InsertOptions.class)))
            .thenReturn(Mono.just(mockedMutationResult));
    when(mockedReactiveCollection.upsert(anyString(), any(), any(UpsertOptions.class)))
            .thenReturn(Mono.just(mockedMutationResult));
    when(mockedReactiveCollection.replace(anyString(), any(), any(ReplaceOptions.class)))
            .thenReturn(Mono.just(mockedMutationResult));
    when(mockedCollection.reactive()).thenReturn(mockedReactiveCollection);
    return mockedCollection;
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
