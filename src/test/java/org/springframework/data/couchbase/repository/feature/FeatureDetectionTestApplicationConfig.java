package org.springframework.data.couchbase.repository.feature;

import java.time.Duration;
import java.util.Collections;
import java.util.List;


import com.couchbase.client.core.env.TimeoutConfig;
import com.couchbase.client.java.env.ClusterEnvironment;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.data.couchbase.config.AbstractCouchbaseConfiguration;
import org.springframework.data.couchbase.core.CouchbaseTemplate;
import org.springframework.data.couchbase.core.WriteResultChecking;

@Configuration
public class FeatureDetectionTestApplicationConfig extends AbstractCouchbaseConfiguration {

  @Bean
  public String couchbaseAdminUser() {
    return "Administrator";
  }

  @Bean
  public String couchbaseAdminPassword() {
    return "password";
  }

  @Override
  protected List<String> getBootstrapHosts() {
    return Collections.singletonList("127.0.0.1");
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
  public  ClusterEnvironment couchbaseEnvironment() {
    return ClusterEnvironment.builder()
            .timeoutConfig(
                TimeoutConfig.builder()
                  .connectTimeout(Duration.ofMillis(10000))
                  .kvTimeout(Duration.ofMillis(10000))
                  .queryTimeout(Duration.ofMillis(10000))
                  .viewTimeout(Duration.ofMillis(10000))
            ).build();
  }

  @Override
  public CouchbaseTemplate couchbaseTemplate() throws Exception {
    CouchbaseTemplate template = super.couchbaseTemplate();
    template.setWriteResultChecking(WriteResultChecking.LOG);
    return template;
  }


  //change the name of the field that will hold type information
  @Override
  public String typeKey() {
    return "javaClass";
  }
}
