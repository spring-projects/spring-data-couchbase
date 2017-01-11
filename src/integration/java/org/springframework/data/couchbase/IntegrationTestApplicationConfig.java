package org.springframework.data.couchbase;

import java.util.Collections;
import java.util.List;

import com.couchbase.client.java.env.CouchbaseEnvironment;
import com.couchbase.client.java.env.DefaultCouchbaseEnvironment;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.core.env.Environment;
import org.springframework.data.couchbase.config.AbstractCouchbaseConfiguration;
import org.springframework.data.couchbase.config.CouchbaseConfigurer;
import org.springframework.data.couchbase.core.CouchbaseTemplate;
import org.springframework.data.couchbase.core.WriteResultChecking;
import org.springframework.data.couchbase.core.query.Consistency;
import org.springframework.data.couchbase.repository.support.IndexManager;

@Configuration
public class IntegrationTestApplicationConfig extends AbstractCouchbaseConfiguration {

  @Autowired
  private Environment springEnv;

  @Bean
  public String couchbaseAdminUser() {
    return springEnv.getProperty("couchbase.adminUser", "Administrator");
  }

  @Bean
  public String couchbaseAdminPassword() {
    return springEnv.getProperty("couchbase.adminUser", "password");
  }

  @Override
  protected List<String> getBootstrapHosts() {
    return Collections.singletonList(springEnv.getProperty("couchbase.host", "127.0.0.1"));
  }

  @Override
  protected String getBucketName() {
    return springEnv.getProperty("couchbase.bucket", "default");
  }

  @Override
  protected String getBucketPassword() {
    return springEnv.getProperty("couchbase.password", "");
  }


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

  //this is for dev so it is ok to auto-create indexes
  @Override
  public IndexManager indexManager() {
    return new IndexManager();
  }

  @Override
  protected Consistency getDefaultConsistency() {
    return Consistency.READ_YOUR_OWN_WRITES;
  }


  @Override
  protected CouchbaseConfigurer couchbaseConfigurer() {
    return this;
  }
}
