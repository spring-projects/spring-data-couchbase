package org.springframework.data.couchbase.repository.auditing;

import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.data.couchbase.IntegrationTestApplicationConfig;
import org.springframework.data.couchbase.repository.config.EnableCouchbaseRepositories;

@Configuration
@EnableCouchbaseRepositories
@EnableCouchbaseAuditing(modifyOnCreate = false)
public class AuditedApplicationConfig extends IntegrationTestApplicationConfig {

  @Bean
  public AuditedAuditorAware couchbaseAuditorAware() {
    return new AuditedAuditorAware();
  }
}
