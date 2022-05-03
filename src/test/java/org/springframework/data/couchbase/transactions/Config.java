package org.springframework.data.couchbase.transactions;

import com.couchbase.client.core.cnc.Event;
import com.couchbase.transactions.TransactionDurabilityLevel;
import com.couchbase.transactions.config.TransactionConfig;
import com.couchbase.transactions.config.TransactionConfigBuilder;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Configuration;
import org.springframework.data.couchbase.config.AbstractCouchbaseConfiguration;
import org.springframework.data.couchbase.repository.config.EnableCouchbaseRepositories;
import org.springframework.data.couchbase.repository.config.EnableReactiveCouchbaseRepositories;
import org.springframework.data.couchbase.util.ClusterAwareIntegrationTests;
import org.springframework.transaction.annotation.EnableTransactionManagement;
import org.springframework.transaction.annotation.Transactional;

import java.time.Duration;

@Configuration
@EnableCouchbaseRepositories("org.springframework.data.couchbase")
@EnableReactiveCouchbaseRepositories("org.springframework.data.couchbase")
@EnableTransactionManagement
class Config extends AbstractCouchbaseConfiguration {

  @Override
  public String getConnectionString() {
    return ClusterAwareIntegrationTests.connectionString();
  }

  @Override
  public String getUserName() {
    return ClusterAwareIntegrationTests.config()
        .adminUsername();
  }

  @Override
  public String getPassword() {
    return ClusterAwareIntegrationTests.config()
        .adminPassword();
  }

  @Override
  public String getBucketName() {
    return ClusterAwareIntegrationTests.bucketName();
  }

  @Override
  public TransactionConfig transactionConfig() {
    // expirationTime 20 minutes for stepping with the debugger
    return TransactionConfigBuilder.create()
        .logDirectly(Event.Severity.INFO)
        .logOnFailure(true,
            Event.Severity.ERROR)
        .expirationTime(Duration.ofMinutes(20))
        .durabilityLevel(TransactionDurabilityLevel.MAJORITY)
        .build();
  }

  /*
      beforeAll creates a PersonService bean in the applicationContext

      context = new AnnotationConfigApplicationContext(CouchbasePersonTransactionIntegrationTests.Config.class,
      PersonService.class);

      @Bean("personService")
          PersonService getPersonService(CouchbaseOperations ops, CouchbaseTransactionManager mgr,
                                         ReactiveCouchbaseOperations opsRx, ReactiveCouchbaseTransactionManager mgrRx) {
            return new PersonService(ops, mgr, opsRx, mgrRx);
          }
  */

}
