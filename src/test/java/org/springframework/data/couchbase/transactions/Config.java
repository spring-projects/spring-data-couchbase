package org.springframework.data.couchbase.transactions;

import java.time.Duration;

import com.couchbase.client.core.msg.kv.DurabilityLevel;
import com.couchbase.client.core.transaction.config.CoreTransactionsConfig;
import com.couchbase.client.java.transactions.config.TransactionsConfig;
import org.springframework.context.annotation.Configuration;
import org.springframework.data.couchbase.config.AbstractCouchbaseConfiguration;
import org.springframework.data.couchbase.repository.config.EnableCouchbaseRepositories;
import org.springframework.data.couchbase.repository.config.EnableReactiveCouchbaseRepositories;
import org.springframework.data.couchbase.util.ClusterAwareIntegrationTests;
import org.springframework.transaction.annotation.EnableTransactionManagement;

import com.couchbase.client.java.transactions.config.TransactionOptions;

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
		return ClusterAwareIntegrationTests.config().adminUsername();
	}

	@Override
	public String getPassword() {
		return ClusterAwareIntegrationTests.config().adminPassword();
	}

	@Override
	public String getBucketName() {
		return ClusterAwareIntegrationTests.bucketName();
	}

	@Override
	public TransactionsConfig.Builder transactionsConfig() {
		return TransactionsConfig.builder().durabilityLevel(DurabilityLevel.NONE).timeout(Duration.ofMinutes(20));// for testing
	}

	/*
	@Override
	public TransactionsConfig transactionConfig() {
	  // expirationTime 20 minutes for stepping with the debugger
	  return TransactionsConfig.create()
	      .logDirectly(Event.Severity.INFO)
	      .logOnFailure(true,
	          Event.Severity.ERROR)
	      .expirationTime(Duration.ofMinutes(20))
	      .durabilityLevel(TransactionDurabilityLevel.MAJORITY)
	      .build();
	}
	*/
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
