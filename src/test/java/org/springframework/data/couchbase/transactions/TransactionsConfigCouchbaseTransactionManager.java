package org.springframework.data.couchbase.transactions;

import com.couchbase.client.java.env.ClusterEnvironment;
import com.couchbase.client.java.transactions.config.TransactionOptions;
import com.couchbase.client.java.transactions.config.TransactionsConfig;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.data.couchbase.CouchbaseClientFactory;
import org.springframework.data.couchbase.ReactiveCouchbaseClientFactory;
import org.springframework.data.couchbase.config.AbstractCouchbaseConfiguration;
import org.springframework.data.couchbase.config.BeanNames;
import org.springframework.data.couchbase.repository.config.EnableCouchbaseRepositories;
import org.springframework.data.couchbase.repository.config.EnableReactiveCouchbaseRepositories;
import org.springframework.data.couchbase.transaction.CouchbaseTransactionDefinition;
import org.springframework.data.couchbase.transaction.CouchbaseTransactionManager;
import org.springframework.data.couchbase.transaction.ReactiveCouchbaseTransactionManager;
import org.springframework.data.couchbase.util.ClusterAwareIntegrationTests;
import org.springframework.transaction.TransactionDefinition;
import org.springframework.transaction.annotation.EnableTransactionManagement;
import org.springframework.transaction.reactive.TransactionalOperator;

import java.time.Duration;

/**
 * There are two rather divergent approaches in the code currently:
 *
 * 1. CouchbaseTransactionManager, ReactiveTransactionManager, CouchbaseTransactionalOperator, CouchbaseTransactionInterceptor
 * 2. CouchbaseSimpleCallbackTransactionManager, CouchbaseSimpleTransactionalOperator, CouchbaseSimpleTransactionInterceptor
 *
 * I know the intent is to remove some aspects of (1), but until that's done it's proving tricky to have tests for
 * both concurrently - I've hit several issues on adding CouchbaseSimpleTransactionInterceptor, with 'multiple
 * transaction manager beans in config' being common.
 *
 * So, temporarily moving some beans from AbstractCouchbaseConfiguration into this Config class, renaming it, and having
 * two separately TransactionsConfig classes for the two approaches.
 *
 * Once we've aligned the approaches more, can move what beans survive back into AbstractCouchbaseConfiguration.
 */
@Configuration
@EnableCouchbaseRepositories("org.springframework.data.couchbase")
@EnableReactiveCouchbaseRepositories("org.springframework.data.couchbase")
@EnableTransactionManagement
public class TransactionsConfigCouchbaseTransactionManager extends AbstractCouchbaseConfiguration {

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

	@Bean(BeanNames.COUCHBASE_TRANSACTION_MANAGER)
	CouchbaseTransactionManager transactionManager(CouchbaseClientFactory clientFactory) {
		return new CouchbaseTransactionManager(clientFactory);
	}

	@Bean(BeanNames.REACTIVE_COUCHBASE_TRANSACTION_MANAGER)
	ReactiveCouchbaseTransactionManager reactiveTransactionManager(
			ReactiveCouchbaseClientFactory reactiveCouchbaseClientFactory) {
		return new ReactiveCouchbaseTransactionManager(reactiveCouchbaseClientFactory);
	}

	@Bean(BeanNames.COUCHBASE_TRANSACTIONAL_OPERATOR)
	public TransactionalOperator transactionOperator(ReactiveCouchbaseTransactionManager reactiveTransactionManager, TransactionDefinition transactionDefinition){
		return 	TransactionalOperator.create(reactiveTransactionManager, transactionDefinition);
	}

	@Bean(BeanNames.COUCHBASE_TRANSACTION_DEFINITION)
	public TransactionDefinition transactionDefinition(){
		return new CouchbaseTransactionDefinition();
	}


}
