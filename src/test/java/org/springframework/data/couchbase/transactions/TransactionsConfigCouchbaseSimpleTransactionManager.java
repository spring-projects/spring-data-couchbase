package org.springframework.data.couchbase.transactions;

import java.time.Duration;

import org.springframework.beans.factory.config.BeanDefinition;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Role;
import org.springframework.data.couchbase.CouchbaseClientFactory;
import org.springframework.data.couchbase.ReactiveCouchbaseClientFactory;
import org.springframework.data.couchbase.config.AbstractCouchbaseConfiguration;
import org.springframework.data.couchbase.config.BeanNames;
import org.springframework.data.couchbase.repository.config.EnableCouchbaseRepositories;
import org.springframework.data.couchbase.repository.config.EnableReactiveCouchbaseRepositories;
import org.springframework.data.couchbase.transaction.CouchbaseSimpleCallbackTransactionManager;
import org.springframework.data.couchbase.transaction.CouchbaseSimpleTransactionInterceptor;
import org.springframework.data.couchbase.transaction.CouchbaseTransactionManager;
import org.springframework.data.couchbase.transaction.ReactiveCouchbaseTransactionManager;
import org.springframework.data.couchbase.util.ClusterAwareIntegrationTests;
import org.springframework.transaction.TransactionManager;
import org.springframework.transaction.annotation.AnnotationTransactionAttributeSource;
import org.springframework.transaction.annotation.EnableTransactionManagement;

import com.couchbase.client.java.env.ClusterEnvironment;
import com.couchbase.client.java.transactions.config.TransactionOptions;
import com.couchbase.client.java.transactions.config.TransactionsConfig;
import org.springframework.transaction.config.TransactionManagementConfigUtils;
import org.springframework.transaction.interceptor.BeanFactoryTransactionAttributeSourceAdvisor;
import org.springframework.transaction.interceptor.TransactionAttributeSource;
import org.springframework.transaction.interceptor.TransactionInterceptor;

/**
 * See comments on TransactionsConfigCouchbaseTransactionManager for why this test exists.
 */
@Configuration
@EnableCouchbaseRepositories("org.springframework.data.couchbase")
@EnableReactiveCouchbaseRepositories("org.springframework.data.couchbase")
@EnableTransactionManagement
public class TransactionsConfigCouchbaseSimpleTransactionManager extends AbstractCouchbaseConfiguration {

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

	@Bean(BeanNames.COUCHBASE_SIMPLE_CALLBACK_TRANSACTION_MANAGER)
	CouchbaseSimpleCallbackTransactionManager callbackTransactionManager(ReactiveCouchbaseClientFactory clientFactory) {
		return new CouchbaseSimpleCallbackTransactionManager(clientFactory);
	}

	@Bean
	@Role(BeanDefinition.ROLE_INFRASTRUCTURE)
	public TransactionInterceptor transactionInterceptor(TransactionAttributeSource transactionAttributeSource,
														 TransactionManager txManager) {
		TransactionInterceptor interceptor = new CouchbaseSimpleTransactionInterceptor(txManager, transactionAttributeSource);
		interceptor.setTransactionAttributeSource(transactionAttributeSource);
		if (txManager != null) {
			interceptor.setTransactionManager(txManager);
		}
		return interceptor;
	}

	// todo gp find for sure if we need this
	@Bean
	@Role(BeanDefinition.ROLE_INFRASTRUCTURE)
	public TransactionAttributeSource transactionAttributeSource() {
		return new AnnotationTransactionAttributeSource();
	}

	// todo gp find for sure if we need this
	@Bean(name = TransactionManagementConfigUtils.TRANSACTION_ADVISOR_BEAN_NAME)
	@Role(BeanDefinition.ROLE_INFRASTRUCTURE)
	public BeanFactoryTransactionAttributeSourceAdvisor transactionAdvisor(
			TransactionAttributeSource transactionAttributeSource, TransactionInterceptor transactionInterceptor) {

		BeanFactoryTransactionAttributeSourceAdvisor advisor = new BeanFactoryTransactionAttributeSourceAdvisor();
		advisor.setTransactionAttributeSource(transactionAttributeSource);
		advisor.setAdvice(transactionInterceptor);
		// if (this.enableTx != null) {
		// advisor.setOrder(this.enableTx.<Integer>getNumber("order"));
		// }
		return advisor;
	}
}
