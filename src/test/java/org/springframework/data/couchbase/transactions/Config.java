package org.springframework.data.couchbase.transactions;

import com.couchbase.client.java.env.ClusterEnvironment;
import com.couchbase.client.java.transactions.config.TransactionOptions;
import com.couchbase.client.java.transactions.config.TransactionsConfig;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.data.couchbase.config.AbstractCouchbaseConfiguration;
import org.springframework.data.couchbase.repository.config.EnableCouchbaseRepositories;
import org.springframework.data.couchbase.repository.config.EnableReactiveCouchbaseRepositories;
import org.springframework.data.couchbase.util.ClusterAwareIntegrationTests;
import org.springframework.transaction.annotation.EnableTransactionManagement;

import java.time.Duration;


@Configuration
@EnableCouchbaseRepositories("org.springframework.data.couchbase")
@EnableReactiveCouchbaseRepositories("org.springframework.data.couchbase")
@EnableTransactionManagement
public class Config extends AbstractCouchbaseConfiguration {

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
	@Bean
	public TransactionOptions transactionsOptions(){
		// twenty minutes for debugging in the debugger.
		return TransactionOptions.transactionOptions().timeout(Duration.ofMinutes(20));
	}

	@Override
	public void configureTransactions(ClusterEnvironment.Builder builder) {
		// twenty minutes for debugging in the debugger.
		builder.transactionsConfig(TransactionsConfig.builder().timeout(Duration.ofMinutes(20)));
	}

}
