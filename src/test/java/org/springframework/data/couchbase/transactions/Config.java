package org.springframework.data.couchbase.transactions;

import org.springframework.context.annotation.Configuration;
import org.springframework.data.couchbase.config.AbstractCouchbaseConfiguration;
import org.springframework.data.couchbase.repository.config.EnableCouchbaseRepositories;
import org.springframework.data.couchbase.repository.config.EnableReactiveCouchbaseRepositories;
import org.springframework.data.couchbase.util.ClusterAwareIntegrationTests;
import org.springframework.transaction.annotation.EnableTransactionManagement;


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
}
