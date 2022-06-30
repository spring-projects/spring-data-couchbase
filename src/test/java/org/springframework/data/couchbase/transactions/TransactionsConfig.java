/*
 * Copyright 2022 the original author or authors
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *        https://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.springframework.data.couchbase.transactions;

import java.time.Duration;

import org.springframework.context.annotation.Configuration;
import org.springframework.data.couchbase.config.AbstractCouchbaseConfiguration;
import org.springframework.data.couchbase.repository.config.EnableCouchbaseRepositories;
import org.springframework.data.couchbase.repository.config.EnableReactiveCouchbaseRepositories;
import org.springframework.data.couchbase.util.ClusterAwareIntegrationTests;
import org.springframework.transaction.annotation.EnableTransactionManagement;

import com.couchbase.client.java.env.ClusterEnvironment;

/**
 * For testing transactions
 *
 * @author Michael Reiche
 */
@Configuration
@EnableCouchbaseRepositories("org.springframework.data.couchbase")
@EnableReactiveCouchbaseRepositories("org.springframework.data.couchbase")
@EnableTransactionManagement
public class TransactionsConfig extends AbstractCouchbaseConfiguration {

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
	public void configureEnvironment(ClusterEnvironment.Builder builder) {
		// twenty minutes for debugging in the debugger.
		builder.transactionsConfig(
				com.couchbase.client.java.transactions.config.TransactionsConfig.builder().timeout(Duration.ofMinutes(20)));
	}

}
