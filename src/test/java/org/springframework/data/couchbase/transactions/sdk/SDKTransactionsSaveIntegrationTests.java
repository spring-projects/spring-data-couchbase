/*
 * Copyright 2022-2025 the original author or authors
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

package org.springframework.data.couchbase.transactions.sdk;

import static org.springframework.data.couchbase.transactions.util.TransactionTestUtil.assertInReactiveTransaction;
import static org.springframework.data.couchbase.transactions.util.TransactionTestUtil.assertInTransaction;
import static org.springframework.data.couchbase.transactions.util.TransactionTestUtil.assertNotInReactiveTransaction;
import static org.springframework.data.couchbase.transactions.util.TransactionTestUtil.assertNotInTransaction;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.data.couchbase.CouchbaseClientFactory;
import org.springframework.data.couchbase.core.CouchbaseTemplate;
import org.springframework.data.couchbase.core.ReactiveCouchbaseTemplate;
import org.springframework.data.couchbase.domain.PersonWithoutVersion;
import org.springframework.data.couchbase.transactions.TransactionsConfig;
import org.springframework.data.couchbase.util.Capabilities;
import org.springframework.data.couchbase.util.ClusterType;
import org.springframework.data.couchbase.util.IgnoreWhen;
import org.springframework.data.couchbase.util.JavaIntegrationTests;
import org.springframework.test.annotation.DirtiesContext;
import org.springframework.test.context.junit.jupiter.SpringJUnitConfig;

/**
 * Added for issue 1527: Tests the .save() command.
 *
 * @author Graham Pople
 */
@IgnoreWhen(missesCapabilities = Capabilities.QUERY, clusterTypes = ClusterType.MOCKED)
@SpringJUnitConfig(TransactionsConfig.class)
@DirtiesContext
public class SDKTransactionsSaveIntegrationTests extends JavaIntegrationTests {
	@Autowired private CouchbaseClientFactory couchbaseClientFactory;
	@Autowired private CouchbaseTemplate ops;
	@Autowired private ReactiveCouchbaseTemplate reactiveOps;

	@BeforeEach
	public void beforeEachTest() {
        super.beforeEach();
		assertNotInTransaction();
		assertNotInReactiveTransaction();
	}

	@AfterEach
	public void afterEachTest() {
		assertNotInTransaction();
		assertNotInReactiveTransaction();
	}


	@DisplayName("ReactiveCouchbaseTemplate.save() called inside a reactive SDK transaction should work")
	@Test
	public void reactiveSaveInReactiveTransaction() {
        logMessage("xxx reactiveSaveInReactiveTransaction");
		couchbaseClientFactory.getCluster().reactive().transactions().run(ctx -> {
			PersonWithoutVersion p = new PersonWithoutVersion("Walter", "White");
			return reactiveOps.save(p).then(assertInReactiveTransaction());
		}).block();
	}

	@DisplayName("ReactiveCouchbaseTemplate.save().block() called inside a non-reactive SDK transaction should work")
	@Test
	public void reactiveSaveInBlockingTransaction() {
		couchbaseClientFactory.getCluster().transactions().run(ctx -> {
			assertInTransaction();
			PersonWithoutVersion p = new PersonWithoutVersion("Walter", "White");
			reactiveOps.save(p).block();
		});
	}

	// This should not work because ops.save(p) calls block() so everything in that call
	// does not have the reactive context (which has the transaction context)
	// what happens is the ops.save(p) is not in a transaction. (it will call upsert instead of insert)
	@DisplayName("ReactiveCouchbaseTemplate.save() called inside a reactive SDK transaction should work")
	@Test
	public void blockingSaveInReactiveTransaction() {
		couchbaseClientFactory.getCluster().reactive().transactions().run(ctx -> {
			PersonWithoutVersion p = new PersonWithoutVersion("Walter", "White");
			ops.save(p);
			return assertInReactiveTransaction();
		}).block();
	}

	@DisplayName("ReactiveCouchbaseTemplate.save().block() called inside a non-reactive SDK transaction should work")
	@Test
	public void blockingSaveInBlockingTransaction() {
		couchbaseClientFactory.getCluster().transactions().run(ctx -> {
			assertInTransaction();
			PersonWithoutVersion p = new PersonWithoutVersion("Walter", "White");
			ops.save(p);
		});
	}
}
