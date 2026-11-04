/*
 * Copyright 2022-present the original author or authors
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

import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;
import static org.springframework.data.couchbase.transactions.util.TransactionTestUtil.assertNotInTransaction;

import org.springframework.data.couchbase.transactions.TransactionsConfig;
import org.springframework.test.annotation.DirtiesContext;
import reactor.core.publisher.Mono;
import reactor.core.scheduler.Schedulers;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.data.couchbase.CouchbaseClientFactory;
import org.springframework.data.couchbase.core.CouchbaseTemplate;
import org.springframework.data.couchbase.core.ReactiveCouchbaseTemplate;
import org.springframework.data.couchbase.domain.Person;
import org.springframework.data.couchbase.util.Capabilities;
import org.springframework.data.couchbase.util.ClusterType;
import org.springframework.data.couchbase.util.IgnoreWhen;
import org.springframework.data.couchbase.util.JavaIntegrationTests;
import org.springframework.test.context.junit.jupiter.SpringJUnitConfig;

/**
 * Added for issue 1527: Tests running regular SDK transactions (blocking and reactive) on a reactor non-blocking
 * thread.
 *
 * @author Graham Pople
 */
@IgnoreWhen(missesCapabilities = Capabilities.QUERY, clusterTypes = ClusterType.MOCKED)
@SpringJUnitConfig(TransactionsConfig.class)
@DirtiesContext
public class SDKTransactionsNonBlockingThreadIntegrationTests extends JavaIntegrationTests {
	@Autowired private CouchbaseClientFactory couchbaseClientFactory;
	@Autowired private CouchbaseTemplate ops;
	@Autowired private ReactiveCouchbaseTemplate reactiveOps;

	@BeforeEach
	public void beforeEachTest() {
		assertNotInTransaction();
	}

	@AfterEach
	public void afterEachTest() {
		assertNotInTransaction();
	}

	@DisplayName("Trying to run a blocking transaction (or anything blocking) on a non-blocking thread, will not work")
	@Test
	public void blockingTransactionOnNonBlockingThread() {
		try {
			Mono.just(1).publishOn(Schedulers.parallel()).flatMap(ignore -> {
				assertTrue(Schedulers.isInNonBlockingThread());
				assertTrue(Thread.currentThread().getName().contains("parallel"));

				couchbaseClientFactory.getCluster().transactions().run(ctx -> {
					ops.insertById(Person.class).one(new Person("Walter", "White"));
				});
				return Mono.empty();
			}).block();
			fail();
		} catch (IllegalStateException ignored) {}
	}

	@DisplayName("Trying to run a reactive transaction on a non-blocking thread should work")
	@Test
	public void reactiveTransactionOnNonBlockingThread() {
		Mono.just(1).publishOn(Schedulers.parallel()).flatMap(ignore -> {
			return couchbaseClientFactory.getCluster().reactive().transactions().run(ctx -> {
				return reactiveOps.insertById(Person.class).one(new Person("Walter", "White"));
			});
		}).block();
	}
}
