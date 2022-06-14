/*
 * Copyright 2012-2021 the original author or authors
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

import static com.couchbase.client.java.query.QueryScanConsistency.REQUEST_PLUS;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;

import lombok.Data;
import org.springframework.data.couchbase.core.RemoveResult;
import reactor.core.publisher.Mono;
import reactor.test.StepVerifier;

import java.util.LinkedList;
import java.util.List;

import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.dao.DataRetrievalFailureException;
import org.springframework.data.annotation.Version;
import org.springframework.data.couchbase.CouchbaseClientFactory;
import org.springframework.data.couchbase.core.CouchbaseTemplate;
import org.springframework.data.couchbase.core.ReactiveCouchbaseTemplate;
import org.springframework.data.couchbase.core.query.Query;
import org.springframework.data.couchbase.core.query.QueryCriteria;
import org.springframework.data.couchbase.domain.Person;
import org.springframework.data.couchbase.domain.PersonRepository;
import org.springframework.data.couchbase.domain.ReactivePersonRepository;
import org.springframework.data.couchbase.transactions.util.TransactionTestUtil;
import org.springframework.data.couchbase.util.Capabilities;
import org.springframework.data.couchbase.util.ClusterType;
import org.springframework.data.couchbase.util.IgnoreWhen;
import org.springframework.data.couchbase.util.JavaIntegrationTests;
import org.springframework.test.context.junit.jupiter.SpringJUnitConfig;

import com.couchbase.client.java.Cluster;
import com.couchbase.client.java.transactions.TransactionResult;
import com.couchbase.client.java.transactions.error.TransactionFailedException;

/**
 * Tests for com.couchbase.transactions without using the spring data transactions framework
 * <p>
 * todo gp: these tests are using the `.as(transactionalOperator::transactional)` method which is for the chopping block, so presumably these tests are too
 *
 * @author Michael Reiche
 */
@IgnoreWhen(missesCapabilities = Capabilities.QUERY, clusterTypes = ClusterType.MOCKED)
@SpringJUnitConfig(classes = { TransactionsConfig.class, PersonServiceReactive.class })
public class CouchbasePersonTransactionReactiveIntegrationTests extends JavaIntegrationTests {
	// intellij flags "Could not autowire" when config classes are specified with classes={...}. But they are populated.
	@Autowired CouchbaseClientFactory couchbaseClientFactory;
	@Autowired ReactivePersonRepository rxRepo;
	@Autowired PersonRepository repo;
	@Autowired CouchbaseTemplate cbTmpl;
	@Autowired ReactiveCouchbaseTemplate rxCBTmpl;
	@Autowired Cluster myCluster;
	@Autowired PersonServiceReactive personService;
	@Autowired ReactiveCouchbaseTemplate operations;

	// if these are changed from default, then beforeEach needs to clean up separately
	String sName = "_default";
	String cName = "_default";
	Person WalterWhite;

	@BeforeAll
	public static void beforeAll() {
		callSuperBeforeAll(new Object() {});
	}

	@AfterAll
	public static void afterAll() {
		callSuperAfterAll(new Object() {});
	}

	@BeforeEach
	public void beforeEachTest() {
		WalterWhite = new Person("Walter", "White");
		TransactionTestUtil.assertNotInTransaction();
		List<RemoveResult> pr = operations.removeByQuery(Person.class).withConsistency(REQUEST_PLUS).all().collectList()
				.block();
		List<RemoveResult> er = operations.removeByQuery(EventLog.class).withConsistency(REQUEST_PLUS).all().collectList()
				.block();
		List<Person> p = operations.findByQuery(Person.class).withConsistency(REQUEST_PLUS).all().collectList().block();
		List<EventLog> e = operations.findByQuery(EventLog.class).withConsistency(REQUEST_PLUS).all().collectList().block();
	}

	@Test
	public void shouldRollbackAfterException() {
		personService.savePersonErrors(WalterWhite) //
				.as(StepVerifier::create) //
				.verifyError(TransactionFailedException.class);
		operations.findByQuery(Person.class).withConsistency(REQUEST_PLUS).count() //
				.as(StepVerifier::create) //
				.expectNext(0L) //
				.verifyComplete();
	}

	@Test
	public void shouldRollbackAfterExceptionOfTxAnnotatedMethod() {
		assertThrowsWithCause(() -> personService.declarativeSavePersonErrors(WalterWhite).blockLast(),
				TransactionFailedException.class, SimulateFailureException.class);
	}

	@Test
	public void commitShouldPersistTxEntries() {

		personService.savePerson(WalterWhite) //
				.as(StepVerifier::create) //
				.expectNextCount(1) //
				.verifyComplete();

		operations.findByQuery(Person.class).withConsistency(REQUEST_PLUS).count() //
				.as(StepVerifier::create) //
				.expectNext(1L) //
				.verifyComplete();
	}

	@Test
	/* todo - does this need to be in
	Caused by: java.lang.UnsupportedOperationException: Return type is Mono or Flux, indicating a reactive transaction
	is being performed in a blocking way.  A potential cause is the CouchbaseSimpleTransactionInterceptor is not in use.
	 */
	public void commitShouldPersistTxEntriesOfTxAnnotatedMethod() {

		personService.declarativeSavePerson(WalterWhite).as(StepVerifier::create) //
				.expectNextCount(1) //
				.verifyComplete();

		operations.findByQuery(Person.class).withConsistency(REQUEST_PLUS).count() //
				.as(StepVerifier::create) //
				.expectNext(1L) //
				.verifyComplete();

	}

	@Test
	public void commitShouldPersistTxEntriesAcrossCollections() {

		personService.saveWithLogs(WalterWhite) //
				.then() //
				.as(StepVerifier::create) //
				.verifyComplete();

		operations.findByQuery(Person.class).withConsistency(REQUEST_PLUS).count() //
				.as(StepVerifier::create) //
				.expectNext(1L) //
				.verifyComplete();

		operations.findByQuery(EventLog.class).withConsistency(REQUEST_PLUS).count() //
				.as(StepVerifier::create) //
				.expectNext(4L) //
				.verifyComplete();
	}

	@Test
	public void rollbackShouldAbortAcrossCollections() {

		personService.saveWithErrorLogs(WalterWhite) //
				.then() //
				.as(StepVerifier::create) //
				.verifyError();

		operations.findByQuery(Person.class).withConsistency(REQUEST_PLUS).count() //
				.as(StepVerifier::create) //
				.expectNext(0L) //
				.verifyComplete();

		operations.findByQuery(EventLog.class).withConsistency(REQUEST_PLUS).count()//
				.as(StepVerifier::create) //
				.expectNext(0L) //
				.verifyComplete();
	}

	@Test
	public void countShouldWorkInsideTransaction() {
		personService.countDuringTx(WalterWhite) //
				.as(StepVerifier::create) //
				.expectNext(1L) //
				.verifyComplete();
	}

	@Test
	public void emitMultipleElementsDuringTransaction() {
		personService.saveWithLogs(WalterWhite) //
				.as(StepVerifier::create) //
				.expectNextCount(4L) //
				.verifyComplete();
	}

	@Test
	public void errorAfterTxShouldNotAffectPreviousStep() {

		personService.savePerson(WalterWhite) //
				.then(Mono.error(new SimulateFailureException())).as(StepVerifier::create) //
				.verifyError();
		operations.findByQuery(Person.class).withConsistency(REQUEST_PLUS).count() //
				.as(StepVerifier::create) //
				.expectNext(1L) //
				.verifyComplete();
	}


	@Data
	// @AllArgsConstructor
	static class EventLog {
		public EventLog() {}

		public EventLog(ObjectId oid, String action) {
			this.id = oid.toString();
			this.action = action;
		}

		public EventLog(String id, String action) {
			this.id = id;
			this.action = action;
		}

		String id;
		String action;
		@Version Long version;
	}
}
