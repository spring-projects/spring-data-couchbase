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

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.springframework.data.couchbase.transactions.util.TransactionTestUtil;
import org.springframework.transaction.TransactionManager;
import org.springframework.transaction.reactive.TransactionalOperator;

import java.util.Optional;

import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.data.couchbase.CouchbaseClientFactory;
import org.springframework.data.couchbase.core.CouchbaseTemplate;
import org.springframework.data.couchbase.core.ReactiveCouchbaseTemplate;
import org.springframework.data.couchbase.domain.Person;
import org.springframework.data.couchbase.domain.PersonRepository;
import org.springframework.data.couchbase.domain.ReactivePersonRepository;
import org.springframework.data.couchbase.util.Capabilities;
import org.springframework.data.couchbase.util.ClusterType;
import org.springframework.data.couchbase.util.IgnoreWhen;
import org.springframework.data.couchbase.util.JavaIntegrationTests;
import org.springframework.test.context.junit.jupiter.SpringJUnitConfig;

import com.couchbase.client.java.transactions.error.TransactionFailedException;

/**
 * Tests for com.couchbase.transactions without using the spring data transactions framework
 *
 * @author Michael Reiche
 */
@IgnoreWhen(missesCapabilities = Capabilities.QUERY, clusterTypes = ClusterType.MOCKED)
@SpringJUnitConfig(TransactionsConfigCouchbaseTransactionManager.class)
// I think these are all redundant (see CouchbaseReactiveTransactionNativeTests). There does not seem to be a blocking
// form of TransactionalOperator. Also there does not seem to be a need for a CouchbaseTransactionalOperator as
// TransactionalOperator.create(reactiveCouchbaseTransactionManager) seems to work just fine. (I don't recall what
// merits the "Native" in the name).
// @Disabled("gp: disabling as these use CouchbaseTransactionalOperator which I've done broke (but also feel we should
// remove)")
public class CouchbaseTransactionNativeTests extends JavaIntegrationTests {
	@Autowired CouchbaseClientFactory couchbaseClientFactory;
	@Autowired TransactionManager couchbaseTransactionManager;
	@Autowired PersonRepository repo;
	@Autowired ReactivePersonRepository repoRx;
	@Autowired CouchbaseTemplate cbTmpl;
	@Autowired ReactiveCouchbaseTemplate rxCbTmpl;
	@Autowired TransactionalOperator txOperator;
	static String cName; // short name

	Person WalterWhite;

	@BeforeAll
	public static void beforeAll() {
		callSuperBeforeAll(new Object() {});
		// short names
		cName = null;// cName;
	}

	@AfterAll
	public static void afterAll() {
		callSuperAfterAll(new Object() {});
	}

	@BeforeEach
	public void beforeEach() {
		WalterWhite = new Person("Walter", "White");
		TransactionTestUtil.assertNotInTransaction();
	}

	@AfterEach
	public void afterEach() {
		TransactionTestUtil.assertNotInTransaction();
	}

	@Test
	public void replacePersonTemplate() {
		Person person = cbTmpl.insertById(Person.class).inCollection(cName).one(WalterWhite);
		assertThrowsWithCause(() -> txOperator.execute((ctx) -> rxCbTmpl.findById(Person.class).one(person.id()) //
				.flatMap(pp -> rxCbTmpl.replaceById(Person.class).one(pp.withIdFirstname()) //
						.map(ppp -> throwSimulateFailureException(ppp))))
				.blockLast(), TransactionFailedException.class, SimulateFailureException.class);

		Person pFound = cbTmpl.findById(Person.class).inCollection(cName).one(person.getId().toString());
		assertEquals(person.getFirstname(), pFound.getFirstname(), "firstname should be " + person.getFirstname());

	}

	@Test
	public void replacePersonRbTemplate() {
		Person person = cbTmpl.insertById(Person.class).inCollection(cName).one(WalterWhite);
		assertThrowsWithCause(
				() -> txOperator.execute((ctx) -> rxCbTmpl.findById(Person.class).one(person.getId().toString()) //
						.flatMap(p -> rxCbTmpl.replaceById(Person.class).one(p.withIdFirstname())) //
						.map(ppp -> throwSimulateFailureException(ppp))).blockLast(), //
				TransactionFailedException.class, SimulateFailureException.class);
		Person pFound = cbTmpl.findById(Person.class).inCollection(cName).one(person.getId().toString());
		assertEquals(person.getFirstname(), pFound.getFirstname(), "firstname should be " + person.getFirstname());

	}

	@Test
	public void insertPersonTemplate() {
		txOperator.execute((ctx) -> rxCbTmpl.insertById(Person.class).one(WalterWhite)
				.flatMap(p -> rxCbTmpl.replaceById(Person.class).one(p.withFirstName("Walt")))).blockLast();
		Person pFound = cbTmpl.findById(Person.class).inCollection(cName).one(WalterWhite.id());
		assertEquals("Walt", pFound.getFirstname(), "firstname should be Walt");
	}

	@Test
	public void insertPersonRbTemplate() {
		assertThrowsWithCause(
				() -> txOperator.execute((ctx) -> rxCbTmpl.insertById(Person.class).one(WalterWhite)
						.flatMap(p -> rxCbTmpl.replaceById(Person.class).one(p.withFirstName("Walt")))
						.map(it -> throwSimulateFailureException(it))).blockLast(),
				TransactionFailedException.class, SimulateFailureException.class);

		Person pFound = cbTmpl.findById(Person.class).inCollection(cName).one(WalterWhite.id());
		assertNull(pFound, "Should NOT have found " + pFound);
	}

	@Test
	public void replacePersonRbRepo() {
		Person person = repo.withCollection(cName).save(WalterWhite);
		assertThrowsWithCause(() -> txOperator.execute(ctx -> {
			return repoRx.withCollection(cName).findById(person.id())
					.flatMap(p -> repoRx.withCollection(cName).save(p.withFirstName("Walt")))
					.map(pp -> throwSimulateFailureException(pp));
		}).blockLast(), TransactionFailedException.class, SimulateFailureException.class);

		Person pFound = cbTmpl.findById(Person.class).inCollection(cName).one(person.id());
		assertEquals(person, pFound, "Should have found " + person);
	}

	@Test
	public void insertPersonRbRepo() {
		assertThrowsWithCause(() -> txOperator.execute((ctx) -> repoRx.withCollection(cName).save(WalterWhite) // insert
				.flatMap(p -> repoRx.withCollection(cName).save(p.withFirstName("Walt"))) // replace
				.map(it -> throwSimulateFailureException(it))).blockLast(), TransactionFailedException.class,
				SimulateFailureException.class);

		Person pFound = cbTmpl.findById(Person.class).inCollection(cName).one(WalterWhite.id());
		assertNull(pFound, "Should NOT have found " + pFound);
	}

	@Test
	public void insertPersonRepo() {
		txOperator.execute((ctx) -> repoRx.withCollection(cName).save(WalterWhite) // insert
				.flatMap(p -> repoRx.withCollection(cName).save(p.withFirstName("Walt"))) // replace
		).blockFirst();
		Optional<Person> pFound = repo.withCollection(cName).findById(WalterWhite.id());
		assertEquals("Walt", pFound.get().getFirstname(), "firstname should be Walt");
	}

	@Test
	public void replacePersonRbSpringTransactional() {
		Person person = cbTmpl.insertById(Person.class).inCollection(cName).one(WalterWhite);
		assertThrowsWithCause(
				() -> txOperator.execute((ctx) -> rxCbTmpl.findById(Person.class).one(person.getId().toString())
						.flatMap(p -> rxCbTmpl.replaceById(Person.class).one(p.withFirstName("Walt")))
						.map(it -> throwSimulateFailureException(it))).blockLast(),
				TransactionFailedException.class, SimulateFailureException.class);
		Person pFound = cbTmpl.findById(Person.class).inCollection(cName).one(person.id());
		assertEquals(person.getFirstname(), pFound.getFirstname(), "firstname should be Walter");
	}

}
