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
import org.junit.jupiter.api.Disabled;
import org.springframework.data.couchbase.transactions.util.TransactionTestUtil;
import org.springframework.transaction.reactive.TransactionalOperator;
import reactor.core.publisher.Mono;

import java.time.Duration;
import java.util.Optional;

import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Configuration;
import org.springframework.dao.DataRetrievalFailureException;
import org.springframework.data.couchbase.CouchbaseClientFactory;
import org.springframework.data.couchbase.config.AbstractCouchbaseConfiguration;
import org.springframework.data.couchbase.core.CouchbaseTemplate;
import org.springframework.data.couchbase.core.ReactiveCouchbaseTemplate;
import org.springframework.data.couchbase.domain.Person;
import org.springframework.data.couchbase.domain.PersonRepository;
import org.springframework.data.couchbase.domain.ReactivePersonRepository;
import org.springframework.data.couchbase.repository.config.EnableCouchbaseRepositories;
import org.springframework.data.couchbase.repository.config.EnableReactiveCouchbaseRepositories;
import org.springframework.data.couchbase.transaction.CouchbaseTransactionManager;
import org.springframework.data.couchbase.transaction.CouchbaseTransactionalOperator;
import org.springframework.data.couchbase.transaction.ReactiveCouchbaseTransactionManager;
import org.springframework.data.couchbase.util.Capabilities;
import org.springframework.data.couchbase.util.ClusterType;
import org.springframework.data.couchbase.util.IgnoreWhen;
import org.springframework.data.couchbase.util.JavaIntegrationTests;
import org.springframework.test.context.junit.jupiter.SpringJUnitConfig;

import com.couchbase.client.core.error.DocumentNotFoundException;
import com.couchbase.client.java.Collection;
import com.couchbase.client.java.ReactiveCollection;
import com.couchbase.client.java.kv.RemoveOptions;
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

	// @Autowired not supported on static fields. These are initialized in beforeAll()
	// Also - @Autowired doesn't work here on couchbaseClientFactory even when it is not static, not sure why - oh, it
	// seems there is not a ReactiveCouchbaseClientFactory bean
	@Autowired CouchbaseClientFactory couchbaseClientFactory;
	@Autowired CouchbaseTransactionManager couchbaseTransactionManager;
	@Autowired ReactiveCouchbaseTransactionManager reactiveCouchbaseTransactionManager;
	@Autowired PersonRepository repo;
	@Autowired ReactivePersonRepository repoRx;
	@Autowired CouchbaseTemplate cbTmpl;
	@Autowired ReactiveCouchbaseTemplate rxCbTmpl;
	@Autowired TransactionalOperator txOperator;
	static String cName; // short name

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
		TransactionTestUtil.assertNotInTransaction();
	}

	@AfterEach
	public void afterEach() {
		TransactionTestUtil.assertNotInTransaction();
	}

	@Test
	public void replacePersonTemplate() {
		Person person = new Person(1, "Walter", "White");
		remove(cbTmpl, cName, person.getId().toString());
		cbTmpl.insertById(Person.class).inCollection(cName).one(person);
		assertThrowsWithCause(
				() -> txOperator.execute((ctx) -> rxCbTmpl.findById(Person.class).one(person.getId().toString()) //
						.flatMap(pp -> rxCbTmpl.replaceById(Person.class).one(pp.withFirstName("Walt")) //
								.map(ppp -> throwSimulateFailureException(ppp))))
						.blockLast(),
				SimulateFailureException.class);

		Person pFound = cbTmpl.findById(Person.class).inCollection(cName).one(person.getId().toString());
		assertEquals("Walter", pFound.getFirstname(), "firstname should be Walter");

	}

	@Test
	public void replacePersonRbTemplate() {
		Person person = new Person(1, "Walter", "White");
		remove(cbTmpl, cName, person.getId().toString());
		cbTmpl.insertById(Person.class).inCollection(cName).one(person);
		TransactionalOperator txOperator = TransactionalOperator.create(reactiveCouchbaseTransactionManager);
		assertThrowsWithCause(
				() -> txOperator.execute((ctx) -> rxCbTmpl.findById(Person.class).one(person.getId().toString()) //
						.flatMap(p -> rxCbTmpl.replaceById(Person.class).one(p.withFirstName("Walt"))) //
						.map(ppp -> throwSimulateFailureException(ppp))).blockLast(), //
				SimulateFailureException.class);
		Person pFound = cbTmpl.findById(Person.class).inCollection(cName).one(person.getId().toString());
		assertEquals("Walter", pFound.getFirstname(), "firstname should be Walter");

	}

	@Test
	public void insertPersonTemplate() {
		Person person = new Person(1, "Walter", "White");
		remove(cbTmpl, cName, person.getId().toString());
		CouchbaseTransactionalOperator txOperator = new CouchbaseTransactionalOperator(reactiveCouchbaseTransactionManager);
		txOperator.reactive((ctx) -> ctx.template(cbTmpl.reactive()).insertById(Person.class).one(person)
				.flatMap(p -> ctx.template(cbTmpl.reactive()).replaceById(Person.class).one(p.withFirstName("Walt"))).then())
				.block();
		Person pFound = cbTmpl.findById(Person.class).inCollection(cName).one(person.getId().toString());
		assertEquals("Walt", pFound.getFirstname(), "firstname should be Walt");
	}

	@Test
	public void insertPersonRbTemplate() {
		Person person = new Person(1, "Walter", "White");
		remove(cbTmpl, cName, person.getId().toString());
		TransactionalOperator txOperator = TransactionalOperator.create(reactiveCouchbaseTransactionManager);
		assertThrowsWithCause(() -> txOperator.execute((ctx) -> rxCbTmpl.insertById(Person.class).one(person)
				.flatMap(p -> rxCbTmpl.replaceById(Person.class).one(p.withFirstName("Walt")))
				.map(it -> throwSimulateFailureException(it))).blockLast(), SimulateFailureException.class);

		Person pFound = cbTmpl.findById(Person.class).inCollection(cName).one(person.getId().toString());
		assertNull(pFound, "Should NOT have found " + pFound);
	}

	@Test
	public void replacePersonRbRepo() {
		Person person = new Person(1, "Walter", "White");
		remove(cbTmpl, cName, person.getId().toString());
		repo.withCollection(cName).save(person);

		CouchbaseTransactionalOperator txOperator = new CouchbaseTransactionalOperator(reactiveCouchbaseTransactionManager);
		assertThrowsWithCause(() -> txOperator.run(ctx -> {
			ctx.repository(repoRx).withCollection(cName).findById(person.getId().toString())
					.flatMap(p -> ctx.repository(repoRx).withCollection(cName).save(p.withFirstName("Walt")));
			throw new PoofException();
		}), TransactionFailedException.class, PoofException.class);

		Person pFound = cbTmpl.findById(Person.class).inCollection(cName).one(person.getId().toString());
		assertEquals(person, pFound, "Should have found " + person);
	}

	@Test
	public void insertPersonRbRepo() {
		Person person = new Person(1, "Walter", "White");
		remove(cbTmpl, cName, person.getId().toString());
		TransactionalOperator txOperator = TransactionalOperator.create(reactiveCouchbaseTransactionManager);
		assertThrowsWithCause(() -> txOperator.execute((ctx) -> repoRx.withCollection(cName).save(person) // insert
				.flatMap(p -> repoRx.withCollection(cName).save(p.withFirstName("Walt"))) // replace
				.map(it -> throwSimulateFailureException(it))).blockLast(), SimulateFailureException.class);

		Person pFound = cbTmpl.findById(Person.class).inCollection(cName).one(person.getId().toString());
		assertNull(pFound, "Should NOT have found " + pFound);
	}

	@Test
	public void insertPersonRepo() {
		Person person = new Person(1, "Walter", "White");
		remove(cbTmpl, cName, person.getId().toString());
		CouchbaseTransactionalOperator txOperator = new CouchbaseTransactionalOperator(reactiveCouchbaseTransactionManager);
		txOperator.reactive((ctx) -> ctx.repository(repoRx).withCollection(cName).save(person) // insert
				.flatMap(p -> ctx.repository(repoRx).withCollection(cName).save(p.withFirstName("Walt"))) // replace
				.then()).block();
		Optional<Person> pFound = repo.withCollection(cName).findById(person.getId().toString());
		assertEquals("Walt", pFound.get().getFirstname(), "firstname should be Walt");
	}

	@Test
	public void replacePersonRbSpringTransactional() {
		Person person = new Person(1, "Walter", "White");
		remove(cbTmpl, cName, person.getId().toString());
		cbTmpl.insertById(Person.class).inCollection(cName).one(person);
		assertThrowsWithCause(() -> txOperator.execute((ctx) -> rxCbTmpl.findById(Person.class)
				.one(person.getId().toString()).flatMap(p -> rxCbTmpl.replaceById(Person.class).one(p.withFirstName("Walt")))
				.map(it -> throwSimulateFailureException(it))).blockLast(), SimulateFailureException.class);
		Person pFound = cbTmpl.findById(Person.class).inCollection(cName).one(person.getId().toString());
		assertEquals("Walter", pFound.getFirstname(), "firstname should be Walter");
	}

	void remove(Collection col, String id) {
		remove(col.reactive(), id);
	}

	void remove(ReactiveCollection col, String id) {
		try {
			col.remove(id, RemoveOptions.removeOptions().timeout(Duration.ofSeconds(10)));
		} catch (DocumentNotFoundException nfe) {
			System.out.println(id + " : " + "DocumentNotFound when deleting");
		}
	}

	void remove(CouchbaseTemplate template, String collection, String id) {
		remove(template.reactive(), collection, id);
	}

	void remove(ReactiveCouchbaseTemplate template, String collection, String id) {
		try {
			template.removeById(Person.class).inCollection(collection).one(id).block();
			System.out.println("removed " + id);
		} catch (DocumentNotFoundException | DataRetrievalFailureException nfe) {
			System.out.println(id + " : " + "DocumentNotFound when deleting");
		}
	}

	static class PoofException extends RuntimeException {};

}
