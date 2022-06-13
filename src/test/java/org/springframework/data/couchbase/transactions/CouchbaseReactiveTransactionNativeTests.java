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
import static org.junit.jupiter.api.Assertions.assertNull;

import com.couchbase.client.java.transactions.error.TransactionFailedException;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import java.util.List;

import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.data.couchbase.CouchbaseClientFactory;
import org.springframework.data.couchbase.core.CouchbaseTemplate;
import org.springframework.data.couchbase.core.ReactiveCouchbaseTemplate;
import org.springframework.data.couchbase.core.RemoveResult;
import org.springframework.data.couchbase.domain.Person;
import org.springframework.data.couchbase.domain.PersonRepository;
import org.springframework.data.couchbase.domain.ReactivePersonRepository;
import org.springframework.data.couchbase.transactions.util.TransactionTestUtil;
import org.springframework.data.couchbase.util.Capabilities;
import org.springframework.data.couchbase.util.ClusterType;
import org.springframework.data.couchbase.util.IgnoreWhen;
import org.springframework.data.couchbase.util.JavaIntegrationTests;
import org.springframework.test.context.junit.jupiter.SpringJUnitConfig;
import org.springframework.transaction.reactive.TransactionalOperator;

/**
 * Tests for com.couchbase.transactions without using the spring data transactions framework
 *
 * @author Michael Reiche
 */
@IgnoreWhen(missesCapabilities = Capabilities.QUERY, clusterTypes = ClusterType.MOCKED)
@SpringJUnitConfig(TransactionsConfig.class)
// @Disabled("gp: disabling as these use CouchbaseTransactionalOperator which I've done broke (but also feel we should
// remove)")
public class CouchbaseReactiveTransactionNativeTests extends JavaIntegrationTests {

	@Autowired CouchbaseClientFactory couchbaseClientFactory;
	@Autowired ReactivePersonRepository rxRepo;
	@Autowired PersonRepository repo;
	@Autowired CouchbaseTemplate cbTmpl;
	@Autowired ReactiveCouchbaseTemplate rxCBTmpl;
	@Autowired ReactiveCouchbaseTemplate operations;
	@Autowired TransactionalOperator txOperator;

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
		TransactionTestUtil.assertNotInTransaction();
		List<RemoveResult> rp0 = cbTmpl.removeByQuery(Person.class).withConsistency(REQUEST_PLUS).all();
		List<RemoveResult> rp1 = cbTmpl.removeByQuery(Person.class).inScope(sName).inCollection(cName)
				.withConsistency(REQUEST_PLUS).all();
		List<Person> p0 = cbTmpl.findByQuery(Person.class).withConsistency(REQUEST_PLUS).all();
		List<Person> p1 = cbTmpl.findByQuery(Person.class).inScope(sName).inCollection(cName).withConsistency(REQUEST_PLUS)
				.all();
	}

	@Test
	public void replacePersonTemplate() {
		Person person = rxCBTmpl.insertById(Person.class).inCollection(cName).one(WalterWhite).block();
		Flux<Person> result = txOperator.execute((ctx) -> rxCBTmpl.findById(Person.class).one(person.id())
				.flatMap(p -> rxCBTmpl.replaceById(Person.class).one(p.withFirstName("Walt"))));
		result.blockLast();
		Person pFound = rxCBTmpl.findById(Person.class).inCollection(cName).one(person.id()).block();
		assertEquals("Walt", pFound.getFirstname(), "firstname should be Walt");
	}

	@Test
	public void replacePersonRbTemplate() {
		Person person = rxCBTmpl.insertById(Person.class).inCollection(cName).one(WalterWhite).block();
		Flux<Person> result = txOperator.execute((ctx) -> rxCBTmpl.findById(Person.class).one(person.id())
				.flatMap(p -> rxCBTmpl.replaceById(Person.class).one(p.withFirstName("Walt")))
				.map(it -> throwSimulateFailureException(it)));
		assertThrowsWithCause(result::blockLast, TransactionFailedException.class, SimulateFailureException.class);
		Person pFound = rxCBTmpl.findById(Person.class).inCollection(cName).one(person.id()).block();
		assertEquals(person, pFound, "Should have found " + person);
	}

	@Test
	public void insertPersonTemplate() {
		Person person = WalterWhite;
		Flux<Person> result = txOperator.execute((ctx) -> rxCBTmpl.insertById(Person.class).one(person)
				.flatMap(p -> rxCBTmpl.replaceById(Person.class).one(p.withFirstName("Walt"))));
		result.blockLast();
		Person pFound = rxCBTmpl.findById(Person.class).inCollection(cName).one(person.id()).block();
		assertEquals("Walt", pFound.getFirstname(), "firstname should be Walt");
	}

	@Test
	public void insertPersonRbTemplate() {
		Person person = WalterWhite;
		Flux<Person> result = txOperator.execute((ctx) -> rxCBTmpl.insertById(Person.class).one(person)
				.flatMap(p -> rxCBTmpl.replaceById(Person.class).one(p.withFirstName("Walt")))
				.map(it -> throwSimulateFailureException(it)));
		assertThrowsWithCause(result::blockLast, TransactionFailedException.class, SimulateFailureException.class);
		Person pFound = rxCBTmpl.findById(Person.class).inCollection(cName).one(person.id()).block();
		assertNull(pFound, "Should NOT have found " + pFound);
	}

	@Test
	public void replacePersonRbRepo() {
		Person person = rxCBTmpl.insertById(Person.class).inCollection(cName).one(WalterWhite).block();
		Flux<Person> result = txOperator.execute((ctx) -> rxRepo.withCollection(cName).findById(person.id())
				.flatMap(p -> rxRepo.withCollection(cName).save(p.withFirstName("Walt")))
				.flatMap(it -> Mono.error(new SimulateFailureException())));
		assertThrowsWithCause(result::blockLast, TransactionFailedException.class, SimulateFailureException.class);
		Person pFound = rxRepo.withCollection(cName).findById(person.id()).block();
		assertEquals(person, pFound, "Should have found " + person);
	}

	@Test
	public void insertPersonRbRepo() {
		Person person = WalterWhite;
		Flux<Person> result = txOperator.execute((ctx) -> rxRepo.withCollection(cName).save(person) // insert
				.map(it -> throwSimulateFailureException(it)));
		assertThrowsWithCause(result::blockLast, TransactionFailedException.class, SimulateFailureException.class);
		Person pFound = rxRepo.withCollection(cName).findById(person.id()).block();
		assertNull(pFound, "Should NOT have found " + pFound);
	}

	@Test
	public void insertPersonRepo() {
		Person person = WalterWhite;
		Flux<Person> result = txOperator.execute((ctx) -> rxRepo.withCollection(cName).save(person) // insert
				.flatMap(p -> rxRepo.withCollection(cName).save(p.withFirstName("Walt"))));
		result.blockLast();
		Person pFound = rxRepo.withCollection(cName).findById(person.id()).block();
		assertEquals("Walt", pFound.getFirstname(), "firstname should be Walt");
	}

	@Test
	public void replacePersonSpringTransactional() {
		Person person = WalterWhite;
		rxCBTmpl.insertById(Person.class).inCollection(cName).one(person).block();
		Mono<?> result = rxCBTmpl.findById(Person.class).one(person.id())
				.flatMap(p -> rxCBTmpl.replaceById(Person.class).one(p.withFirstName("Walt"))).as(txOperator::transactional);
		result.block();
		Person pFound = rxCBTmpl.findById(Person.class).inCollection(cName).one(person.id()).block();
		assertEquals(person.withFirstName("Walt"), pFound, "Should have found " + person);
	}

	@Test
	public void replacePersonRbSpringTransactional() {
		Person person = rxCBTmpl.insertById(Person.class).inCollection(cName).one(WalterWhite).block();
		Mono<?> result = rxCBTmpl.findById(Person.class).one(person.id())
				.flatMap(p -> rxCBTmpl.replaceById(Person.class).one(p.withFirstName("Walt")))
				.flatMap(it -> Mono.error(new SimulateFailureException())).as(txOperator::transactional);
		assertThrowsWithCause(result::block, TransactionFailedException.class, SimulateFailureException.class);
		Person pFound = rxCBTmpl.findById(Person.class).inCollection(cName).one(person.id()).block();
		assertEquals(person, pFound, "Should have found " + person);
		assertEquals(person.getFirstname(), pFound.getFirstname(), "firstname should be " + person.getFirstname());
	}

	@Test
	public void findReplacePersonCBTransactionsRxTmpl() {
		Person person = rxCBTmpl.insertById(Person.class).inCollection(cName).one(WalterWhite).block();
		Flux<Person> result = txOperator.execute(ctx -> rxCBTmpl.findById(Person.class).inCollection(cName).one(person.id())
				.flatMap(pGet -> rxCBTmpl.replaceById(Person.class).inCollection(cName).one(pGet.withFirstName("Walt"))));
		result.blockLast();
		Person pFound = rxCBTmpl.findById(Person.class).inCollection(cName).one(person.id()).block();
		assertEquals(person.withFirstName("Walt"), pFound, "Should have found Walt");
	}

	@Test
	public void insertReplacePersonsCBTransactionsRxTmpl() {
		Person person = WalterWhite;
		Flux<Person> result = txOperator.execute((ctx) -> rxCBTmpl.insertById(Person.class).inCollection(cName).one(person)
				.flatMap(pInsert -> rxCBTmpl.replaceById(Person.class).inCollection(cName).one(pInsert.withFirstName("Walt"))));
		result.blockLast();
		Person pFound = rxCBTmpl.findById(Person.class).inCollection(cName).one(person.id()).block();
		assertEquals(person.withFirstName("Walt"), pFound, "Should have found Walt");
	}

	@Test
	void transactionalSavePerson() {
		Person person = WalterWhite;
		savePerson(person).block();
		Person pFound = rxCBTmpl.findById(Person.class).inCollection(cName).one(person.id()).block();
		assertEquals(person, pFound, "Should have found " + person);
	}

	public Mono<Person> savePerson(Person person) {
		return operations.save(person) //
				.as(txOperator::transactional);
	}

}
