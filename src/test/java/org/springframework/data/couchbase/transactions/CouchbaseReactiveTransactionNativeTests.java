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

import com.couchbase.client.java.transactions.TransactionResult;
import com.couchbase.client.java.transactions.error.TransactionFailedException;
import org.junit.jupiter.api.Disabled;
import reactor.core.publisher.Mono;

import java.time.Duration;

import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.support.GenericApplicationContext;
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
import org.springframework.data.couchbase.transaction.CouchbaseStuffHandle;
import org.springframework.data.couchbase.transaction.ReactiveCouchbaseTransactionManager;
import org.springframework.data.couchbase.util.Capabilities;
import org.springframework.data.couchbase.util.ClusterType;
import org.springframework.data.couchbase.util.IgnoreWhen;
import org.springframework.data.couchbase.util.JavaIntegrationTests;
import org.springframework.test.context.junit.jupiter.SpringJUnitConfig;
import org.springframework.transaction.reactive.TransactionalOperator;

import com.couchbase.client.core.cnc.Event;
import com.couchbase.client.core.error.DocumentNotFoundException;
import com.couchbase.client.java.Collection;
import com.couchbase.client.java.ReactiveCollection;
import com.couchbase.client.java.kv.RemoveOptions;

/**
 * Tests for com.couchbase.transactions without using the spring data transactions framework
 *
 * @author Michael Reiche
 */
@IgnoreWhen(missesCapabilities = Capabilities.QUERY, clusterTypes = ClusterType.MOCKED)
@SpringJUnitConfig(CouchbaseReactiveTransactionNativeTests.Config.class)
@Disabled // Now using TransactionSyncronizationManager for the session
public class CouchbaseReactiveTransactionNativeTests extends JavaIntegrationTests {

	@Autowired CouchbaseClientFactory couchbaseClientFactory;
	@Autowired ReactiveCouchbaseTransactionManager reactiveCouchbaseTransactionManager;
	@Autowired ReactivePersonRepository rxRepo;
	@Autowired PersonRepository repo;
	@Autowired ReactiveCouchbaseTemplate rxCBTmpl;

	static String cName; // short name

	static GenericApplicationContext context;
	ReactiveCouchbaseTemplate operations;

	@BeforeAll
	public static void beforeAll() {
		callSuperBeforeAll(new Object() {});
		cName = null;// cName;
	}

	@AfterAll
	public static void afterAll() {
		callSuperAfterAll(new Object() {});
		context.close();
	}

	@BeforeEach
	public void beforeEachTest() {
		operations = rxCBTmpl;

	}

	@Test
	public void replacePersonTemplate() {
		Person person = new Person(1, "Walter", "White");
		remove(rxCBTmpl, cName, person.getId().toString());
		rxCBTmpl.insertById(Person.class).inCollection(cName).one(person).block();

		CouchbaseStuffHandle txOperator = new CouchbaseStuffHandle(reactiveCouchbaseTransactionManager);
		Mono<TransactionResult> result = txOperator
				.reactive((ctx) -> ctx.template(rxCBTmpl).findById(Person.class).one(person.getId().toString())
						.flatMap(p -> ctx.template(rxCBTmpl).replaceById(Person.class).one(p.withFirstName("Walt")))
						// .flatMap(it -> Mono.error(new PoofException()))
						.then());

		try {
			result.block();
		} catch (TransactionFailedException e) {
			if (e.getCause() instanceof SimulateFailureException) {
				Person pFound = rxCBTmpl.findById(Person.class).inCollection(cName).one(person.getId().toString()).block();
				assertEquals(person, pFound, "Should have found " + person);
				return;
			}
			e.printStackTrace();
		}
		Person pFound = rxCBTmpl.findById(Person.class).inCollection(cName).one(person.getId().toString()).block();
		assertEquals("Walt", pFound.getFirstname(), "firstname should be Walt");
		// throw new RuntimeException("Should have been a TransactionFailedException exception with a cause of PoofException");
	}

	@Test
	public void replacePersonRbTemplate() {
		Person person = new Person(1, "Walter", "White");
		remove(rxCBTmpl, cName, person.getId().toString());
		rxCBTmpl.insertById(Person.class).inCollection(cName).one(person).block();
sleepMs(1000);
		CouchbaseStuffHandle txOperator = new CouchbaseStuffHandle(reactiveCouchbaseTransactionManager);
		Mono<TransactionResult> result = txOperator
				.reactive((ctx) -> ctx.template(rxCBTmpl).findById(Person.class).one(person.getId().toString())
						.flatMap(p -> ctx.template(rxCBTmpl).replaceById(Person.class).one(p.withFirstName("Walt")))
						.flatMap(it -> Mono.error(new SimulateFailureException())).then());

		try {
			result.block();
		} catch (TransactionFailedException e) {
			if (e.getCause() instanceof SimulateFailureException) {
				Person pFound = rxCBTmpl.findById(Person.class).inCollection(cName).one(person.getId().toString()).block();
				assertEquals(person, pFound, "Should have found " + person);
				return;
			}
			e.printStackTrace();
		}
		// Person pFound = rxCBTmpl.findById(Person.class).inCollection(cName).one(person.getId().toString()).block();
		// assertEquals("Walt", pFound.getFirstname(), "firstname should be Walt");
		throw new RuntimeException("Should have been a TransactionFailedException exception with a cause of PoofException");
	}

	@Test
	public void insertPersonTemplate() {
		Person person = new Person(1, "Walter", "White");
		remove(rxCBTmpl, cName, person.getId().toString());

		CouchbaseStuffHandle txOperator = new CouchbaseStuffHandle(reactiveCouchbaseTransactionManager);
		Mono<TransactionResult> result = txOperator.reactive((ctx) -> ctx.template(rxCBTmpl).insertById(Person.class)
				.one(person).flatMap(p -> ctx.template(rxCBTmpl).replaceById(Person.class).one(p.withFirstName("Walt")))
				// .flatMap(it -> Mono.error(new PoofException()))
				.then());

		try {
			result.block();
		} catch (TransactionFailedException e) {
			if (e.getCause() instanceof SimulateFailureException) {
				Person pFound = rxCBTmpl.findById(Person.class).inCollection(cName).one(person.getId().toString()).block();
				assertNull(pFound, "Should NOT have found " + pFound);
				return;
			}
			e.printStackTrace();
		}
		Person pFound = rxCBTmpl.findById(Person.class).inCollection(cName).one(person.getId().toString()).block();
		assertEquals("Walt", pFound.getFirstname(), "firstname should be Walt");
		// throw new RuntimeException("Should have been a TransactionFailedException exception with a cause of PoofException");
	}

	@Test
	public void insertPersonRbTemplate() {
		Person person = new Person(1, "Walter", "White");
		remove(rxCBTmpl, cName, person.getId().toString());

		CouchbaseStuffHandle txOperator = new CouchbaseStuffHandle(reactiveCouchbaseTransactionManager);
		Mono<TransactionResult> result = txOperator.reactive((ctx) -> ctx.template(rxCBTmpl).insertById(Person.class)
				.one(person).flatMap(p -> ctx.template(rxCBTmpl).replaceById(Person.class).one(p.withFirstName("Walt")))
				.flatMap(it -> Mono.error(new SimulateFailureException())).then());

		try {
			result.block();
		} catch (TransactionFailedException e) {
			if (e.getCause() instanceof SimulateFailureException) {
				Person pFound = rxCBTmpl.findById(Person.class).inCollection(cName).one(person.getId().toString()).block();
				assertNull(pFound, "Should NOT have found " + pFound);
				return;
			}
			e.printStackTrace();
		}
		// Person pFound = rxCBTmpl.findById(Person.class).inCollection(cName).one(person.getId().toString()).block();
		// assertEquals("Walt", pFound.getFirstname(), "firstname should be Walt");
		throw new RuntimeException("Should have been a TransactionFailedException exception with a cause of PoofException");
	}

	@Test
	public void replacePersonRbRepo() {
		Person person = new Person(1, "Walter", "White");
		remove(rxCBTmpl, cName, person.getId().toString());
		rxRepo.withCollection(cName).save(person).block();

		CouchbaseStuffHandle txOperator = new CouchbaseStuffHandle(reactiveCouchbaseTransactionManager);
		Mono<TransactionResult> result = txOperator
				.reactive((ctx) -> ctx.repository(rxRepo).withCollection(cName).findById(person.getId().toString())
						.flatMap(p -> ctx.repository(rxRepo).withCollection(cName).save(p.withFirstName("Walt")))
						.flatMap(it -> Mono.error(new SimulateFailureException())).then());

		try {
			result.block();
		} catch (TransactionFailedException e) {
			if (e.getCause() instanceof SimulateFailureException) {
				Person pFound = rxRepo.withCollection(cName).findById(person.getId().toString()).block();
				assertEquals(person, pFound, "Should have found " + person);
				return;
			}
			e.printStackTrace();
		}
		// Person pFound = rxRepo.withCollection(cName).findById(person.getId().toString()).block();
		// assertEquals("Walt", pFound.getFirstname(), "firstname should be Walt");
		throw new RuntimeException("Should have been a TransactionFailedException exception with a cause of PoofException");
	}

	@Test
	public void insertPersonRbRepo() {
		Person person = new Person(1, "Walter", "White");
		remove(rxCBTmpl, cName, person.getId().toString());

		CouchbaseStuffHandle txOperator = new CouchbaseStuffHandle(reactiveCouchbaseTransactionManager);
		Mono<TransactionResult> result = txOperator
				.reactive((ctx) -> ctx.repository(rxRepo).withTransaction(txOperator).withCollection(cName).save(person) // insert
						//.flatMap(p -> ctx.repository(rxRepo).withCollection(cName).save(p.withFirstName("Walt"))) // replace
						.flatMap(it -> Mono.error(new SimulateFailureException())).then());

		try {
			result.block();
		} catch (TransactionFailedException e) {
			if (e.getCause() instanceof SimulateFailureException) {
				Person pFound = rxRepo.withCollection(cName).findById(person.getId().toString()).block();
				assertNull(pFound, "Should NOT have found " + pFound);
				return;
			}
			e.printStackTrace();
		}
		// Person pFound = rxRepo.withCollection(cName).findById(person.getId().toString()).block();
		// assertEquals("Walt", pFound.getFirstname(), "firstname should be Walt");
		throw new RuntimeException("Should have been a TransactionFailedException exception with a cause of PoofException");
	}

	@Test
	public void insertPersonRepo() {
		Person person = new Person(1, "Walter", "White");
		remove(rxCBTmpl, cName, person.getId().toString());

		CouchbaseStuffHandle txOperator = new CouchbaseStuffHandle(reactiveCouchbaseTransactionManager);
		Mono<TransactionResult> result = txOperator
				.reactive((ctx) -> ctx.repository(rxRepo).withCollection(cName).save(person) // insert
						.flatMap(p -> ctx.repository(rxRepo).withCollection(cName).save(p.withFirstName("Walt"))) // replace
						// .flatMap(it -> Mono.error(new PoofException()))
						.then());

		try {
			result.block();
		} catch (TransactionFailedException e) {
			if (e.getCause() instanceof SimulateFailureException) {
				Person pFound = rxRepo.withCollection(cName).findById(person.getId().toString()).block();
				assertNull(pFound, "Should NOT have found " + pFound);
				return;
			}
			e.printStackTrace();
		}
		Person pFound = rxRepo.withCollection(cName).findById(person.getId().toString()).block();
		assertEquals("Walt", pFound.getFirstname(), "firstname should be Walt");
		// throw new RuntimeException("Should have been a TransactionFailedException exception with a cause of PoofException");
	}

	/*
	@Test
	public void replacePersonRbSpringTransactional() {
		Person person = new Person(1, "Walter", "White");
		remove(rxCBTmpl, cName, person.getId().toString());
		rxCBTmpl.insertById(Person.class).inCollection(cName).one(person).block();


				TransactionalOperator txOperator = TransactionalOperator.create(reactiveCouchbaseTransactionManager);
		Mono<TransactionResult> result = txOperator.reactive((ctx) -> {
			ctx.transactionResultHolder(123);
			return ctx.template(rxCBTmpl).findById(Person.class).one(person.getId().toString())
					.flatMap(p -> ctx.template(rxCBTmpl).replaceById(Person.class).one(p.withFirstName("Walt")))
					.as(txOperator::transactional).then();
		}, false);

		//TransactionalOperator txOperator = TransactionalOperator.create(reactiveCouchbaseTransactionManager, new CouchbaseTransactionDefinition());
		//Mono<TransactionResult> result = txOperator.reactive((ctx) -> {
		//	ctx.transactionResultHolder(123);
		//	return ctx.template(rxCBTmpl).findById(Person.class).one(person.getId().toString())
		//			.flatMap(p -> ctx.template(rxCBTmpl).replaceById(Person.class).one(p.withFirstName("Walt")))
		//			.as(txOperator::transactional).then();
		//}, false);

		try {
			result.block();
		} catch (TransactionFailedException e) {
			if (e.getCause() instanceof PoofException) {
				Person pFound = rxCBTmpl.findById(Person.class).inCollection(cName).one(person.getId().toString()).block();
				assertEquals(person, pFound, "Should have found " + person);
				return;
			}
			e.printStackTrace();
		}
		Person pFound = rxCBTmpl.findById(Person.class).inCollection(cName).one(person.getId().toString()).block();
		assertEquals("Walt", pFound.getFirstname(), "firstname should be Walt");
		// throw new RuntimeException("Should have been a TransactionFailedException exception with a cause of PoofException");
	}
*/
	@Test
	public void findReplacePersonCBTransactionsRxTmpl() {
		Person person = new Person(1, "Walter", "White");
		remove(rxCBTmpl, cName, person.getId().toString());
		rxCBTmpl.insertById(Person.class).inCollection(cName).one(person).block();
		CouchbaseStuffHandle txOperator = new CouchbaseStuffHandle(reactiveCouchbaseTransactionManager);
		Mono<TransactionResult> result = txOperator.reactive((ctx) -> {
			rxCBTmpl.support().getTxResultHolder(person);
			return rxCBTmpl.findById(Person.class).inCollection(cName).transaction(txOperator).one(person.getId().toString())
					.flatMap(pGet -> rxCBTmpl.replaceById(Person.class).inCollection(cName).transaction(txOperator)
							.one(pGet.withFirstName("Walt")))
					.then();
		});
		result.block();
		Person pFound = rxCBTmpl.findById(Person.class).inCollection(cName).one(person.getId().toString()).block();
		assertEquals(person.withFirstName("Walt"), pFound, "Should have found Walt");
	}

	@Test
	public void insertReplacePersonsCBTransactionsRxTmpl() {
		Person person = new Person(1, "Walter", "White");
		remove(rxCBTmpl, cName, person.getId().toString());

		CouchbaseStuffHandle txOperator = new CouchbaseStuffHandle(reactiveCouchbaseTransactionManager);
		Mono<TransactionResult> result = txOperator.reactive((ctx) -> {
			return rxCBTmpl
					.insertById(Person.class).inCollection(cName).transaction(txOperator).one(person).flatMap(pInsert -> rxCBTmpl
							.replaceById(Person.class).inCollection(cName).transaction(txOperator).one(pInsert.withFirstName("Walt")))
					.then();
		});

		result.block();
		Person pFound = rxCBTmpl.findById(Person.class).inCollection(cName).one(person.getId().toString()).block();
		assertEquals(person.withFirstName("Walt"), pFound, "Should have found Walt");
	}

	@Test void transactionalSavePerson(){
		Person person = new Person(1, "Walter", "White");
		remove(rxCBTmpl, cName, person.getId().toString());
		savePerson(person).block();
		Person pFound = rxCBTmpl.findById(Person.class).inCollection(cName).one(person.getId().toString()).block();
		assertEquals(person, pFound, "Should have found "+person);
	}

	public Mono<Person> savePerson(Person person) {

		TransactionalOperator transactionalOperator = TransactionalOperator.create(reactiveCouchbaseTransactionManager);

		return operations.save(person) //
				.flatMap(Mono::just) //
				.as(transactionalOperator::transactional);
	}

		void remove(Collection col, String id) {
		remove(col.reactive(), id);
	}

	void remove(ReactiveCollection col, String id) {
		try {
			col.remove(id, RemoveOptions.removeOptions().timeout(Duration.ofSeconds(10))).block();
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

	@Configuration
	@EnableCouchbaseRepositories("org.springframework.data.couchbase")
	@EnableReactiveCouchbaseRepositories("org.springframework.data.couchbase")
	static class Config extends AbstractCouchbaseConfiguration {

		@Override
		public String getConnectionString() {
			return connectionString();
		}

		@Override
		public String getUserName() {
			return config().adminUsername();
		}

		@Override
		public String getPassword() {
			return config().adminPassword();
		}

		@Override
		public String getBucketName() {
			return bucketName();
		}
	}

}
