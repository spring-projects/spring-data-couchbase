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
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.springframework.data.couchbase.config.BeanNames.COUCHBASE_TEMPLATE;
import static org.springframework.data.couchbase.config.BeanNames.COUCHBASE_TRANSACTIONS;
import static org.springframework.data.couchbase.config.BeanNames.REACTIVE_COUCHBASE_TEMPLATE;

import org.junit.jupiter.api.Disabled;
import reactor.core.publisher.Mono;

import java.io.IOException;
import java.time.Duration;
import java.util.LinkedList;
import java.util.List;

import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.AnnotationConfigApplicationContext;
import org.springframework.context.support.GenericApplicationContext;
import org.springframework.dao.DataRetrievalFailureException;
import org.springframework.data.couchbase.CouchbaseClientFactory;
import org.springframework.data.couchbase.config.BeanNames;
import org.springframework.data.couchbase.core.CouchbaseTemplate;
import org.springframework.data.couchbase.core.ReactiveCouchbaseOperations;
import org.springframework.data.couchbase.core.ReactiveCouchbaseTemplate;
import org.springframework.data.couchbase.core.query.Query;
import org.springframework.data.couchbase.core.query.QueryCriteria;
import org.springframework.data.couchbase.domain.Config;
import org.springframework.data.couchbase.domain.Person;
import org.springframework.data.couchbase.domain.ReactivePersonRepository;
import org.springframework.data.couchbase.util.Capabilities;
import org.springframework.data.couchbase.util.ClusterType;
import org.springframework.data.couchbase.util.CollectionAwareIntegrationTests;
import org.springframework.data.couchbase.util.IgnoreWhen;
import org.springframework.test.context.junit.jupiter.SpringJUnitConfig;

import com.couchbase.client.core.error.DocumentNotFoundException;
import com.couchbase.client.java.Collection;
import com.couchbase.client.java.ReactiveCollection;
import com.couchbase.client.java.kv.RemoveOptions;
import com.couchbase.transactions.TransactionResult;
import com.couchbase.transactions.Transactions;
import com.couchbase.transactions.error.TransactionFailed;

/**
 * Tests for com.couchbase.transactions without using the spring data transactions framework
 *
 * @author Michael Reiche
 */
@IgnoreWhen(missesCapabilities = Capabilities.QUERY, clusterTypes = ClusterType.MOCKED)
@SpringJUnitConfig(Config.class)
@Disabled
public class CouchbaseTransactionNativeTests extends CollectionAwareIntegrationTests {

	// @Autowired not supported on static fields. These are initialized in beforeAll()
	// Also - @Autowired doesn't work here on couchbaseClientFactory even when it is not static, not sure why - oh, it
	// seems there is not a ReactiveCouchbaseClientFactory bean
	static CouchbaseClientFactory couchbaseClientFactory;
	static ReactiveCouchbaseOperations operations;
	static GenericApplicationContext appContext;
	static Transactions transactions;
	@Autowired ReactivePersonRepository rxPersonRepo;

	@BeforeAll
	public static void beforeAll() {

		// AnnotationConfigApplicationContext() is going to create a Transactions object.
		appContext = new AnnotationConfigApplicationContext(Config.class);
		operations = appContext.getBean(ReactiveCouchbaseOperations.class);
		couchbaseTemplate = (CouchbaseTemplate) appContext.getBean(COUCHBASE_TEMPLATE);
		transactions = (Transactions) appContext.getBean(COUCHBASE_TRANSACTIONS);
		reactiveCouchbaseTemplate = (ReactiveCouchbaseTemplate) appContext.getBean(REACTIVE_COUCHBASE_TEMPLATE);
		couchbaseClientFactory = (CouchbaseClientFactory) appContext.getBean(BeanNames.COUCHBASE_CLIENT_FACTORY);

		// this will initialize couchbaseTemplate and reactiveCouchbaseTemplate if not already initialized
		callSuperBeforeAll(new Object() {});
	}

	@AfterAll
	public static void afterAll() {
		try {
			couchbaseClientFactory.close();
		} catch (IOException e) {
			e.printStackTrace();
		}
		appContext.close();
		callSuperAfterAll(new Object() {});
	}

	@Test
	public void replacePersonCBTransactions() {
		Person person = new Person(1, "Walter", "White");
		remove(couchbaseTemplate, collectionName, person.getId().toString());
		couchbaseTemplate.insertById(Person.class).inCollection(collectionName).one(person);

		Mono<TransactionResult> result = transactions.reactive(((ctx) -> { // get the ctx
			return reactiveCouchbaseTemplate.findById(Person.class).inScope(null).inCollection(collectionName)
					.transaction(ctx).one(person.getId().toString()).flatMap(pGet -> reactiveCouchbaseTemplate
							.replaceById(Person.class).inCollection(collectionName).transaction(ctx).one(pGet.withFirstName("Walt")))
					.then();
		}));
		result.block();
		Person pFound = couchbaseTemplate.findById(Person.class).inCollection(collectionName)
				.one(person.getId().toString());
		assertEquals(pFound, person.withFirstName("Walt"), "Should have found Walt");
	}

	@Test
	public void insertReplacePersonsCBTransactions() {
		Person person = new Person(1, "Walter", "White");
		remove(couchbaseTemplate, collectionName, person.getId().toString());

		Mono<TransactionResult> result = transactions.reactive((ctx) -> { // get the ctx
			return reactiveCouchbaseTemplate.insertById(Person.class).inCollection(collectionName).transaction(ctx)
					.one(person).flatMap(pInsert -> reactiveCouchbaseTemplate.replaceById(Person.class)
							.inCollection(collectionName).transaction(ctx).one(pInsert.withFirstName("Walt")))
					.then();
		});

		TransactionResult tr = result.block();
		Person pFound = couchbaseTemplate.findById(Person.class).inCollection(collectionName)
				.one(person.getId().toString());
		assertEquals(pFound, person.withFirstName("Walt"), "Should have found Walt");
	}

	@Test
	public void deletePersonCBTransactionsRepo() {
		Person person = new Person(1, "Walter", "White");
		remove(couchbaseTemplate, collectionName, person.getId().toString());
		rxPersonRepo.withCollection(collectionName).save(person).block();

		Mono<TransactionResult> result = transactions.reactive(((ctx) -> { // get the ctx
			return rxPersonRepo.withCollection(collectionName).withTransaction(ctx).deleteById(person.getId().toString())
					.then(rxPersonRepo.withCollection(collectionName).withTransaction(ctx).deleteById(person.getId().toString()))
					.then();
		}));
		assertThrows(TransactionFailed.class, () -> result.block());
		Person pFound = couchbaseTemplate.findById(Person.class).inCollection(collectionName)
				.one(person.getId().toString());
		assertEquals(pFound, person, "Should have found " + person);
	}

	@Test
	public void findPersonCBTransactions() {
		Person person = new Person(1, "Walter", "White");
		remove(couchbaseTemplate, collectionName, person.getId().toString());
		couchbaseTemplate.insertById(Person.class).inCollection(collectionName).one(person);
		List<Object> docs = new LinkedList<Object>();
		Query q = Query.query(QueryCriteria.where("meta().id").eq(person.getId()));
		Mono<TransactionResult> result = transactions.reactive(((ctx) -> { // get the ctx
			return reactiveCouchbaseTemplate.findByQuery(Person.class).inCollection(collectionName).matching(q)
					.transaction(ctx).one().map(doc -> {
						docs.add(doc);
						return doc;
					}).then();
		}));
		result.block();
		assertFalse(docs.isEmpty(), "Should have found " + person);
		for (Object o : docs) {
			assertEquals(o, person, "Should have found " + person);
		}
	}

	@Test
	// @Transactional // TODO @Transactional does nothing. Transaction is handled by transactionalOperator
	// Failed to retrieve PlatformTransactionManager for @Transactional test:
	public void insertPersonRbCBTransactions() {
		Person person = new Person(1, "Walter", "White");
		remove(couchbaseTemplate, collectionName, person.getId().toString());

		Mono<TransactionResult> result = transactions.reactive((ctx) -> { // get the ctx
			return reactiveCouchbaseTemplate.insertById(Person.class).inCollection(collectionName).transaction(ctx)
					.one(person).<Person> flatMap(it -> Mono.error(new PoofException())).then();
		});

		try {
			result.block();
		} catch (TransactionFailed e) {
			e.printStackTrace();
			if (e.getCause() instanceof PoofException) {
				Person pFound = couchbaseTemplate.findById(Person.class).inCollection(collectionName)
						.one(person.getId().toString());
				assertNull(pFound, "Should not have found " + pFound);
				return;
			} else {
				e.printStackTrace();
			}
		}
		throw new RuntimeException("Should have been a TransactionFailed exception with a cause of PoofException");
	}

	@Test
	// @Transactional // TODO @Transactional does nothing. Transaction is handled by transactionalOperator
	// Failed to retrieve PlatformTransactionManager for @Transactional test:
	public void replacePersonRbCBTransactions() {
		Person person = new Person(1, "Walter", "White");
		remove(couchbaseTemplate, collectionName, person.getId().toString());
		couchbaseTemplate.insertById(Person.class).inCollection(collectionName).one(person);
		Mono<TransactionResult> result = transactions.reactive((ctx) -> { // get the ctx
			return reactiveCouchbaseTemplate.findById(Person.class).inCollection(collectionName).transaction(ctx)
					.one(person.getId().toString())
					.flatMap(pFound -> reactiveCouchbaseTemplate.replaceById(Person.class).inCollection(collectionName)
							.transaction(ctx).one(pFound.withFirstName("Walt")))
					.<Person> flatMap(it -> Mono.error(new PoofException())).then();
		});

		try {
			result.block();
		} catch (TransactionFailed e) {
			if (e.getCause() instanceof PoofException) {
				Person pFound = couchbaseTemplate.findById(Person.class).inCollection(collectionName)
						.one(person.getId().toString());
				assertEquals(person, pFound, "Should have found " + person);
				return;
			} else {
				e.printStackTrace();
			}
		}
		throw new RuntimeException("Should have been a TransactionFailed exception with a cause of PoofException");
	}

	@Test
	public void findPersonSpringTransactions() {
		Person person = new Person(1, "Walter", "White");
		remove(couchbaseTemplate, collectionName, person.getId().toString());
		couchbaseTemplate.insertById(Person.class).inCollection(collectionName).one(person);
		List<Object> docs = new LinkedList<Object>();
		Query q = Query.query(QueryCriteria.where("meta().id").eq(person.getId()));
		Mono<TransactionResult> result = transactions.reactive((ctx) -> { // get the ctx
			return reactiveCouchbaseTemplate.findByQuery(Person.class).inCollection(collectionName).matching(q)
					.transaction(ctx).one().map(doc -> {
						docs.add(doc);
						return doc;
					}).then();
		});
		result.block();
		assertFalse(docs.isEmpty(), "Should have found " + person);
		for (Object o : docs) {
			assertEquals(o, person, "Should have found " + person);
		}
	}

	void remove(Mono<Collection> col, String id) {
		remove(col.block(), id);
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
		try {
			couchbaseTemplate.removeById(Person.class).inCollection(collection).one(id);
			System.out.println("removed " + id);
		} catch (DocumentNotFoundException | DataRetrievalFailureException nfe) {
			System.out.println(id + " : " + "DocumentNotFound when deleting");
		}
	}

	static class PoofException extends Exception {};
}
