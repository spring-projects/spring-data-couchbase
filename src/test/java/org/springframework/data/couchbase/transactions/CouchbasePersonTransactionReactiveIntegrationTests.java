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

import com.couchbase.client.java.Cluster;
import com.couchbase.client.java.transactions.error.TransactionFailedException;
import lombok.Data;
import org.springframework.data.annotation.Version;
import org.springframework.data.couchbase.transactions.util.TransactionTestUtil;
import reactor.core.publisher.Mono;
import reactor.test.StepVerifier;

import java.time.Duration;

import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.dao.DataRetrievalFailureException;
import org.springframework.data.couchbase.CouchbaseClientFactory;
import org.springframework.data.couchbase.core.CouchbaseTemplate;
import org.springframework.data.couchbase.core.ReactiveCouchbaseTemplate;
import org.springframework.data.couchbase.domain.Person;
import org.springframework.data.couchbase.domain.PersonRepository;
import org.springframework.data.couchbase.domain.ReactivePersonRepository;
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

import static com.couchbase.client.java.query.QueryScanConsistency.REQUEST_PLUS;

/**
 * Tests for com.couchbase.transactions without using the spring data transactions framework
 *
 * @author Michael Reiche
 */
@IgnoreWhen(missesCapabilities = Capabilities.QUERY, clusterTypes = ClusterType.MOCKED)
@SpringJUnitConfig(classes = { TransactionsConfigCouchbaseTransactionManager.class, PersonServiceReactive.class } )
//@Disabled("gp: disabling as these use TransactionalOperator which I've done broke (but also feel we should not and cannot support)")
public class CouchbasePersonTransactionReactiveIntegrationTests extends JavaIntegrationTests {
	// intellij flags "Could not autowire" when config classes are specified with classes={...}. But they are populated.
	@Autowired CouchbaseClientFactory couchbaseClientFactory;
	@Autowired ReactiveCouchbaseTransactionManager reactiveCouchbaseTransactionManager;
	@Autowired ReactivePersonRepository rxRepo;
	@Autowired PersonRepository repo;
	@Autowired ReactiveCouchbaseTemplate rxCBTmpl;
	@Autowired Cluster myCluster;
	@Autowired
	PersonServiceReactive personService;
	@Autowired ReactiveCouchbaseTemplate operations;

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
		TransactionTestUtil.assertNotInTransaction();
		operations.removeByQuery(Person.class).withConsistency(REQUEST_PLUS).all().collectList().block();
		operations.removeByQuery(EventLog.class).withConsistency(REQUEST_PLUS).all().collectList().block();
		operations.findByQuery(Person.class).withConsistency(REQUEST_PLUS).all().collectList().block();
		operations.findByQuery(EventLog.class).withConsistency(REQUEST_PLUS).all().collectList().block();
	}

	@Test
	public void shouldRollbackAfterException() {
		personService.savePersonErrors(new Person(null, "Walter", "White")) //
				.as(StepVerifier::create) //
				.verifyError(RuntimeException.class);
		operations.findByQuery(Person.class).withConsistency(REQUEST_PLUS).count() //
				.as(StepVerifier::create) //
				.expectNext(0L) //
				.verifyComplete();
	}

	@Test
	public void shouldRollbackAfterExceptionOfTxAnnotatedMethod() {
		Person p = new Person(null, "Walter", "White");
		assertThrowsWithCause(() ->
			personService.declarativeSavePersonErrors(p) //
					.as(StepVerifier::create) //
					.expectComplete(),
				 SimulateFailureException.class);
	}

	@Test
	public void commitShouldPersistTxEntries() {

		personService.savePerson(new Person(null, "Walter", "White")) //
				.as(StepVerifier::create) //
				.expectNextCount(1) //
				.verifyComplete();

		operations.findByQuery(Person.class).withConsistency(REQUEST_PLUS).count().block();

		operations.findByQuery(Person.class).withConsistency(REQUEST_PLUS).count() //
				.as(StepVerifier::create) //
				.expectNext(1L) //
				.verifyComplete();
	}

	@Test
	public void commitShouldPersistTxEntriesOfTxAnnotatedMethod() {

		personService.declarativeSavePerson(new Person(null, "Walter", "White")).as(StepVerifier::create) //
				.expectNextCount(1) //
				.verifyComplete();

		operations.findByQuery(Person.class).withConsistency(REQUEST_PLUS).count() //
				.as(StepVerifier::create) //
				.expectNext(1L) //
				.verifyComplete();

	}

	@Test
	public void commitShouldPersistTxEntriesAcrossCollections() {

		personService.saveWithLogs(new Person(null, "Walter", "White")) //
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
	//@Disabled("the rollback is not occurring")
	public void rollbackShouldAbortAcrossCollections() {

		personService.saveWithErrorLogs(new Person(null, "Walter", "White")) //
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
		personService.countDuringTx(new Person(null, "Walter", "White")) //
				.as(StepVerifier::create) //
				.expectNext(1L) //
				.verifyComplete();
	}

	@Test
	public void emitMultipleElementsDuringTransaction() {
			personService.saveWithLogs(new Person(null, "Walter", "White")) //
					.as(StepVerifier::create) //
					.expectNextCount(4L) //
					.verifyComplete();
	}

	@Test
	public void errorAfterTxShouldNotAffectPreviousStep() {

		Person p = new Person(1, "Walter", "White");
		personService.savePerson(p) //
				.then(Mono.error(new RuntimeException("my big bad evil error"))).as(StepVerifier::create) //
				.expectError()
				.verify();
		operations.findByQuery(Person.class).withConsistency(REQUEST_PLUS).count() //
				.as(StepVerifier::create) //
				.expectNext(1L) //
				.verifyComplete();
	}

	/*
	  @Test
	  public void deletePersonCBTransactionsRxTmpl() {
	    Person person = new Person(1, "Walter", "White");
	    remove(cbTmpl, cName, person.getId().toString());
	    rxCBTmpl.insertById(Person.class).inCollection(cName).one(person).block();

	    Mono<TransactionResult> result = transactions.reactive(((ctx) -> { // get the ctx
	      return rxCBTmpl.removeById(Person.class).inCollection(cName).transaction(ctx).one(person.getId().toString())
	          .then();
	    }));
	    result.block();
	    Person pFound = rxCBTmpl.findById(Person.class).inCollection(cName).one(person.getId().toString()).block();
	    assertNull(pFound, "Should not have found " + pFound);
	  }

	  @Test
	  public void deletePersonCBTransactionsRxTmplFail() {
	    Person person = new Person(1, "Walter", "White");
	    remove(cbTmpl, cName, person.getId().toString());
	    cbTmpl.insertById(Person.class).inCollection(cName).one(person);

	    Mono<TransactionResult> result = transactions.reactive(((ctx) -> { // get the ctx
	      return rxCBTmpl.removeById(Person.class).inCollection(cName).transaction(ctx).one(person.getId().toString())
	          .then(rxCBTmpl.removeById(Person.class).inCollection(cName).transaction(ctx).one(person.getId().toString()))
	          .then();
	    }));
	    assertThrows(TransactionFailedException.class, result::block);
	    Person pFound = cbTmpl.findById(Person.class).inCollection(cName).one(person.getId().toString());
	    assertEquals(pFound, person, "Should have found " + person);
	  }

	  //  RxRepo ////////////////////////////////////////////////////////////////////////////////////////////

	  @Test
	  public void deletePersonCBTransactionsRxRepo() {
	    Person person = new Person(1, "Walter", "White");
	    remove(cbTmpl, cName, person.getId().toString());
	    rxRepo.withCollection(cName).save(person).block();

	    Mono<TransactionResult> result = transactions.reactive(((ctx) -> { // get the ctx
	      return rxRepo.withCollection(cName).withTransaction(ctx).deleteById(person.getId().toString()).then();
	    }));
	    result.block();
	    Person pFound = cbTmpl.findById(Person.class).inCollection(cName).one(person.getId().toString());
	    assertNull(pFound, "Should not have found " + pFound);
	  }

	  @Test
	  public void deletePersonCBTransactionsRxRepoFail() {
	    Person person = new Person(1, "Walter", "White");
	    remove(cbTmpl, cName, person.getId().toString());
	    rxRepo.withCollection(cName).save(person).block();

	    Mono<TransactionResult> result = transactions.reactive(((ctx) -> { // get the ctx
	      return rxRepo.withCollection(cName).withTransaction(ctx).deleteById(person.getId().toString())
	          .then(rxRepo.withCollection(cName).withTransaction(ctx).deleteById(person.getId().toString())).then();
	    }));
	    assertThrows(TransactionFailedException.class, result::block);
	    Person pFound = cbTmpl.findById(Person.class).inCollection(cName).one(person.getId().toString());
	    assertEquals(pFound, person, "Should have found " + person);
	  }

	  @Test
	  public void findPersonCBTransactions() {
	    Person person = new Person(1, "Walter", "White");
	    remove(cbTmpl, cName, person.getId().toString());
	    cbTmpl.insertById(Person.class).inCollection(cName).one(person);
	    List<Object> docs = new LinkedList<>();
	    Query q = Query.query(QueryCriteria.where("meta().id").eq(person.getId()));
	    Mono<TransactionResult> result = transactions.reactive(((ctx) -> { // get the ctx
	      return rxCBTmpl.findByQuery(Person.class).inCollection(cName).matching(q).transaction(ctx).one().map(doc -> {
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
	    remove(cbTmpl, cName, person.getId().toString());

	    Mono<TransactionResult> result = transactions.reactive((ctx) -> { // get the ctx
	      return rxCBTmpl.insertById(Person.class).inCollection(cName).transaction(ctx).one(person)
	          .<Person> flatMap(it -> Mono.error(new PoofException())).then();
	    });

	    try {
	      result.block();
	    } catch (TransactionFailedException e) {
	      e.printStackTrace();
	      if (e.getCause() instanceof PoofException) {
	        Person pFound = cbTmpl.findById(Person.class).inCollection(cName).one(person.getId().toString());
	        assertNull(pFound, "Should not have found " + pFound);
	        return;
	      } else {
	        e.printStackTrace();
	      }
	    }
	    throw new RuntimeException("Should have been a TransactionFailedException exception with a cause of PoofException");
	  }

	  @Test
	  // @Transactional // TODO @Transactional does nothing. Transaction is handled by transactionalOperator
	  // Failed to retrieve PlatformTransactionManager for @Transactional test:
	  public void replacePersonRbCBTransactions() {
	    Person person = new Person(1, "Walter", "White");
	    remove(cbTmpl, cName, person.getId().toString());
	    cbTmpl.insertById(Person.class).inCollection(cName).one(person);
	    Mono<TransactionResult> result = transactions.reactive((ctx) -> { // get the ctx
	      return rxCBTmpl.findById(Person.class).inCollection(cName).transaction(ctx).one(person.getId().toString())
	          .flatMap(pFound -> rxCBTmpl.replaceById(Person.class).inCollection(cName).transaction(ctx)
	              .one(pFound.withFirstName("Walt")))
	          .<Person> flatMap(it -> Mono.error(new PoofException())).then();
	    });

	    try {
	      result.block();
	    } catch (TransactionFailedException e) {
	      if (e.getCause() instanceof PoofException) {
	        Person pFound = cbTmpl.findById(Person.class).inCollection(cName).one(person.getId().toString());
	        assertEquals(person, pFound, "Should have found " + person);
	        return;
	      } else {
	        e.printStackTrace();
	      }
	    }
	    throw new RuntimeException("Should have been a TransactionFailedException exception with a cause of PoofException");
	  }

	  @Test
	  public void findPersonSpringTransactions() {
	    Person person = new Person(1, "Walter", "White");
	    remove(cbTmpl, cName, person.getId().toString());
	    cbTmpl.insertById(Person.class).inCollection(cName).one(person);
	    List<Object> docs = new LinkedList<>();
	    Query q = Query.query(QueryCriteria.where("meta().id").eq(person.getId()));
	    Mono<TransactionResult> result = transactions.reactive((ctx) -> { // get the ctx
	      return rxCBTmpl.findByQuery(Person.class).inCollection(cName).matching(q).transaction(ctx).one().map(doc -> {
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
	*/
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
        @Version
        Long version;
    }
}
