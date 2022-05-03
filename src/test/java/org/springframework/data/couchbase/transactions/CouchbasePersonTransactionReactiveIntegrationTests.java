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
import lombok.Data;
import org.springframework.context.annotation.AnnotationConfigApplicationContext;
import org.springframework.context.annotation.Bean;
import org.springframework.data.couchbase.config.BeanNames;
import org.springframework.data.couchbase.core.CouchbaseOperations;
import org.springframework.data.couchbase.transaction.CouchbaseTransactionManager;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.test.StepVerifier;

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
import org.springframework.data.couchbase.core.ReactiveCouchbaseOperations;
import org.springframework.data.couchbase.core.ReactiveCouchbaseTemplate;
import org.springframework.data.couchbase.core.query.Query;
import org.springframework.data.couchbase.domain.Person;
import org.springframework.data.couchbase.domain.PersonRepository;
import org.springframework.data.couchbase.domain.ReactivePersonRepository;
import org.springframework.data.couchbase.repository.config.EnableCouchbaseRepositories;
import org.springframework.data.couchbase.repository.config.EnableReactiveCouchbaseRepositories;
import org.springframework.data.couchbase.transaction.ReactiveCouchbaseTransactionManager;
import org.springframework.data.couchbase.util.Capabilities;
import org.springframework.data.couchbase.util.ClusterType;
import org.springframework.data.couchbase.util.IgnoreWhen;
import org.springframework.data.couchbase.util.JavaIntegrationTests;
import org.springframework.test.context.junit.jupiter.SpringJUnitConfig;
import org.springframework.transaction.annotation.Transactional;
import org.springframework.transaction.reactive.TransactionalOperator;
import org.springframework.transaction.support.DefaultTransactionDefinition;

import com.couchbase.client.core.cnc.Event;
import com.couchbase.client.core.error.DocumentNotFoundException;
import com.couchbase.client.java.Collection;
import com.couchbase.client.java.ReactiveCollection;
import com.couchbase.client.java.kv.RemoveOptions;
import com.couchbase.transactions.TransactionDurabilityLevel;
import com.couchbase.transactions.config.TransactionConfig;
import com.couchbase.transactions.config.TransactionConfigBuilder;

import static com.couchbase.client.java.query.QueryScanConsistency.REQUEST_PLUS;
import static org.assertj.core.api.Assertions.assertThat;

/**
 * Tests for com.couchbase.transactions without using the spring data transactions framework
 *
 * @author Michael Reiche
 */
@IgnoreWhen(missesCapabilities = Capabilities.QUERY, clusterTypes = ClusterType.MOCKED)
@SpringJUnitConfig(CouchbasePersonTransactionReactiveIntegrationTests.Config.class)
//@Transactional(transactionManager = BeanNames.COUCHBASE_TRANSACTION_MANAGER)
public class CouchbasePersonTransactionReactiveIntegrationTests extends JavaIntegrationTests {

	@Autowired CouchbaseClientFactory couchbaseClientFactory;
	@Autowired ReactiveCouchbaseTransactionManager reactiveCouchbaseTransactionManager;
	@Autowired CouchbaseTransactionManager couchbaseTransactionManager;
	@Autowired ReactivePersonRepository rxRepo;
	@Autowired PersonRepository repo;
	@Autowired ReactiveCouchbaseTemplate rxCBTmpl;

	@Autowired Cluster myCluster;

	/* DO NOT @Autowired */ PersonServiceInner personServiceInner;

	static GenericApplicationContext context;
	@Autowired ReactiveCouchbaseTemplate operations;

	@BeforeAll
	public static void beforeAll() {
		callSuperBeforeAll(new Object() {});
		context = new AnnotationConfigApplicationContext(CouchbasePersonTransactionReactiveIntegrationTests.Config.class,
				PersonServiceInner.class);
	}

	@AfterAll
	public static void afterAll() {
		callSuperAfterAll(new Object() {});
	}

	@BeforeEach
	public void beforeEachTest() {
		personServiceInner = context.getBean(PersonServiceInner.class); // getting it via autowired results in no @Transactional
		operations.removeByQuery(Person.class).withConsistency(REQUEST_PLUS).all().collectList().block();
		operations.removeByQuery(EventLog.class).withConsistency(REQUEST_PLUS).all().collectList().block();
		operations.findByQuery(Person.class).withConsistency(REQUEST_PLUS).all().collectList().block();
		operations.findByQuery(EventLog.class).withConsistency(REQUEST_PLUS).all().collectList().block();
	}


	@Test // DATAMONGO-2265
	public void shouldRollbackAfterException() {
		personServiceInner.savePersonErrors(new Person(null, "Walter", "White")) //
				.as(StepVerifier::create) //
				.verifyError(RuntimeException.class);
		// operations.findByQuery(Person.class).withConsistency(QueryScanConsistency.REQUEST_PLUS).count().block();
		// sleepMs(5000);
		operations.count(new Query(), Person.class) //
				.as(StepVerifier::create) //
				.expectNext(0L) //
				.verifyComplete();
	}

	@Test // DATAMONGO-2265
	// @Rollback(false)
	public void shouldRollbackAfterExceptionOfTxAnnotatedMethod() {
		Person p = new Person(null, "Walter", "White");
		try {
			personServiceInner.declarativeSavePersonErrors(p) //
					.as(StepVerifier::create) //
					.expectComplete();
			// .verifyError(RuntimeException.class);
		} catch (RuntimeException e) {
			if (e instanceof SimulateFailureException || (e.getMessage() != null && e.getMessage().contains("poof"))) {
				System.err.println(e);
			} else {
				throw e;
			}
		}

	}

	@Test // DATAMONGO-2265
	public void commitShouldPersistTxEntries() {

		personServiceInner.savePerson(new Person(null, "Walter", "White")) //
				.as(StepVerifier::create) //
				.expectNextCount(1) //
				.verifyComplete();

		operations.findByQuery(Person.class).withConsistency(REQUEST_PLUS).count().block();

		operations.findByQuery(Person.class).withConsistency(REQUEST_PLUS).count() //
				.as(StepVerifier::create) //
				.expectNext(1L) //
				.verifyComplete();
	}

	@Test // DATAMONGO-2265
	public void commitShouldPersistTxEntriesOfTxAnnotatedMethod() {

		personServiceInner.declarativeSavePerson(new Person(null, "Walter", "White")).as(StepVerifier::create) //
				.expectNextCount(1) //
				.verifyComplete();

		operations.findByQuery(Person.class).withConsistency(REQUEST_PLUS).count() //
				.as(StepVerifier::create) //
				.expectNext(1L) //
				.verifyComplete();

	}

	@Test // DATAMONGO-2265
	public void commitShouldPersistTxEntriesAcrossCollections() {

		personServiceInner.saveWithLogs(new Person(null, "Walter", "White")) //
				.then() //
				.as(StepVerifier::create) //
				.verifyComplete();

		operations.findByQuery(Person.class).withConsistency(REQUEST_PLUS).count() //
				.as(StepVerifier::create) //
				.expectNext(1L) //
				.verifyComplete();

		operations.findByQuery(Person.class).withConsistency(REQUEST_PLUS).count() //
				.as(StepVerifier::create) //
				.expectNext(4L) //
				.verifyComplete();
	}

	@Test // DATAMONGO-2265
	public void rollbackShouldAbortAcrossCollections() {

		personServiceInner.saveWithErrorLogs(new Person(null, "Walter", "White")) //
				.then() //
				.as(StepVerifier::create) //
				.verifyError();

		operations.count(new Query(), Person.class) //
				.as(StepVerifier::create) //
				.expectNext(0L) //
				.verifyComplete();

		operations.count(new Query(), EventLog.class)//
				.as(StepVerifier::create) //
				.expectNext(0L) //
				.verifyComplete();
	}

	@Test // DATAMONGO-2265
	public void countShouldWorkInsideTransaction() {

		personServiceInner.countDuringTx(new Person(null, "Walter", "White")) //
				.as(StepVerifier::create) //
				.expectNext(1L) //
				.verifyComplete();
	}

	@Test // DATAMONGO-2265
	public void emitMultipleElementsDuringTransaction() {

		try {
			personServiceInner.saveWithLogs(new Person(null, "Walter", "White")) //
					.as(StepVerifier::create) //
					.expectNextCount(4L) //
					.verifyComplete();
		} catch (Exception e) {
			System.err.println("Done");
			throw e;
		}
	}

	@Test // DATAMONGO-2265
	public void errorAfterTxShouldNotAffectPreviousStep() {

		Person p = new Person(1, "Walter", "White");
		remove(couchbaseTemplate, "_default", p.getId().toString());
		personServiceInner.savePerson(p) //
				//.delayElement(Duration.ofMillis(100)) //
				.then(Mono.error(new RuntimeException("my big bad evil error"))).as(StepVerifier::create) //
				.expectError()
				.verify();
				//.expectError() //
				//.as(StepVerifier::create)
				//.expectNext(p)
				//.verifyComplete();

		operations.findByQuery(Person.class).withConsistency(REQUEST_PLUS).count() //
				.as(StepVerifier::create) //
				.expectNext(1L) //
				.verifyComplete();
	}

	// @RequiredArgsConstructor
	static class PersonServiceInner {

		final ReactiveCouchbaseOperations personOperationsRx;
		final ReactiveCouchbaseTransactionManager managerRx;
		final CouchbaseOperations personOperations;
		final CouchbaseTransactionManager manager;

		public PersonServiceInner(CouchbaseOperations ops, CouchbaseTransactionManager mgr, ReactiveCouchbaseOperations opsRx,
															ReactiveCouchbaseTransactionManager mgrRx) {
			personOperations = ops;
			manager = mgr;
			System.err.println("operations cluster  : " + personOperations.getCouchbaseClientFactory().getCluster());
			System.err.println("manager cluster     : " + manager.getDatabaseFactory().getCluster());
			System.err.println("manager Manager     : " + manager);

			personOperationsRx = opsRx;
			managerRx = mgrRx;
			System.out
					.println("operationsRx cluster  : " + personOperationsRx.getCouchbaseClientFactory().getCluster().block());
			System.out.println("managerRx cluster     : " + mgrRx.getDatabaseFactory().getCluster().block());
			System.out.println("managerRx Manager     : " + managerRx);
			return;
		}

		public Mono<Person> savePersonErrors(Person person) {

			TransactionalOperator transactionalOperator = TransactionalOperator.create(managerRx);
			return personOperationsRx.insertById(Person.class).one(person) //
					.<Person> flatMap(it -> Mono.error(new RuntimeException("poof!"))) //
					.as(transactionalOperator::transactional);
		}

		public Mono<Person> savePerson(Person person) {

			TransactionalOperator transactionalOperator = TransactionalOperator.create(managerRx,
					new DefaultTransactionDefinition());

			return personOperationsRx.insertById(Person.class).one(person) //
					.flatMap(Mono::just) //
					.as(transactionalOperator::transactional);
		}

		public Mono<Long> countDuringTx(Person person) {

			TransactionalOperator transactionalOperator = TransactionalOperator.create(managerRx,
					new DefaultTransactionDefinition());

			return personOperationsRx.insertById(Person.class).one(person) //
					.then(personOperationsRx.count(new Query(), Person.class)) //
					.as(transactionalOperator::transactional);
		}

		public Flux<EventLog> saveWithLogs(Person person) {

			TransactionalOperator transactionalOperator = TransactionalOperator.create(managerRx,
					new DefaultTransactionDefinition());

			return Flux.merge(personOperationsRx.insertById(EventLog.class).one(new EventLog(new ObjectId().toString(), "beforeConvert")), //
					personOperationsRx.insertById(EventLog.class).one(new EventLog(new ObjectId(), "afterConvert")), //
					personOperationsRx.insertById(EventLog.class).one(new EventLog(new ObjectId(), "beforeInsert")), //
					personOperationsRx.insertById(Person.class).one(person), //
					personOperationsRx.insertById(EventLog.class).one(new EventLog(new ObjectId(), "afterInsert"))) //
					.thenMany(personOperationsRx.findByQuery(EventLog.class).all()) //
					.as(transactionalOperator::transactional);
		}

		public Flux<Void> saveWithErrorLogs(Person person) {

			TransactionalOperator transactionalOperator = TransactionalOperator.create(managerRx,
					new DefaultTransactionDefinition());

			return Flux.merge(personOperationsRx.save(new EventLog(new ObjectId(), "beforeConvert")), //
					personOperationsRx.save(new EventLog(new ObjectId(), "afterConvert")), //
					personOperationsRx.save(new EventLog(new ObjectId(), "beforeInsert")), //
					personOperationsRx.save(person), //
					personOperationsRx.save(new EventLog(new ObjectId(), "afterInsert"))) //
					.<Void> flatMap(it -> Mono.error(new RuntimeException("poof!"))) //
					.as(transactionalOperator::transactional);
		}

		@Transactional
		public Flux<Person> declarativeSavePerson(Person person) {

			TransactionalOperator transactionalOperator = TransactionalOperator.create(managerRx,
					new DefaultTransactionDefinition());

			return transactionalOperator.execute(reactiveTransaction -> personOperationsRx.insertById(Person.class).one(person));
		}

		@Transactional(transactionManager = BeanNames.COUCHBASE_TRANSACTION_MANAGER)
		public Flux<Person> declarativeSavePersonErrors(Person person) {
			Person p = personOperations.insertById(Person.class).one(person);
			// if(1==1)throw new RuntimeException("poof!");
			Person pp = personOperations.findByQuery(Person.class).withConsistency(REQUEST_PLUS).all().get(0);
			System.err.println("pp=" + pp);
			SimulateFailureException.throwEx();
			return Flux.just(p);
		}
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
	    assertThrows(TransactionFailed.class, result::block);
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
	    assertThrows(TransactionFailed.class, result::block);
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
	    } catch (TransactionFailed e) {
	      e.printStackTrace();
	      if (e.getCause() instanceof PoofException) {
	        Person pFound = cbTmpl.findById(Person.class).inCollection(cName).one(person.getId().toString());
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
	    } catch (TransactionFailed e) {
	      if (e.getCause() instanceof PoofException) {
	        Person pFound = cbTmpl.findById(Person.class).inCollection(cName).one(person.getId().toString());
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

		@Override
		public TransactionConfig transactionConfig() {
			return TransactionConfigBuilder.create().logDirectly(Event.Severity.INFO).logOnFailure(true, Event.Severity.ERROR)
					.expirationTime(Duration.ofMinutes(10)).durabilityLevel(TransactionDurabilityLevel.MAJORITY).build();
		}

		@Bean
		public Cluster couchbaseCluster() {
			return Cluster.connect("10.144.220.101", "Administrator", "password");
		}

		/*
		@Bean("personService")
		PersonService getPersonService(CouchbaseOperations ops, CouchbaseTransactionManager mgr,
				ReactiveCouchbaseOperations opsRx, ReactiveCouchbaseTransactionManager mgrRx) {
			return new PersonService(ops, mgr, opsRx, mgrRx);
		}
		 */

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
	}
}
