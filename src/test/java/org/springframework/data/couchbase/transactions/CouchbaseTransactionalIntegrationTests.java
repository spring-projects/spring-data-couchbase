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

import com.couchbase.client.java.env.ClusterEnvironment;
import com.couchbase.client.java.transactions.config.TransactionsCleanupConfig;
import com.couchbase.client.java.transactions.config.TransactionsConfig;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.AnnotationConfigApplicationContext;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.support.GenericApplicationContext;
import org.springframework.data.couchbase.CouchbaseClientFactory;
import org.springframework.data.couchbase.config.AbstractCouchbaseConfiguration;
import org.springframework.data.couchbase.config.BeanNames;
import org.springframework.data.couchbase.core.CouchbaseOperations;
import org.springframework.data.couchbase.core.CouchbaseTemplate;
import org.springframework.data.couchbase.core.ReactiveCouchbaseOperations;
import org.springframework.data.couchbase.core.ReactiveCouchbaseTemplate;
import org.springframework.data.couchbase.domain.Person;
import org.springframework.data.couchbase.repository.config.EnableCouchbaseRepositories;
import org.springframework.data.couchbase.repository.config.EnableReactiveCouchbaseRepositories;
import org.springframework.data.couchbase.transaction.CouchbaseSimpleCallbackTransactionManager;
import org.springframework.data.couchbase.transaction.ReactiveCouchbaseTransactionManager;
import org.springframework.data.couchbase.util.Capabilities;
import org.springframework.data.couchbase.util.ClusterType;
import org.springframework.data.couchbase.util.IgnoreWhen;
import org.springframework.data.couchbase.util.JavaIntegrationTests;
import org.springframework.stereotype.Component;
import org.springframework.stereotype.Service;
import org.springframework.test.context.junit.jupiter.SpringJUnitConfig;
import org.springframework.transaction.annotation.EnableTransactionManagement;
import org.springframework.transaction.annotation.Transactional;
import reactor.core.publisher.Mono;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

import static com.couchbase.client.java.query.QueryScanConsistency.REQUEST_PLUS;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.fail;

/**
 * Tests for @Transactional.
 */
@IgnoreWhen(missesCapabilities = Capabilities.QUERY, clusterTypes = ClusterType.MOCKED)
@SpringJUnitConfig(CouchbaseTransactionalIntegrationTests.Config.class)
public class CouchbaseTransactionalIntegrationTests extends JavaIntegrationTests {

	@Autowired CouchbaseClientFactory couchbaseClientFactory;
	@Autowired CouchbaseTemplate cbTmpl;
	@Autowired ReactiveCouchbaseTemplate rxCBTmpl;
	/* DO NOT @Autowired - it will result in no @Transactional annotation behavior */ PersonService personService;
	@Autowired CouchbaseTemplate operations;

	static GenericApplicationContext context;

	@BeforeAll
	public static void beforeAll() {
		callSuperBeforeAll(new Object() {});
		context = new AnnotationConfigApplicationContext(CouchbaseTransactionalIntegrationTests.Config.class,
//				PersonService.class, CouchbasePersonTransactionIntegrationTests.TransactionInterception.class);
				PersonService.class);
	}

	@AfterAll
	public static void afterAll() {
		callSuperAfterAll(new Object() {});
		//context.close();
	}

	@BeforeEach
	public void beforeEachTest() {
		personService = context.getBean(PersonService.class); // getting it via autowired results in no @Transactional
		// Skip this as we just one to track TransactionContext
		operations.removeByQuery(Person.class).withConsistency(REQUEST_PLUS).all(); // doesn't work???
		List<Person> p = operations.findByQuery(Person.class).withConsistency(REQUEST_PLUS).all();

		Person walterWhite = new Person(1, "Walter", "White");
		try {
			couchbaseClientFactory.getBucket().defaultCollection().remove(walterWhite.getId().toString());
		} catch (Exception ex) {
			// System.err.println(ex);
		}
	}

	@Test
	public void shouldRollbackAfterExceptionOfTxAnnotatedMethod() {
		Person p = new Person(null, "Walter", "White");
		assertThrows(SimulateFailureException.class, () -> personService.declarativeSavePersonErrors(p));
		Long count = operations.findByQuery(Person.class).withConsistency(REQUEST_PLUS).count();
		assertEquals(0, count, "should have done roll back and left 0 entries");
	}

	@Test
	public void shouldRollbackAfterExceptionOfTxAnnotatedMethodReactive() {
		Person p = new Person(null, "Walter", "White");
		assertThrows(SimulateFailureException.class, () -> personService.declarativeSavePersonErrorsReactive(p).block());
		Long count = operations.findByQuery(Person.class).withConsistency(REQUEST_PLUS).count();
		assertEquals(0, count, "should have done roll back and left 0 entries");
	}

	@Test
	public void commitShouldPersistTxEntriesOfTxAnnotatedMethod() {
		Person p = new Person(null, "Walter", "White");
		Person s = personService.declarativeSavePerson(p);
		Long count = operations.findByQuery(Person.class).withConsistency(REQUEST_PLUS).count();
		assertEquals(1, count, "should have saved and found 1");
	}

	@Disabled("because hanging - requires JCBC-1955 fix")
	@Test
	public void concurrentTxns() {
		Runnable r = () -> {
			Thread t = Thread.currentThread();
			System.out.printf("Started thread %d %s%n", t.getId(), t.getName());
			Person p = new Person(null, "Walter", "White");
			Person s = personService.declarativeSavePersonWithThread(p, t);
			System.out.printf("Finished thread %d %s%n", t.getId(), t.getName());
		};
		List<Thread> threads = new ArrayList<>();
		for (int i = 0; i < 99; i ++) {
			Thread t = new Thread(r);
			t.start();
			threads.add(t);
		}
		threads.forEach(t -> {
			try {
				System.out.printf("Waiting for thread %d %s%n", t.getId(), t.getName());
				t.join();
				System.out.printf("Finished waiting for thread %d %s%n", t.getId(), t.getName());
			} catch (InterruptedException e) {
				fail();
			}
		});
	}

	@Test
	public void replaceInTxAnnotatedCallback() {
		Person person = new Person(1, "Walter", "White");
		Person switchedPerson = new Person(1, "Dave", "Reynolds");
		cbTmpl.insertById(Person.class).one(person);
		AtomicInteger tryCount = new AtomicInteger(0);
		Person p = personService.declarativeFindReplacePersonCallback(person, tryCount);
		Person pFound = rxCBTmpl.findById(Person.class).one(person.getId().toString()).block();
		assertEquals(switchedPerson.getFirstname(), pFound.getFirstname(), "should have been switched");
	}

	@Test
	public void replaceTwiceInTxAnnotatedCallback() {
		Person person = new Person(1, "Walter", "White");
		Person switchedPerson = new Person(1, "Dave", "Reynolds");
		cbTmpl.insertById(Person.class).one(person);
		AtomicInteger tryCount = new AtomicInteger(0);
		Person p = personService.declarativeFindReplacePersonCallback(person, tryCount);
		Person pFound = rxCBTmpl.findById(Person.class).one(person.getId().toString()).block();
		assertEquals(switchedPerson.getFirstname(), pFound.getFirstname(), "should have been switched");
	}


	@Test
	public void commitShouldPersistTxEntriesOfTxAnnotatedMethodReactive() {
		Person p = new Person(null, "Walter", "White");
		Person s = personService.declarativeSavePersonReactive(p).block();
		Long count = operations.findByQuery(Person.class).withConsistency(REQUEST_PLUS).count();
		assertEquals(1, count, "should have saved and found 1");
	}


	@Configuration
	@EnableCouchbaseRepositories("org.springframework.data.couchbase")
	@EnableReactiveCouchbaseRepositories("org.springframework.data.couchbase")
	static class Config extends AbstractCouchbaseConfiguration {

		@Override
		protected void configureEnvironment(final ClusterEnvironment.Builder builder) {
			// todo gp for test clarity
			builder.transactionsConfig(TransactionsConfig.cleanupConfig(TransactionsCleanupConfig.builder()
					.cleanupLostAttempts(false)));
		}

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


	@Service
	@Component
	@EnableTransactionManagement
	static
	// @Transactional(transactionManager = BeanNames.COUCHBASE_TRANSACTION_MANAGER)
	class PersonService {

		final CouchbaseOperations personOperations;
		final CouchbaseSimpleCallbackTransactionManager manager; // final ReactiveCouchbaseTransactionManager manager;
		final ReactiveCouchbaseOperations personOperationsRx;
		final ReactiveCouchbaseTransactionManager managerRx;

		public PersonService(CouchbaseOperations ops, 	CouchbaseSimpleCallbackTransactionManager mgr, ReactiveCouchbaseOperations opsRx,
				ReactiveCouchbaseTransactionManager mgrRx) {
			personOperations = ops;
			manager = mgr;
			System.err.println("operations cluster  : " + personOperations.getCouchbaseClientFactory().getCluster());
//			System.err.println("manager cluster     : " + manager.getDatabaseFactory().getCluster());
			System.err.println("manager Manager     : " + manager);

			personOperationsRx = opsRx;
			managerRx = mgrRx;
			System.out
					.println("operationsRx cluster  : " + personOperationsRx.getCouchbaseClientFactory().getCluster().block());
			System.out.println("managerRx cluster     : " + mgrRx.getDatabaseFactory().getCluster().block());
			System.out.println("managerRx Manager     : " + managerRx);
			return;
		}

		@Transactional(transactionManager = BeanNames.COUCHBASE_TRANSACTION_MANAGER)
		public Person declarativeSavePerson(Person person) {
			assertInAnnotationTransaction(true);
			long currentThreadId = Thread.currentThread().getId();
			System.out.println(String.format("Thread %d %s", Thread.currentThread().getId(), Thread.currentThread().getName()));
			Person ret = personOperations.insertById(Person.class).one(person);
			System.out.println(String.format("Thread %d (was %d) %s", Thread.currentThread().getId(), currentThreadId, Thread.currentThread().getName()));
			if (currentThreadId != Thread.currentThread().getId()) {
				throw new IllegalStateException();
			}
			return ret;
		}

		@Transactional(transactionManager = BeanNames.COUCHBASE_TRANSACTION_MANAGER)
		public Person declarativeSavePersonWithThread(Person person, Thread thread) {
			assertInAnnotationTransaction(true);
			long currentThreadId = Thread.currentThread().getId();
			System.out.printf("Thread %d %s, started from %d %s%n", Thread.currentThread().getId(), Thread.currentThread().getName(), thread.getId(), thread.getName());
			Person ret = personOperations.insertById(Person.class).one(person);
			System.out.printf("Thread %d (was %d) %s, started from %d %s%n", Thread.currentThread().getId(), currentThreadId, Thread.currentThread().getName(), thread.getId(), thread.getName());
			if (currentThreadId != Thread.currentThread().getId()) {
				throw new IllegalStateException();
			}
			return ret;
		}

		@Transactional(transactionManager = BeanNames.COUCHBASE_TRANSACTION_MANAGER)
		public Person declarativeSavePersonErrors(Person person) {
			assertInAnnotationTransaction(true);
			Person p = personOperations.insertById(Person.class).one(person); //
			SimulateFailureException.throwEx();
			return p;
		}

		@Autowired CouchbaseSimpleCallbackTransactionManager callbackTm;

		/**
		 * to execute while ThreadReplaceloop() is running should force a retry
		 *
		 * @param person
		 * @return
		 */
		@Transactional(transactionManager = BeanNames.COUCHBASE_TRANSACTION_MANAGER)
		public Person declarativeFindReplacePersonCallback(Person person, AtomicInteger tryCount) {
			assertInAnnotationTransaction(true);
			System.err.println("declarativeFindReplacePersonCallback try: " + tryCount.incrementAndGet());
//			System.err.println("declarativeFindReplacePersonCallback cluster : "
//					+ callbackTm.template().getCouchbaseClientFactory().getCluster().block());
//			System.err.println("declarativeFindReplacePersonCallback resourceHolder : "
//					+ org.springframework.transaction.support.TransactionSynchronizationManager
//							.getResource(callbackTm.template().getCouchbaseClientFactory().getCluster().block()));
			Person p = personOperations.findById(Person.class).one(person.getId().toString());
			return personOperations.replaceById(Person.class).one(p);
		}

		@Transactional(transactionManager = BeanNames.COUCHBASE_TRANSACTION_MANAGER)
		public Person declarativeFindReplaceTwicePersonCallback(Person person, AtomicInteger tryCount) {
			assertInAnnotationTransaction(true);
			System.err.println("declarativeFindReplacePersonCallback try: " + tryCount.incrementAndGet());
//			System.err.println("declarativeFindReplacePersonCallback cluster : "
//					+ callbackTm.template().getCouchbaseClientFactory().getCluster().block());
//			System.err.println("declarativeFindReplacePersonCallback resourceHolder : "
//					+ org.springframework.transaction.support.TransactionSynchronizationManager
//							.getResource(callbackTm.template().getCouchbaseClientFactory().getCluster().block()));
			Person p = personOperations.findById(Person.class).one(person.getId().toString());
			Person pUpdated = personOperations.replaceById(Person.class).one(p);
			return personOperations.replaceById(Person.class).one(pUpdated);
		}


		/**
		 * to execute while ThreadReplaceloop() is running should force a retry
		 *
		 * @param person
		 * @return
		 */
		@Transactional(transactionManager = BeanNames.REACTIVE_COUCHBASE_TRANSACTION_MANAGER)
		public Mono<Person> declarativeFindReplacePersonReactive(Person person, AtomicInteger tryCount) {
			assertInAnnotationTransaction(true);
			System.err.println("declarativeFindReplacePersonReactive try: " + tryCount.incrementAndGet());
			/*  NoTransactionInContextException
			TransactionSynchronizationManager.forCurrentTransaction().flatMap( sm -> {
				System.err.println("declarativeFindReplacePersonReactive reactive resourceHolder : "+sm.getResource(callbackTm.template().getCouchbaseClientFactory().getCluster().block()));
				return Mono.just(sm);
			}).block();
			*/
			return personOperationsRx.findById(Person.class).one(person.getId().toString())
					.flatMap(p -> personOperationsRx.replaceById(Person.class).one(p));
		}

		/**
		 * to execute while ThreadReplaceloop() is running should force a retry
		 *
		 * @param person
		 * @return
		 */
		@Transactional(transactionManager = BeanNames.COUCHBASE_TRANSACTION_MANAGER) // doesn't retry
		public Person declarativeFindReplacePerson(Person person, AtomicInteger tryCount) {
			assertInAnnotationTransaction(true);
			System.err.println("declarativeFindReplacePerson try: " + tryCount.incrementAndGet());
			Person p = personOperations.findById(Person.class).one(person.getId().toString());
			return personOperations.replaceById(Person.class).one(p);
		}

		@Transactional(transactionManager = BeanNames.REACTIVE_COUCHBASE_TRANSACTION_MANAGER) // doesn't retry
		public Mono<Person> declarativeSavePersonReactive(Person person) {
			assertInAnnotationTransaction(true);
			return personOperationsRx.insertById(Person.class).one(person);
		}

		@Transactional(transactionManager = BeanNames.REACTIVE_COUCHBASE_TRANSACTION_MANAGER)
		public Mono<Person> declarativeSavePersonErrorsReactive(Person person) {
			assertInAnnotationTransaction(true);
			Mono<Person> p = personOperationsRx.insertById(Person.class).one(person); //
			SimulateFailureException.throwEx();
			return p;
		}

		void assertInAnnotationTransaction(boolean inTransaction) {
			StackTraceElement[] stack = Thread.currentThread().getStackTrace();
			for (StackTraceElement ste : stack) {
				if (ste.getClassName().startsWith("org.springframework.transaction.interceptor")) {
					if (inTransaction) {
						return;
					}
				}
			}
			if (!inTransaction) {
				return;
			}
			throw new RuntimeException(
					"in transaction = " + (!inTransaction) + " but expected in annotation transaction = " + inTransaction);
		}

	}
}
