/*
 * Copyright 2021 the original author or authors
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
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.function.Function;

import com.couchbase.client.core.error.CasMismatchException;
import com.couchbase.client.java.query.QueryOptions;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.support.GenericApplicationContext;
import org.springframework.dao.DataIntegrityViolationException;
import org.springframework.data.couchbase.CouchbaseClientFactory;
import org.springframework.data.couchbase.config.AbstractCouchbaseConfiguration;
import org.springframework.data.couchbase.core.CouchbaseTemplate;
import org.springframework.data.couchbase.repository.config.EnableCouchbaseRepositories;
import org.springframework.data.couchbase.repository.config.EnableReactiveCouchbaseRepositories;
import org.springframework.data.couchbase.util.Capabilities;
import org.springframework.data.couchbase.util.ClusterType;
import org.springframework.data.couchbase.util.IgnoreWhen;
import org.springframework.data.couchbase.util.JavaIntegrationTests;
import org.springframework.test.context.junit.jupiter.SpringJUnitConfig;
import org.springframework.transaction.TransactionUsageException;
import org.springframework.transaction.support.DefaultTransactionDefinition;

import com.couchbase.client.core.cnc.Event;
import com.couchbase.transactions.TransactionDurabilityLevel;
import com.couchbase.transactions.config.TransactionConfig;
import com.couchbase.transactions.config.TransactionConfigBuilder;
import com.couchbase.transactions.error.TransactionFailed;

/**
 * Tests for com.couchbase.transactions without using the spring data transactions framework
 *
 * @author Michael Reiche
 */
@IgnoreWhen(missesCapabilities = Capabilities.QUERY, clusterTypes = ClusterType.MOCKED)
@SpringJUnitConfig(CouchbaseTransactionalTests.Config.class)
public class CouchbaseTransactionalTests extends JavaIntegrationTests {

	@Autowired CouchbaseClientFactory couchbaseClientFactory;
	@Autowired CouchbaseTemplate template;
	@Autowired CouchbaseTransactionManager txManager;
	@Autowired MyAirlineRepository airlineRepo;
	@Autowired MyAirlineNoKeyRepository airlineNoKeyRepo;
	@Autowired TransactionConfig transactionConfig;
	boolean reuseTxManager = false; // logging output may be delayed when set to true

	static GenericApplicationContext context;
	String myAirline = "My Airline";
	String yourAirline = "Your Airline";

	@BeforeAll
	public static void beforeAll() {
		callSuperBeforeAll(new Object() {});
	}

	@AfterAll
	public static void afterAll() {
		callSuperAfterAll(new Object() {});
		if (context != null) {
			context.close();
		}
	}

	@BeforeEach
	public void beforeEachTest() {
		removeAll(Airline.class);
		removeAll(AirlineNoKey.class);
		if (!reuseTxManager) {
			txManager = new CouchbaseTransactionManager(template, transactionConfig);
		}
	}

	@AfterEach
	public void afterEachTest() {
		if (!reuseTxManager) {
			txManager.destroy();
		}
	}

	String methodName() {
		return Thread.currentThread().getStackTrace()[2].getMethodName();
	}

	@Test
	void queryAndCountCommitted() {
		String airlineId = methodName();
		String threadName = Thread.currentThread().getName();
		txManager.execute(new DefaultTransactionDefinition(), (status) -> {
			if (!Thread.currentThread().getName().equals(threadName)) {
				System.err.println("Not in the same thread" + Thread.currentThread().getName() + " launched from " + threadName);
			}
			Airline a = template.insertById(Airline.class).one(new Airline(airlineId, myAirline));
			Airline b = template.insertById(Airline.class).one(new Airline(airlineId + "_your", yourAirline));
			return status;
		});
		List<Airline> airlines = template.findByQuery(Airline.class).withConsistency(REQUEST_PLUS).all();
		assertEquals(2, airlines.size(), "should have found uncommitted documents");
		long count = template.findByQuery(Airline.class).count();
		assertEquals(2, count, "should have found uncommitted documents");
	}

	@Test
	void queryAndCountCommittedRepo() {
		String airlineId = methodName();
		txManager.execute(new DefaultTransactionDefinition(), (status) -> {
			Airline a = airlineRepo.save(new Airline(airlineId, myAirline));
			Airline b = airlineRepo.save(new Airline(airlineId + "_your", yourAirline));
			return status;
		});

		// Iterable<Airline> persons = airlineRepo.withOptions(QueryOptions.queryOptions().scanConsistency(REQUEST_PLUS))
		// .findAll();
		List<Airline> airlines = template.findByQuery(Airline.class).withConsistency(REQUEST_PLUS).all();

		Iterable<Airline> persons = airlineRepo.findAll();
		int airlinesCount = 0;
		for (Airline p : persons) {
			airlinesCount++;
		}
		assertEquals(2, airlinesCount, "should have found committed documents");
		long count = airlineRepo.count();
		assertEquals(2, count, "should have found committed documents");
	}

	@Test
	void queryAndCountUncommitted() {
		String airlineId = methodName();
		try {
			txManager.execute(new DefaultTransactionDefinition(), (status) -> {
				Airline a = template.insertById(Airline.class).one(new Airline(airlineId, myAirline));
				Airline b = template.insertById(Airline.class).one(new Airline(airlineId + "_your", yourAirline));
				List<Airline> airlines = template.findByQuery(Airline.class).all();
				assertEquals(2, airlines.size(), "should have found uncommitted documents");
				long count = template.findByQuery(Airline.class).count();
				assertEquals(2, count, "should have found uncommitted documents");
				throwPoofException();
				return status;
			});
		} catch (TransactionFailed e) {
			System.err.println(e);
			if (!(e.getCause() instanceof PoofException)) {
				throw new RuntimeException(e.getCause());
			}
		}
		List<Airline> airlines = template.findByQuery(Airline.class).all();
		assertEquals(0, airlines.size(), "should not have have found uncommitted documents");
		long count = template.findByQuery(Airline.class).count();
		assertEquals(0, count, "should not have found uncommitted documents");
	}

	@Test
	void queryAndCountUncommittedRepo() {
		String airlineId = methodName();
		try {
			txManager.execute(new DefaultTransactionDefinition(), (status) -> {
				Airline a = airlineRepo.save(new Airline(airlineId, myAirline));
				Airline b = airlineRepo.save(new Airline(airlineId + "_your", yourAirline));
				Iterable<Airline> persons = airlineRepo.findAll();
				int airlinesCount = 0;
				for (Airline p : persons) {
					airlinesCount++;
				}
				assertEquals(2, airlinesCount, "should have found uncommitted documents");
				long count = airlineRepo.count();
				assertEquals(2, count, "should have found uncommitted documents");
				throwPoofException();
				return status;
			});
		} catch (TransactionFailed e) {
			System.err.println(e);
			if (!(e.getCause() instanceof PoofException)) {
				throw new RuntimeException(e.getCause());
			}
		}

		Iterable<Airline> persons = airlineRepo.findAll();
		int airlinesCount = 0;
		for (Airline p : persons) {
			airlinesCount++;
		}
		assertEquals(0, airlinesCount, "should not have found uncommitted documents");
		long count = airlineRepo.count();
		assertEquals(0, count, "should not have found uncommitted documents");
	}

	@Test
	void concurrentUpdate() {
		concurrentTemplate(1, 100, true, true);
	}

	@Test
	void concurrentUpdateNotrans() {
		concurrentTemplate(1, 100, true, false);
	}

	@Test
	void concurrentUpdateRepo() {
		concurrentRepo(1, 100, true, true);
	}

	@Test
	void concurrentUpdateNoTransRepo() {
		concurrentRepo(1, 100, true, false);
	}

	@Test
	void maxThroughput() {
		concurrentTemplate(100, 2, false, true);
	}

	@Test
	void maxThroughputNoTrans() {
		concurrentTemplate(100, 2, true, false);
	}

	@Test
	void maxThroughputRepo() {
		concurrentRepo(100, 2, false, true);
	}

	@Test
	void maxThroughputRepoNoTrans() {
		concurrentRepo(100, 2, false, false);
	}

	@Test
	void insertCommit() {
		String airlineId = methodName();
		txManager.execute(new DefaultTransactionDefinition(), (status) -> {
			Airline a = template.insertById(Airline.class).one(new Airline(airlineId, myAirline));
			return status;
		});
		Airline b = template.findById(Airline.class).one(airlineId);
		assertNotNull(b, "not committed");
	}

	@Test
	void insertCommitRepo() {
		String airlineId = methodName();
		txManager.execute(new DefaultTransactionDefinition(), (status) -> {
			Airline a = airlineRepo.save(new Airline(airlineId, myAirline));
			return status;
		});
		Optional<Airline> b = airlineRepo.findById(airlineId);
		assertTrue(b.isPresent(), "not committed");
	}

	@Test
	void replaceCommit() {
		String airlineId = methodName();
		Airline a = template.insertById(Airline.class).one(new Airline(airlineId, myAirline));
		txManager.execute(new DefaultTransactionDefinition(), (status) -> {
			Airline b = template.findById(Airline.class).one(airlineId);
			b.name = yourAirline;
			Airline c = template.replaceById(Airline.class).one(b);
			return status;
		});
		Airline d = template.findById(Airline.class).one(airlineId);
		assertEquals(yourAirline, d.name, "not commited");
	}

	@Test
	void replaceCommitRepo() {
		String airlineId = methodName();
		Airline a = airlineRepo.save(new Airline(airlineId, myAirline));
		txManager.execute(new DefaultTransactionDefinition(), (status) -> {
			Optional<Airline> b = airlineRepo.findById(airlineId);
			b.get().name = yourAirline;
			Airline c = airlineRepo.save(b.get());
			return status;
		});
		Optional<Airline> d = airlineRepo.findById(airlineId);
		assertEquals(yourAirline, d.get().name, "not commited");
	}

	@Test
	void replaceCommitNoKey() {
		String airlineId = methodName();
		AirlineNoKey a = template.insertById(AirlineNoKey.class).one(new AirlineNoKey(airlineId, myAirline));
		txManager.execute(new DefaultTransactionDefinition(), (status) -> {
			AirlineNoKey b = template.findById(AirlineNoKey.class).one(airlineId);
			b.name = yourAirline;
			AirlineNoKey c = template.replaceById(AirlineNoKey.class).one(b);
			return status;
		});
		AirlineNoKey c = template.findById(AirlineNoKey.class).one(airlineId);
		assertEquals(yourAirline, c.name, "not commited");
	}

	@Test
	void replaceCommitNoKeyRepo() {
		String airlineId = methodName();
		AirlineNoKey a = airlineNoKeyRepo.save(new AirlineNoKey(airlineId, myAirline));
		txManager.execute(new DefaultTransactionDefinition(), (status) -> {
			Optional<AirlineNoKey> b = airlineNoKeyRepo.findById(airlineId);
			b.get().name = yourAirline;
			AirlineNoKey c = airlineNoKeyRepo.save(b.get());
			return status;
		});
		Optional<AirlineNoKey> d = airlineNoKeyRepo.findById(airlineId);
		assertEquals(yourAirline, d.get().name, "not commited");
	}

	@Test
	void replaceCommitImmutable() {
		String airlineId = methodName();
		Airline a = template.insertById(Airline.class).one(new Airline(airlineId, myAirline));
		txManager.execute(new DefaultTransactionDefinition(), (status) -> {
			Airline b = template.findById(Airline.class).one(airlineId);
			Airline c = template.replaceById(Airline.class).one(b.withName(yourAirline));
			return status;
		});
		Airline d = template.findById(Airline.class).one(airlineId);
		assertEquals(yourAirline, d.name, "not committed");
	}

	@Test
	void replaceCommitImmutableRepo() {
		String airlineId = methodName();
		Airline a = airlineRepo.save(new Airline(airlineId, myAirline));
		txManager.execute(new DefaultTransactionDefinition(), (status) -> {
			Optional<Airline> b = airlineRepo.findById(airlineId);
			Airline c = airlineRepo.save(b.get().withName(yourAirline));
			return status;
		});
		Optional<Airline> d = airlineRepo.findById(airlineId);
		assertEquals(yourAirline, d.get().name, "not commited");
	}

	@Test
	void replaceCommitImmutableNoKeyValue() {
		String airlineId = methodName();
		Airline a = template.insertById(Airline.class).one(new Airline(airlineId, myAirline));
		try {
			txManager.execute(new DefaultTransactionDefinition(), (status) -> {
				Airline b = template.findById(Airline.class).one(airlineId);
				Airline c = new Airline(b.id, b.name); // copy data, but not txResultKey
				Airline d = template.replaceById(Airline.class).one(c);
				return status;
			});
		} catch (TransactionFailed e) {
			System.err.println(e);
			if (!(e.getCause() instanceof TransactionUsageException)) {
				throw new RuntimeException(e.getCause());
			}
			if (!e.getCause().getMessage().contains("field is null")) {
				throw new RuntimeException(e);
			}
		}
		Airline e = template.findById(Airline.class).one(airlineId);
		assertEquals(myAirline, e.name, "not rolled back");
	}

	@Test
	void replaceCommitImmutableNoKeyValueRepo() {
		String airlineId = methodName();
		Airline a = airlineRepo.save(new Airline(airlineId, myAirline));
		try {
			txManager.execute(new DefaultTransactionDefinition(), (status) -> {
				Optional<Airline> b = airlineRepo.findById(airlineId);
				Airline c = new Airline(airlineId, yourAirline); // copy data, but not txResultKey
				c.version = b.get().version; // repo needs version to determine if save is insert or replace
				Airline d = airlineRepo.save(c);
				return status;
			});
		} catch (TransactionFailed e) {
			System.err.println(e);
			if (!(e.getCause() instanceof TransactionUsageException)) {
				throw new RuntimeException(e.getCause());
			}
			if (!e.getCause().getMessage().contains("field is null")) {
				throw new RuntimeException(e);
			}
		}
		Optional<Airline> e = airlineRepo.findById(airlineId);
		assertEquals(myAirline, e.get().name, "not rolledback");
	}

	@Test
	void replaceCommitImmutableNoKey() {
		String airlineId = methodName();
		AirlineNoKey a = template.insertById(AirlineNoKey.class).one(new AirlineNoKey(airlineId, myAirline));
		try {
			txManager.execute(new DefaultTransactionDefinition(), (status) -> {
				AirlineNoKey b = template.findById(AirlineNoKey.class).one(airlineId);
				AirlineNoKey c = template.replaceById(AirlineNoKey.class).one(b.withName(yourAirline));
				return status;
			});
		} catch (TransactionFailed e) {
			System.err.println(e);
			if (!(e.getCause() instanceof TransactionUsageException)) {
				throw new RuntimeException(e.getCause());
			}
			if (!e.getCause().getMessage().contains("does not have")) {
				throw new RuntimeException(e);
			}
		}
		AirlineNoKey d = template.findById(AirlineNoKey.class).one(airlineId);
		assertEquals(myAirline, d.name, "not rolled back");
	}

	@Test
	void replaceCommitImmutableNoKeyRepo() {
		String airlineId = methodName();
		Airline a = airlineRepo.save(new Airline(airlineId, myAirline));
		try {
			txManager.execute(new DefaultTransactionDefinition(), (status) -> {
				Optional<AirlineNoKey> b = airlineNoKeyRepo.findById(airlineId);
				AirlineNoKey c = new AirlineNoKey(airlineId, yourAirline);
				c.version = b.get().version;
				AirlineNoKey d = airlineNoKeyRepo.save(c);
				return status;
			});
		} catch (TransactionFailed e) {
			System.err.println(e);
			if (!(e.getCause() instanceof TransactionUsageException)) {
				throw new RuntimeException(e.getCause());
			}
			if (!e.getCause().getMessage().contains("does not have")) {
				throw new RuntimeException(e);
			}
		}
		Optional<AirlineNoKey> e = airlineNoKeyRepo.findById(airlineId);
		assertEquals(myAirline, e.get().name, "not rolledback");
	}

	@Test
	void removeCommit() {
		String airlineId = methodName();
		Airline a = template.insertById(Airline.class).one(new Airline(airlineId, myAirline));
		txManager.execute(new DefaultTransactionDefinition(), (status) -> {
			Airline b = template.findById(Airline.class).one(airlineId);
			template.removeById(Airline.class).one(b);
			return status;
		});
		Airline c = template.findById(Airline.class).one(airlineId);
		assertNull(c, "not committed");
	}

	@Test
	void removeCommitRepo() {
		String airlineId = methodName();
		Airline a = airlineRepo.save(new Airline(airlineId, myAirline));
		txManager.execute(new DefaultTransactionDefinition(), (status) -> {
			Optional<Airline> b = airlineRepo.findById(airlineId);
			airlineRepo.delete(b.get());
			return status;
		});
		Optional<Airline> c = airlineRepo.findById(airlineId);
		assertFalse(c.isPresent(), "not committed");
	}

	@Test
	void insertRollback() {
		String airlineId = methodName();
		try {
			txManager.execute(new DefaultTransactionDefinition(), (status) -> {
				Airline a = template.insertById(Airline.class).one(new Airline(airlineId, myAirline));
				throwPoofException();
				return status;
			});
		} catch (TransactionFailed e) {
			if (!(e.getCause() instanceof PoofException)) {
				throw new RuntimeException(e.getCause());
			}
		}
		Airline b = template.findById(Airline.class).one(airlineId);
		assertNull(b, "not rolled back");
	}

	@Test
	void insertRollbackRepo() {
		String airlineId = methodName();
		try {
			txManager.execute(new DefaultTransactionDefinition(), (status) -> {
				Airline a = airlineRepo.save(new Airline(airlineId, myAirline));
				throwPoofException();
				return status;
			});
		} catch (TransactionFailed e) {
			if (!(e.getCause() instanceof PoofException)) {
				throw new RuntimeException(e.getCause());
			}
		}
		Optional<Airline> a = airlineRepo.findById(airlineId);
		assertFalse(a.isPresent(), "not rolled back");
	}

	@Test
	void replaceRollback() {
		String airlineId = methodName();
		Airline a = template.insertById(Airline.class).one(new Airline(airlineId, myAirline));
		try {
			txManager.execute(new DefaultTransactionDefinition(), (status) -> {
				Airline b = template.findById(Airline.class).one(airlineId);
				b.name = yourAirline;
				template.replaceById(Airline.class).one(b);
				throwPoofException();
				return status;
			});
		} catch (TransactionFailed e) {
			if (!(e.getCause() instanceof PoofException)) {
				throw new RuntimeException(e.getCause());
			}
		}
		Airline c = template.findById(Airline.class).one(airlineId);
		assertEquals(myAirline, c.name, "not rolled back");
	}

	@Test
	void replaceRollbackRepo() {
		String airlineId = methodName();
		Airline a = airlineRepo.save(new Airline(airlineId, myAirline));
		try {
			txManager.execute(new DefaultTransactionDefinition(), (status) -> {
				Optional<Airline> b = airlineRepo.findById(airlineId);
				b.get().name = yourAirline;
				Airline c = airlineRepo.save(b.get());
				throwPoofException();
				return status;
			});
		} catch (TransactionFailed e) {
			if (!(e.getCause() instanceof PoofException)) {
				throw new RuntimeException(e.getCause());
			}
		}
		Optional<Airline> c = airlineRepo.findById(airlineId);
		assertEquals(myAirline, c.get().name, "not rolled back");
	}

	@Test
	void removeRollback() {
		String airlineId = methodName();
		Airline a = template.insertById(Airline.class).one(new Airline(airlineId, myAirline));
		try {
			txManager.execute(new DefaultTransactionDefinition(), (status) -> {
				Airline b = template.findById(Airline.class).one(airlineId);
				template.removeById(Airline.class).one(b);
				throwPoofException();
				return status;
			});
		} catch (TransactionFailed e) {
			if (!(e.getCause() instanceof PoofException)) {
				throw new RuntimeException(e.getCause());
			}
		}
		Airline c = template.findById(Airline.class).one(airlineId);
		assertNotNull(c, "not rolled back");
	}

	@Test
	void removeRollbackRepo() {
		String airlineId = methodName();
		Airline a = airlineRepo.save(new Airline(airlineId, myAirline));
		try {
			txManager.execute(new DefaultTransactionDefinition(), (status) -> {
				Optional<Airline> b = airlineRepo.findById(airlineId);
				airlineRepo.delete(b.get());
				throwPoofException();
				return status;
			});
		} catch (TransactionFailed e) {
			if (!(e.getCause() instanceof PoofException)) {
				throw new RuntimeException(e.getCause());
			}
		}
		Optional<Airline> c = airlineRepo.findById(airlineId);
		assertTrue(c.isPresent(), "not rolled back");
	}

	@Test
	void replaceCommitKeyIsNull() {
		String airlineId = methodName();
		Airline a = template.insertById(Airline.class).one(new Airline(airlineId, myAirline));
		try {
			txManager.execute(new DefaultTransactionDefinition(), (status) -> {
				Airline b = template.findById(Airline.class).one(airlineId);
				Airline c = new Airline(airlineId, yourAirline);
				Airline d = template.replaceById(Airline.class).one(c);// not the same object, and key is null
				return status;
			});
		} catch (TransactionFailed e) {
			System.err.println(e);
			if (!(e.getCause() instanceof TransactionUsageException)) {
				throw new RuntimeException(e.getCause());
			}
			if (!e.getCause().getMessage().contains("field is null")) {
				throw new RuntimeException(e);
			}
		}
		Airline e = template.findById(Airline.class).one(airlineId);
		assertEquals(myAirline, e.name, "not rolledback");
	}

	@Test
	void replaceCommitKeyIsNullRepo() {
		String airlineId = methodName();
		Airline a = airlineRepo.save(new Airline(airlineId, myAirline));
		try {
			txManager.execute(new DefaultTransactionDefinition(), (status) -> {
				Optional<Airline> b = airlineRepo.findById(airlineId);
				Airline c = new Airline(airlineId, yourAirline); // not the same object, and key is null
				c.version = b.get().version; // repository need version to determine if save is insert or replace
				Airline d = airlineRepo.save(c);
				return status;
			});
		} catch (TransactionFailed e) {
			System.err.println(e);
			if (!(e.getCause() instanceof TransactionUsageException)) {
				throw new RuntimeException(e.getCause());
			}
			if (!e.getCause().getMessage().contains("field is null")) {
				throw new RuntimeException(e);
			}
		}
		Optional<Airline> e = airlineRepo.findById(airlineId);
		assertEquals(myAirline, e.get().name, "not rolledback");
	}

	@Test
	void replaceCommitKeyIsWrong() {
		String airlineId = methodName();
		Airline a = template.insertById(Airline.class).one(new Airline(airlineId, myAirline));
		try {
			txManager.execute(new DefaultTransactionDefinition(), (status) -> {
				Airline b = template.findById(Airline.class).one(airlineId);
				Airline c = new Airline(airlineId, yourAirline); // not the same object
				c.txResultKey = 123; // give it a bogus key
				Airline d = template.replaceById(Airline.class).one(c);
				return status;
			});
		} catch (TransactionFailed e) {
			System.err.println(e);
			if (!(e.getCause() instanceof TransactionUsageException)) {
				throw new RuntimeException(e.getCause());
			}
			if (!e.getCause().getMessage().contains("could not find")) {
				throw new RuntimeException(e);
			}
		}
		Airline e = template.findById(Airline.class).one(airlineId);
		assertEquals(myAirline, e.name, "not rolled back");
	}

	@Test
	void replaceCommitKeyIsWrongRepo() {
		String airlineId = methodName();
		Airline a = airlineRepo.save(new Airline(airlineId, myAirline));
		try {
			txManager.execute(new DefaultTransactionDefinition(), (status) -> {
				Optional<Airline> b = airlineRepo.findById(airlineId);
				Airline c = new Airline(airlineId, yourAirline); // not the same object
				c.version = b.get().version; // repository needs version to determine if save is insert or replace
				c.txResultKey = 123; // give it a bogus key
				Airline d = airlineRepo.save(c);
				return status;
			});
		} catch (TransactionFailed e) {
			System.err.println(e);
			if (!(e.getCause() instanceof TransactionUsageException)) {
				throw new RuntimeException(e.getCause());
			}
			if (!e.getCause().getMessage().contains("could not find")) {
				throw new RuntimeException(e);
			}
		}
		Optional<Airline> e = airlineRepo.findById(airlineId);
		assertEquals(myAirline, e.get().name, "not rolledback");
	}

	private <T> void removeAll(Class<T> clazz) {
		template.removeByQuery(clazz).withConsistency(REQUEST_PLUS).all();
		List<T> list = template.findByQuery(clazz).withConsistency(REQUEST_PLUS).all();
		if (!list.isEmpty()) {
			throw new RuntimeException("there are some " + clazz + " left over");
		}
	}

	private void throwPoofException() {
		throw new PoofException();
	}

	private void concurrentTemplate(int numDocs, int numThreadsPerDoc, boolean shouldHaveRetries, boolean useTrans) {
		for (int i = 0; i < numDocs; i++) {
			Airline a = template.insertById(Airline.class).one(new Airline("" + i, myAirline));
		}
		// Create threads that increment the number of aircraft by one.
		int numThreads = numDocs * numThreadsPerDoc;
		List<TxRunner> runners = new ArrayList<>(numThreads);
		// use two txManagers because execute() uses sychronize(). logging set to WARN
		CouchbaseTransactionManager thisTxManager = new CouchbaseTransactionManager(template, txConfigLoadTest());
		CouchbaseTransactionManager thatTxManager = new CouchbaseTransactionManager(template, txConfigLoadTest());
		for (int i = 0; i < numThreads; i++) {
			CouchbaseTransactionManager theTxManager = (i % 2) == 0 ? thisTxManager : thatTxManager;
			TxRunner txRunner = new TxRunner("" + (i % numDocs), theTxManager, useTrans, (id) -> {
				Airline al = template.findById(Airline.class).one(id);
				sleepMs(10);
				return template.replaceById(Airline.class).one(al.withNumAircraft(al.numAircraft + 1));
			});
			runners.add(txRunner);
		}
		// Start the threads.
		long t0 = System.currentTimeMillis();
		for (TxRunner t : runners) {
			t.start();
		}

		// Wait for threads to complete and count up the tries.
		int tries = 0;
		for (TxRunner t : runners) {
			try {
				t.join();
			} catch (InterruptedException e) {
				System.err.println(e);
			}
			tries = tries + t.tries;
		}

		thisTxManager.destroy();
		thatTxManager.destroy();
		if (shouldHaveRetries) {
			assertNotEquals(0, tries - runners.size(), "there should have been a retries due to CAS-mismatch");
		}
		for (int i = 0; i < numDocs; i++)

		{
			Airline b = template.findById(Airline.class).one("" + i);
			assertEquals(runners.size() / numDocs, b.numAircraft, "should have been incremented once in each thread");
		}
		System.err.println(methodName() + " runners:" + runners.size() + " docs: " + numDocs + " retries:"
				+ (tries - runners.size()) + " elapsedMs: " + (System.currentTimeMillis() - t0));
	}

	void concurrentRepo(int numDocs, int threadsPerDoc, boolean shouldHaveRetries, boolean useTrans) {
		for (int i = 0; i < numDocs; i++) {
			Airline a = template.insertById(Airline.class).one(new Airline("" + i, myAirline));
		}
		// Create threads that increment the number of aircraft by one
		int numThreads = numDocs * threadsPerDoc;
		List<TxRunner> runners = new ArrayList<>(numThreads);
		// use two txManagers because execute() uses sychronize(). logging set to WARN
		CouchbaseTransactionManager thisTxManager = new CouchbaseTransactionManager(template, txConfigLoadTest());
		CouchbaseTransactionManager thatTxManager = new CouchbaseTransactionManager(template, txConfigLoadTest());
		for (int i = 0; i < numThreads; i++) {
			CouchbaseTransactionManager theTxManager = (i % 2) == 0 ? thisTxManager : thatTxManager;
			TxRunner txRunner = new TxRunner("" + (i % numDocs), theTxManager, useTrans, (id) -> {
				Airline al = airlineRepo.findById(id).get();
				//sleepMs(10);
				return airlineRepo.save(al.withNumAircraft(al.numAircraft + 1));
			});
			runners.add(txRunner);
		}
		// Start the threads.
		long t0 = System.currentTimeMillis();
		for (TxRunner t : runners) {
			t.start();
		}
		// Wait for threads to complete and count up the tries.
		int tries = 0;
		for (TxRunner t : runners) {
			try {
				t.join();
			} catch (InterruptedException e) {
				System.err.println(e);
			}
			tries = tries + t.tries;
		}
		thisTxManager.destroy();
		thatTxManager.destroy();
		if (shouldHaveRetries) {
			assertNotEquals(0, tries - runners.size(), "there should have been a retries due to CAS-mismatch");
		}
		for (int i = 0; i < numDocs; i++) {
			Airline b = airlineRepo.findById("" + i).get();
			assertEquals(runners.size() / numDocs, b.numAircraft, "should have been incremented once in each thread");
		}
		System.err.println(methodName() + " runners:" + runners.size() + " docs: " + numDocs + " retries:"
				+ (tries - runners.size()) + " elapsedMs: " + (System.currentTimeMillis() - t0));
	}

	TransactionConfig txConfigLoadTest() {
		return TransactionConfigBuilder.create().logDirectly(Event.Severity.WARN).logOnFailure(true, Event.Severity.ERROR)
				.expirationTime(Duration.ofMinutes(10)).durabilityLevel(TransactionDurabilityLevel.NONE).build();
	}

	class PoofException extends RuntimeException {}

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
			return username();
		}

		@Override
		public String getPassword() {
			return password();
		}

		@Override
		public String getBucketName() {
			return bucketName();
		}

		@Override
		public TransactionConfig transactionConfig() {
			return TransactionConfigBuilder.create().logDirectly(Event.Severity.INFO).logOnFailure(true, Event.Severity.ERROR)
					.expirationTime(Duration.ofMinutes(10)).durabilityLevel(TransactionDurabilityLevel.NONE).build();
		}

	}

	class TxRunner extends Thread {
		String id;
		CouchbaseTransactionManager txManager;
		Function<String, Airline> action;
		boolean useTrans;
		boolean createdTxManager;
		int tries = 0;

		public TxRunner(String id, CouchbaseTransactionManager txManager, boolean useTrans,
				Function<String, Airline> action) {
			this.id = id;
			this.createdTxManager = txManager == null;
			this.txManager = txManager != null ? txManager : new CouchbaseTransactionManager(template, transactionConfig);
			this.useTrans = useTrans;
			this.action = action;
		}

		public void run() {
			if (useTrans) {
				txManager.execute(new DefaultTransactionDefinition(), (status) -> {
					tries++;
					Airline a = action.apply(id); // airlineRepo.findById(id).get();
					return status;
				});
			} else {
				Airline a = null;
				while (a == null) {
					try {
						tries++;
						a = action.apply(id); // airlineRepo.findById(id).get();
					} catch (DataIntegrityViolationException div) {
						// System.err.println("Caught "+div);
					}
				}
			}
			if (createdTxManager) {
				txManager.destroy();
			}
		}
	}

}
