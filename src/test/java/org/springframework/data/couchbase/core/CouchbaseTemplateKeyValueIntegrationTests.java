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

package org.springframework.data.couchbase.core;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;
import java.time.Duration;
import java.time.Instant;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Set;
import java.util.UUID;

import com.couchbase.client.java.Cluster;
import com.couchbase.client.java.Collection;
import com.couchbase.client.java.env.ClusterEnvironment;
import com.couchbase.client.java.json.JsonObject;
import com.couchbase.client.java.kv.GetOptions;
import com.couchbase.client.java.kv.GetResult;
import com.couchbase.client.java.kv.InsertOptions;
import com.couchbase.client.java.query.QueryResult;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.dao.DataIntegrityViolationException;
import org.springframework.dao.DuplicateKeyException;
import org.springframework.data.auditing.DateTimeProvider;
import org.springframework.data.couchbase.config.AbstractCouchbaseConfiguration;
import org.springframework.data.couchbase.core.ExecutableRemoveByIdOperation.ExecutableRemoveById;
import org.springframework.data.couchbase.core.ExecutableReplaceByIdOperation.ExecutableReplaceById;
import org.springframework.data.couchbase.core.mapping.event.ValidatingCouchbaseEventListener;
import org.springframework.data.couchbase.core.support.OneAndAllEntity;
import org.springframework.data.couchbase.domain.NaiveAuditorAware;
import org.springframework.data.couchbase.domain.PersonValue;
import org.springframework.data.couchbase.domain.User;
import org.springframework.data.couchbase.domain.UserAnnotated;
import org.springframework.data.couchbase.domain.UserAnnotated2;
import org.springframework.data.couchbase.domain.UserAnnotated3;
import org.springframework.data.couchbase.domain.UserRepository;
import org.springframework.data.couchbase.domain.time.AuditingDateTimeProvider;
import org.springframework.data.couchbase.repository.CouchbaseRepositoryQueryIntegrationTests;
import org.springframework.data.couchbase.repository.auditing.EnableCouchbaseAuditing;
import org.springframework.data.couchbase.repository.config.EnableCouchbaseRepositories;
import org.springframework.data.couchbase.util.ClusterType;
import org.springframework.data.couchbase.util.IgnoreWhen;
import org.springframework.data.couchbase.util.JavaIntegrationTests;

import com.couchbase.client.java.kv.PersistTo;
import com.couchbase.client.java.kv.ReplicateTo;
import com.couchbase.client.java.query.QueryScanConsistency;
import org.springframework.test.context.junit.jupiter.SpringJUnitConfig;
import org.springframework.validation.beanvalidation.LocalValidatorFactoryBean;

;

/**
 * KV tests Theses tests rely on a cb server running.
 *
 * @author Michael Nitschinger
 * @author Michael Reiche
 */
@IgnoreWhen(clusterTypes = ClusterType.MOCKED)
@SpringJUnitConfig(CouchbaseTemplateKeyValueIntegrationTests.Config.class)
class CouchbaseTemplateKeyValueIntegrationTests extends JavaIntegrationTests {

	@Autowired UserRepository userRepository;

	@BeforeEach
	@Override
	public void beforeEach() {
		super.beforeEach();
		couchbaseTemplate.removeByQuery(User.class).all();
		couchbaseTemplate.removeByQuery(UserAnnotated.class).all();
		couchbaseTemplate.removeByQuery(UserAnnotated2.class).all();
		couchbaseTemplate.removeByQuery(UserAnnotated3.class).all();
		couchbaseTemplate.removeByQuery(User.class).withConsistency(QueryScanConsistency.REQUEST_PLUS).all();
	}

	@Test
	void upsertAndFindById() {
		User user = new User(UUID.randomUUID().toString(), "firstname", "lastname");
		User modified = couchbaseTemplate.upsertById(User.class).one(user);
		assertEquals(user, modified);
		// create a new object so that the object returned by replaceById() is a different object from the original user
		// don't need to copy the ModifiedDate/ModifiedTime as they are not read and are overwritten.
		User modifying = new User(user.getId(), user.getFirstname(), user.getLastname());
		modifying.setCreatedDate(user.getCreatedDate());
		modifying.setCreatedBy(user.getCreatedBy());
		modifying.setVersion(user.getVersion());
		modified = couchbaseTemplate.replaceById(User.class).one(modifying);
		assertEquals(modifying, modified);
		if (user == modified) {
			throw new RuntimeException(" user == modified ");
		}
		assertNotEquals(user, modified);
		assertEquals(NaiveAuditorAware.AUDITOR, modified.getCreatedBy());
		assertEquals(NaiveAuditorAware.AUDITOR, modified.getLastModifiedBy());
		assertNotEquals(0, modified.getCreatedDate());
		assertNotEquals(0, modified.getLastModifiedDate());
		// The FixedDateTimeService of the AuditingDateTimeProvider will guarantee these are equal
		assertEquals(user.getLastModifiedDate(), modified.getLastModifiedDate());

		User badUser = new User(user.getId(), user.getFirstname(), user.getLastname());
		badUser.setVersion(12345678);
		assertThrows(DataIntegrityViolationException.class, () -> couchbaseTemplate.replaceById(User.class).one(badUser));

		User found = couchbaseTemplate.findById(User.class).one(user.getId());
		assertEquals(modified, found);

		couchbaseTemplate.removeById().one(user.getId());
	}

	@Test
	void withDurability()
			throws NoSuchMethodException, IllegalAccessException, InvocationTargetException, InstantiationException {
		Class<?> clazz = User.class; // for now, just User.class. There is no Durability annotation.
		// insert, replace, upsert
		for (OneAndAllEntity<User> operator : new OneAndAllEntity[] { couchbaseTemplate.insertById(clazz),
				couchbaseTemplate.replaceById(clazz), couchbaseTemplate.upsertById(clazz) }) {
			// create an entity of type clazz
			Constructor<?> cons = clazz.getConstructor(String.class, String.class, String.class);
			User user = (User) cons.newInstance("" + operator.getClass().getSimpleName() + "_" + clazz.getSimpleName(),
					"firstname", "lastname");

			if (clazz.equals(User.class)) { // User.java doesn't have an durability annotation
				operator = (OneAndAllEntity) ((WithDurability<User>) operator).withDurability(PersistTo.ACTIVE,
						ReplicateTo.NONE);
			}

			// if replace, we need to insert a document to replace
			if (operator instanceof ExecutableReplaceById) {
				couchbaseTemplate.insertById(User.class).one(user);
			}
			// call to insert/replace/update
			User returned = null;

			// occasionally gives "reactor.core.Exceptions$OverflowException: Could not emit value due to lack of requests"
			for (int i = 1; i != 5; i++) {
				try {
					returned = (User) operator.one(user);
					break;
				} catch (Exception ofe) {
					System.out.println("" + i + " caught: " + ofe);
					couchbaseTemplate.removeByQuery(User.class).withConsistency(QueryScanConsistency.REQUEST_PLUS).all();
					if (i == 4) {
						throw ofe;
					}
					sleepSecs(1);
				}
			}
			assertEquals(user, returned);
			User found = couchbaseTemplate.findById(User.class).one(user.getId());
			assertEquals(user, found);

			if (operator instanceof ExecutableReplaceById) {
				couchbaseTemplate.removeById().withDurability(PersistTo.ACTIVE, ReplicateTo.NONE).one(user.getId());
				User removed = (User) couchbaseTemplate.findById(user.getClass()).one(user.getId());
				assertNull(removed, "found should have been null as document should be removed");
			}
		}

	}

	@Test
	void withExpiryAndExpiryAnnotation()
			throws IllegalAccessException, InstantiationException, NoSuchMethodException, InvocationTargetException {

		// first we need to get the server's current time.
		// we will save a document with expiration 24 hours in the future, then get the expiration time and subtract 24hours

		User testUser = new User(UUID.randomUUID().toString(), "f", "l");
		Collection collection = couchbaseTemplate.getCouchbaseClientFactory().getCluster().bucket(bucketName())
				.defaultCollection();
		// set expiration 1 day in the future
		collection.insert(testUser.getId(), JsonObject.create(), InsertOptions.insertOptions().expiry(Duration.ofDays(1)));
		GetResult o = collection.get(testUser.getId(), GetOptions.getOptions().withExpiry(true));
		// read back expiration and subtract one day
		long now = o.expiryTime().get().getEpochSecond() - 24 * 60 * 60;
		// subtract from local time to get the number of seconds the server is behind the client.
		long drift = Instant.now().getEpochSecond() - now;
		System.err.println("drift : "+drift);
		// ( withExpiry()<User>, expiry=1<UserAnnotated>, expiryExpression=${myExpiry}<UserAnnotated2> ) X ( insert,
		// replace, upsert )
		Set<User> users = new HashSet<>(); // set of all documents we will insert
		// Entity classes
		int i=1;
		for (Class clazz : new Class[] { User.class, UserAnnotated.class, UserAnnotated2.class, UserAnnotated3.class }) {
			// insert, replace, upsert
			for (OneAndAllEntity<User> operator : new OneAndAllEntity[] { couchbaseTemplate.insertById(clazz),
					couchbaseTemplate.replaceById(clazz), couchbaseTemplate.upsertById(clazz) }) {

				// create an entity of type clazz
				Constructor cons = clazz.getConstructor(String.class, String.class, String.class);
				User user = (User) cons.newInstance(""+(i++)+"_"+ operator.getClass().getSimpleName() + "_" + clazz.getSimpleName(),
						"firstname", "lastname");

				if (clazz.equals(User.class)) { // User.java doesn't have an expiry annotation
					operator = (OneAndAllEntity) ((WithExpiry<User>) operator).withExpiry(Duration.ofSeconds(2));
				} else if (clazz.equals(UserAnnotated3.class)) { // override the expiry from the annotation with no expiry
					operator = (OneAndAllEntity) ((WithExpiry<User>) operator).withExpiry(Duration.ofSeconds(0));
				}

				// if replace or remove, we need to insert a document to replace
				if (operator instanceof ExecutableReplaceById || operator instanceof ExecutableRemoveById) {
					couchbaseTemplate.insertById(User.class).one(user);
				}
				// call to insert/replace/update
				User returned = operator.one(user);
				assertEquals(user, returned);
				users.add(user);
			}
		}
		// check that they are gone after a few seconds.
		sleepSecs(drift < 0 ? (int)6 : (int)drift+6 );
		List<String> errorList = new LinkedList();

		for (User user : users) {
			User found = userRepository.getById(user.getId());// couchbaseTemplate.findById(user.getClass()).one(user.getId());
			if (user.getId().endsWith(UserAnnotated3.class.getSimpleName())) {
				if (found == null) {
					errorList.add("\nfound should be non null as it was set to have no expiry " + user.getId() + " " + found);
				}
			} else {
				if (found != null) {
					errorList.add("\nfound should have been null as document should be expired " + user.getId() + " "
							+ (found.exp - Instant.now().getEpochSecond()) + " exp : " + found.exp + " now: "
							+ Instant.now().getEpochSecond());
				}
			}
		}

		if (!errorList.isEmpty()) {
			throw new RuntimeException(errorList.toString());
		}
		return;
	}

	@Test
	void findDocWhichDoesNotExist() {
		assertNull(couchbaseTemplate.findById(User.class).one(UUID.randomUUID().toString()));
	}

	@Test
	void upsertAndReplaceById() {
		User user = new User(UUID.randomUUID().toString(), "firstname", "lastname");
		User modified = couchbaseTemplate.upsertById(User.class).one(user);
		assertEquals(user, modified);

		User toReplace = new User(modified.getId(), "some other", "lastname");
		couchbaseTemplate.replaceById(User.class).one(toReplace);

		User loaded = couchbaseTemplate.findById(User.class).one(toReplace.getId());
		assertEquals("some other", loaded.getFirstname());

		couchbaseTemplate.removeById().one(toReplace.getId());

	}

	@Test
	void upsertAndRemoveById() {
		{
			User user = new User(UUID.randomUUID().toString(), "firstname", "lastname");
			User modified = couchbaseTemplate.upsertById(User.class).one(user);
			assertEquals(user, modified);

			RemoveResult removeResult = couchbaseTemplate.removeById().one(user.getId());
			assertEquals(user.getId(), removeResult.getId());
			assertTrue(removeResult.getCas() != 0);
			assertTrue(removeResult.getMutationToken().isPresent());

			assertNull(couchbaseTemplate.findById(User.class).one(user.getId()));
		}
		{
			User user = new User(UUID.randomUUID().toString(), "firstname", "lastname");
			User modified = couchbaseTemplate.upsertById(User.class).one(user);
			assertEquals(user, modified);

			// careful now - user and modified are the same object. The object has the new cas (@Version version)
			Long savedCas = modified.getVersion();
			modified.setVersion(123);
			assertThrows(DataIntegrityViolationException.class, () -> couchbaseTemplate.removeById()
					.withCas(reactiveCouchbaseTemplate.support().getCas(modified)).one(modified.getId()));
			modified.setVersion(savedCas);
			couchbaseTemplate.removeById().withCas(reactiveCouchbaseTemplate.support().getCas(modified))
					.one(modified.getId());
		}
	}

	@Test
	void insertById() {
		User user = new User(UUID.randomUUID().toString(), "firstname", "lastname");
		User inserted = couchbaseTemplate.insertById(User.class).one(user);
		assertEquals(user, inserted);
		assertThrows(DuplicateKeyException.class, () -> couchbaseTemplate.insertById(User.class).one(user));
	}

	@Test
	void insertByIdwithDurability() {
		User user = new User(UUID.randomUUID().toString(), "firstname", "lastname");
		User inserted = null;

		// occasionally gives "reactor.core.Exceptions$OverflowException: Could not emit value due to lack of requests"
		for (int i = 1; i != 5; i++) {
			try {
				inserted = couchbaseTemplate.insertById(User.class).withDurability(PersistTo.ACTIVE, ReplicateTo.NONE)
						.one(user);
				break;
			} catch (Exception ofe) {
				System.out.println("" + i + " caught: " + ofe);
				couchbaseTemplate.removeByQuery(User.class).withConsistency(QueryScanConsistency.REQUEST_PLUS).all();
				if (i == 4) {
					throw ofe;
				}
				sleepSecs(1);
			}
		}
		assertEquals(user, inserted);
		assertThrows(DuplicateKeyException.class, () -> couchbaseTemplate.insertById(User.class).one(user));
	}

	@Test
	void existsById() {
		String id = UUID.randomUUID().toString();
		assertFalse(couchbaseTemplate.existsById().one(id));

		User user = new User(id, "firstname", "lastname");
		User inserted = couchbaseTemplate.insertById(User.class).one(user);
		assertEquals(user, inserted);
		assertTrue(couchbaseTemplate.existsById().one(id));
	}

	@Test
	@IgnoreWhen(clusterTypes = ClusterType.MOCKED)
	void saveAndFindImmutableById() {
		PersonValue personValue = new PersonValue(UUID.randomUUID().toString(), 123, "f", "l");
		PersonValue inserted = null;
		PersonValue upserted = null;
		PersonValue replaced = null;

		inserted = couchbaseTemplate.insertById(PersonValue.class).one(personValue);
		assertNotEquals(0, inserted.getVersion());
		PersonValue foundInserted = couchbaseTemplate.findById(PersonValue.class).one(inserted.getId());
		assertNotNull(foundInserted, "inserted personValue not found");
		assertEquals(inserted, foundInserted);

		// upsert will insert
		couchbaseTemplate.removeById().one(inserted.getId());
		upserted = couchbaseTemplate.upsertById(PersonValue.class).one(inserted);
		assertNotEquals(0, upserted.getVersion());
		PersonValue foundUpserted = couchbaseTemplate.findById(PersonValue.class).one(upserted.getId());
		assertNotNull(foundUpserted, "upserted personValue not found");
		assertEquals(upserted, foundUpserted);

		// upsert will replace
		upserted = couchbaseTemplate.upsertById(PersonValue.class).one(inserted);
		assertNotEquals(0, upserted.getVersion());
		PersonValue foundUpserted2 = couchbaseTemplate.findById(PersonValue.class).one(upserted.getId());
		assertNotNull(foundUpserted2, "upserted personValue not found");
		assertEquals(upserted, foundUpserted2);

		replaced = couchbaseTemplate.replaceById(PersonValue.class).one(upserted);
		assertNotEquals(0, replaced.getVersion());
		PersonValue foundReplaced = couchbaseTemplate.findById(PersonValue.class).one(replaced.getId());
		assertNotNull(foundReplaced, "replaced personValue not found");
		assertEquals(replaced, foundReplaced);

	}

	private void sleepSecs(int i) {
		try {
			Thread.sleep(i * 1000);
		} catch (InterruptedException ie) {}
	}

	@Configuration
	@EnableCouchbaseRepositories("org.springframework.data.couchbase")
	@EnableCouchbaseAuditing(dateTimeProviderRef = "dateTimeProviderRef")
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

		@Bean(name = "auditorAwareRef")
		public NaiveAuditorAware testAuditorAware() {
			return new NaiveAuditorAware();
		}

		@Override
		public void configureEnvironment(final ClusterEnvironment.Builder builder) {
			builder.ioConfig().maxHttpConnections(11).idleHttpConnectionTimeout(Duration.ofSeconds(4));
			return;
		}

		@Bean(name = "dateTimeProviderRef")
		public DateTimeProvider testDateTimeProvider() {
			return new AuditingDateTimeProvider();
		}

		@Bean
		public LocalValidatorFactoryBean validator() {
			return new LocalValidatorFactoryBean();
		}

		@Bean
		public ValidatingCouchbaseEventListener validationEventListener() {
			return new ValidatingCouchbaseEventListener(validator());
		}
	}
}
