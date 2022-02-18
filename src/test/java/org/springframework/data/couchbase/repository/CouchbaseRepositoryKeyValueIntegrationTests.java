/*
 * Copyright 2012-2022 the original author or authors
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

package org.springframework.data.couchbase.repository;



import static com.couchbase.client.java.query.QueryScanConsistency.REQUEST_PLUS;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.lang.reflect.InvocationTargetException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Optional;
import java.util.UUID;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Configuration;
import org.springframework.dao.DuplicateKeyException;
import org.springframework.dao.OptimisticLockingFailureException;
import org.springframework.data.couchbase.config.AbstractCouchbaseConfiguration;
import org.springframework.data.couchbase.core.CouchbaseTemplate;
import org.springframework.data.couchbase.domain.Airline;
import org.springframework.data.couchbase.domain.AirlineRepository;
import org.springframework.data.couchbase.domain.Course;
import org.springframework.data.couchbase.domain.Library;
import org.springframework.data.couchbase.domain.LibraryRepository;
import org.springframework.data.couchbase.domain.PersonValue;
import org.springframework.data.couchbase.domain.PersonValueRepository;
import org.springframework.data.couchbase.domain.Submission;
import org.springframework.data.couchbase.domain.SubscriptionToken;
import org.springframework.data.couchbase.domain.SubscriptionTokenRepository;
import org.springframework.data.couchbase.domain.User;
import org.springframework.data.couchbase.domain.UserRepository;
import org.springframework.data.couchbase.domain.UserSubmission;
import org.springframework.data.couchbase.domain.UserSubmissionRepository;
import org.springframework.data.couchbase.repository.config.EnableCouchbaseRepositories;
import org.springframework.data.couchbase.util.ClusterAwareIntegrationTests;
import org.springframework.data.couchbase.util.ClusterType;
import org.springframework.data.couchbase.util.IgnoreWhen;
import org.springframework.test.context.junit.jupiter.SpringJUnitConfig;

import com.couchbase.client.java.kv.GetResult;

/**
 * Repository KV tests
 *
 * @author Michael Nitschinger
 * @author Michael Reiche
 */
@SpringJUnitConfig(CouchbaseRepositoryKeyValueIntegrationTests.Config.class)
@IgnoreWhen(clusterTypes = ClusterType.MOCKED)
public class CouchbaseRepositoryKeyValueIntegrationTests extends ClusterAwareIntegrationTests {

	@Autowired UserRepository userRepository;
	@Autowired LibraryRepository libraryRepository;
	@Autowired SubscriptionTokenRepository subscriptionTokenRepository;
	@Autowired UserSubmissionRepository userSubmissionRepository;
	@Autowired AirlineRepository airlineRepository;
	@Autowired PersonValueRepository personValueRepository;
	@Autowired CouchbaseTemplate couchbaseTemplate;

	@BeforeEach
	public void beforeEach() {
		super.beforeEach();
		couchbaseTemplate.removeByQuery(SubscriptionToken.class).withConsistency(REQUEST_PLUS).all();
		couchbaseTemplate.findByQuery(SubscriptionToken.class).withConsistency(REQUEST_PLUS).all();
	}

	@Test
	void subscriptionToken() {
		SubscriptionToken st = new SubscriptionToken("id", 0, "type", "Dave Smith", "app123", "dev123", 0);
		st = subscriptionTokenRepository.save(st);
		st = subscriptionTokenRepository.findById(st.getId()).get();

		GetResult jdkResult = couchbaseTemplate.getCouchbaseClientFactory().getDefaultCollection().get(st.getId());
		assertNotEquals(0, st.getVersion());
		assertEquals(jdkResult.cas(), st.getVersion());
		subscriptionTokenRepository.delete(st);
	}

	@Test
	@IgnoreWhen(clusterTypes = ClusterType.MOCKED)
	void saveReplaceUpsertInsert() {
		// the User class has a version.
		User user = new User(UUID.randomUUID().toString(), "f", "l");
		// save the document - we don't care how on this call
		userRepository.save(user);
		// Now set the version to 0, it should attempt an insert and fail.
		long saveVersion = user.getVersion();
		user.setVersion(0);
		assertThrows(DuplicateKeyException.class, () -> userRepository.save(user));
		user.setVersion(saveVersion + 1);
		assertThrows(OptimisticLockingFailureException.class, () -> userRepository.save(user));
		userRepository.delete(user);

		// Airline does not have a version
		Airline airline = new Airline(UUID.randomUUID().toString(), "MyAirline", null);
		// save the document - we don't care how on this call
		airlineRepository.save(airline);
		airlineRepository.save(airline); // If it was an insert it would fail. Can't tell if it is an upsert or replace.
		airlineRepository.delete(airline);
	}

	@Test
	@IgnoreWhen(clusterTypes = ClusterType.MOCKED)
	void saveAndFindById() {
		User user = new User(UUID.randomUUID().toString(), "saveAndFindById", "l");
		// this currently fails when using mocked in integration.properties with status "UNKNOWN"
		assertFalse(userRepository.existsById(user.getId()));

		userRepository.save(user);

		Optional<User> found = userRepository.findById(user.getId());
		assertTrue(found.isPresent());
		found.ifPresent(u -> assertEquals(user, u));

		assertTrue(userRepository.existsById(user.getId()));
		userRepository.delete(user);
	}

	@Test
	@IgnoreWhen(clusterTypes = ClusterType.MOCKED)
	void saveAndFindImmutableById() throws NoSuchMethodException, InvocationTargetException, IllegalAccessException {
		PersonValue personValue = new PersonValue(null, 0, "saveAndFindImmutableById", "l");
		personValue = personValueRepository.save(personValue);
		Optional<PersonValue> found = personValueRepository.findById(personValue.getId());
		assertTrue(found.isPresent());
		assertEquals(personValue, found.get());
		personValueRepository.delete(personValue);
	}

	@Test // DATACOUCH-564
	@IgnoreWhen(clusterTypes = ClusterType.MOCKED)
	void saveAndFindByIdWithList() {
		List<String> books = new ArrayList<>();
		books.add("book1");
		books.add("book2");
		Library library = new Library(UUID.randomUUID().toString(), books);
		// this currently fails when using mocked in integration.properties with status "UNKNOWN"
		assertFalse(libraryRepository.existsById(library.getId()));

		libraryRepository.save(library);

		Optional<Library> found = libraryRepository.findById(library.getId());
		assertTrue(found.isPresent());
		found.ifPresent(l -> assertEquals(library, l));

		assertTrue(userRepository.existsById(library.getId()));
		libraryRepository.delete(library);

		assertFalse(userRepository.existsById(library.getId()));
	}

	@Test
	@IgnoreWhen(clusterTypes = ClusterType.MOCKED)
	void saveAndFindByWithNestedId() {
		UserSubmission user = new UserSubmission();
		user.setId(UUID.randomUUID().toString());
		user.setSubmissions(
				Arrays.asList(new Submission(UUID.randomUUID().toString(), user.getId(), "tid", "status", 123)));
		user.setCourses(Arrays.asList(new Course(UUID.randomUUID().toString(), user.getId(), "581")));

		// this currently fails when using mocked in integration.properties with status "UNKNOWN"
		assertFalse(userSubmissionRepository.existsById(user.getId()));

		userSubmissionRepository.save(user);

		Optional<UserSubmission> found = userSubmissionRepository.findById(user.getId());
		assertTrue(found.isPresent());
		found.ifPresent(u -> assertEquals(user, u));

		assertTrue(userSubmissionRepository.existsById(user.getId()));
		assertEquals(user.getSubmissions().get(0).getId(), found.get().getSubmissions().get(0).getId());
		assertEquals(user.getCourses().get(0).getId(), found.get().getCourses().get(0).getId());
		assertEquals(user, found.get());
		userSubmissionRepository.delete(user);
	}

	@Configuration
	@EnableCouchbaseRepositories("org.springframework.data.couchbase")
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
