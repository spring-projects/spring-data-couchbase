/*
 * Copyright 2012-2020 the original author or authors
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

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.Optional;
import java.util.UUID;

import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.data.auditing.DateTimeProvider;
import org.springframework.data.couchbase.config.AbstractCouchbaseConfiguration;
import org.springframework.data.couchbase.domain.Airport;
import org.springframework.data.couchbase.domain.ReactiveAirportRepository;
import org.springframework.data.couchbase.domain.ReactiveNaiveAuditorAware;
import org.springframework.data.couchbase.domain.ReactiveUserRepository;
import org.springframework.data.couchbase.domain.User;
import org.springframework.data.couchbase.domain.time.AuditingDateTimeProvider;
import org.springframework.data.couchbase.repository.auditing.EnableReactiveCouchbaseAuditing;
import org.springframework.data.couchbase.repository.config.EnableReactiveCouchbaseRepositories;
import org.springframework.data.couchbase.util.ClusterAwareIntegrationTests;
import org.springframework.data.couchbase.util.ClusterType;
import org.springframework.data.couchbase.util.IgnoreWhen;
import org.springframework.test.context.junit.jupiter.SpringJUnitConfig;

@SpringJUnitConfig(ReactiveCouchbaseRepositoryKeyValueIntegrationTests.Config.class)
@IgnoreWhen(clusterTypes = ClusterType.MOCKED)
public class ReactiveCouchbaseRepositoryKeyValueIntegrationTests extends ClusterAwareIntegrationTests {

	@Autowired ReactiveUserRepository userRepository;

	@Autowired ReactiveAirportRepository airportRepository;

	@Test
	void saveAndFindById() {
		User user = new User(UUID.randomUUID().toString(), "f", "l");

		assertFalse(userRepository.existsById(user.getId()).block());

		final User save = userRepository.save(user).block();

		Optional<User> found = userRepository.findById(user.getId()).blockOptional();
		assertTrue(found.isPresent());
		found.ifPresent(u -> assertEquals(save, u));

		assertTrue(userRepository.existsById(user.getId()).block());
	}

	@Test
	void findByIdAudited() {
		Airport vie = null;
		try {
			vie = new Airport("airports::vie", "vie", "low2");
			Airport saved = airportRepository.save(vie).block();
			Airport airport1 = airportRepository.findById(saved.getId()).block();
			assertEquals(airport1, saved);
			assertEquals(saved.getCreatedBy(), ReactiveNaiveAuditorAware.AUDITOR); // ReactiveNaiveAuditorAware will provide
																																							// this
		} finally {
			airportRepository.delete(vie).block();
		}
	}

	@Configuration
	@EnableReactiveCouchbaseRepositories("org.springframework.data.couchbase")
	@EnableReactiveCouchbaseAuditing(dateTimeProviderRef = "dateTimeProviderRef")
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
		public ReactiveNaiveAuditorAware testAuditorAware() {
			return new ReactiveNaiveAuditorAware();
		}

		@Bean(name = "dateTimeProviderRef")
		public DateTimeProvider testDateTimeProvider() {
			return new AuditingDateTimeProvider();
		}

	}

}
