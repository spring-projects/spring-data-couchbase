/*
 * Copyright 2017-2019 the original author or authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      https://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.springframework.data.couchbase.repository;

import static java.util.Arrays.asList;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import reactor.test.StepVerifier;

import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.stream.Collectors;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Configuration;
import org.springframework.dao.DataIntegrityViolationException;
import org.springframework.dao.DataRetrievalFailureException;
import org.springframework.data.couchbase.CouchbaseClientFactory;
import org.springframework.data.couchbase.config.AbstractCouchbaseConfiguration;
import org.springframework.data.couchbase.domain.Airport;
import org.springframework.data.couchbase.domain.ReactiveAirportRepository;
import org.springframework.data.couchbase.domain.ReactiveUserRepository;
import org.springframework.data.couchbase.domain.User;
import org.springframework.data.couchbase.repository.config.EnableReactiveCouchbaseRepositories;
import org.springframework.data.couchbase.util.Capabilities;
import org.springframework.data.couchbase.util.ClusterAwareIntegrationTests;
import org.springframework.data.couchbase.util.ClusterType;
import org.springframework.data.couchbase.util.IgnoreWhen;
import org.springframework.data.domain.PageRequest;
import org.springframework.test.context.junit.jupiter.SpringJUnitConfig;

import com.couchbase.client.core.error.IndexExistsException;

/**
 * template class for Reactive Couchbase operations
 *
 * @author Michael Nitschinger
 * @author Michael Reiche
 */
@SpringJUnitConfig(ReactiveCouchbaseRepositoryQueryIntegrationTests.Config.class)
@IgnoreWhen(missesCapabilities = Capabilities.QUERY, clusterTypes = ClusterType.MOCKED)
public class ReactiveCouchbaseRepositoryQueryIntegrationTests extends ClusterAwareIntegrationTests {

	@Autowired CouchbaseClientFactory clientFactory;

	@Autowired ReactiveAirportRepository airportRepository; // intellij flags "Could not Autowire", but it runs ok.
	@Autowired ReactiveUserRepository userRepository; // intellij flags "Could not Autowire", but it runs ok.

	@BeforeEach
	void beforeEach() {
		try {
			clientFactory.getCluster().queryIndexes().createPrimaryIndex(bucketName());
		} catch (IndexExistsException ex) {
			// ignore, all good.
		}
	}

	@Test
	void shouldSaveAndFindAll() {
		Airport vie = null;
		Airport jfk = null;
		try {
			vie = new Airport("airports::vie", "vie", "loww");
			airportRepository.save(vie).block();
			jfk = new Airport("airports::jfk", "JFK", "xxxx");
			airportRepository.save(jfk).block();

			List<Airport> all = airportRepository.findAll().toStream().collect(Collectors.toList());

			assertFalse(all.isEmpty());
			assertTrue(all.stream().anyMatch(a -> a.getId().equals("airports::vie")));
			assertTrue(all.stream().anyMatch(a -> a.getId().equals("airports::jfk")));
		} finally {
			airportRepository.delete(vie).block();
			airportRepository.delete(jfk).block();
		}
	}

	@Test
	void findBySimpleProperty() {
		Airport vie = null;
		try {
			vie = new Airport("airports::vie", "vie", "loww");
			airportRepository.save(vie).block();
			List<Airport> airports1 = airportRepository.findAllByIata("vie").collectList().block();
			assertEquals(1, airports1.size());
			List<Airport> airports2 = airportRepository.findAllByIata("vie").collectList().block();
			assertEquals(1, airports2.size());
			vie = airportRepository.save(vie).block();
			List<Airport> airports = airportRepository.findAllByIata("vie").collectList().block();
			assertEquals(1, airports.size());
			Airport airport1 = airportRepository.findById(airports.get(0).getId()).block();
			assertEquals(airport1.getIata(), vie.getIata());
			Airport airport2 = airportRepository.findByIata(airports.get(0).getIata()).block();
			assertEquals(airport1.getId(), vie.getId());
		} finally {
			airportRepository.delete(vie).block();
		}
	}

	@Test
	public void testCas() {
		User user = new User("1", "Dave", "Wilson");
		userRepository.save(user).block();
		user.setVersion(user.getVersion() - 1);
		assertThrows(DataIntegrityViolationException.class, () -> userRepository.save(user).block());
		user.setVersion(0);
		userRepository.save(user).block();
		userRepository.delete(user).block();
	}

	@Test
	void count() {
		Set<String> iatas = new HashSet();
		iatas.add("JFK");
		iatas.add("IAD");
		iatas.add("SFO");
		iatas.add("SJC");
		iatas.add("SEA");
		iatas.add("LAX");
		iatas.add("PHX");
		Future[] future = new Future[iatas.size()];
		ExecutorService executorService = Executors.newFixedThreadPool(iatas.size());
		try {
			Callable<Boolean>[] suppliers = new Callable[iatas.size()];
			for (String iata : iatas) {
				Airport airport = new Airport("airports::" + iata, iata, iata.toLowerCase() /* lcao */);
				airportRepository.save(airport).block();
			}

			int page = 0;

			airportRepository.findAllByIataLike("S%", PageRequest.of(page++, 2)).as(StepVerifier::create) //
					.expectNextMatches(a -> {
						return iatas.contains(a.getIata());
					}).expectNextMatches(a -> iatas.contains(a.getIata())).verifyComplete();

			airportRepository.findAllByIataLike("S%", PageRequest.of(page++, 2)).as(StepVerifier::create) //
					.expectNextMatches(a -> iatas.contains(a.getIata())).verifyComplete();

			Long airportCount = airportRepository.count().block();
			assertEquals(iatas.size(), airportCount);

			airportCount = airportRepository.countByIataIn("JFK", "IAD", "SFO").block();
			assertEquals(3, airportCount);

			airportCount = airportRepository.countByIcaoAndIataIn("jfk", "JFK", "IAD", "SFO", "XXX").block();
			assertEquals(1, airportCount);

			airportCount = airportRepository.countByIataIn("XXX").block();
			assertEquals(0, airportCount);

		} finally {
			for (String iata : iatas) {
				Airport airport = new Airport("airports::" + iata, iata, iata.toLowerCase() /* lcao */);
				try {
					airportRepository.delete(airport).block();
				} catch (DataRetrievalFailureException drfe) {
					System.out.println("Failed to delete: " + airport);
				}
			}
		}
	}

	@Test
	// DATACOUCH-650
	void deleteAllById() {

		Airport vienna = new Airport("airports::vie", "vie", "LOWW");
		Airport frankfurt = new Airport("airports::fra", "fra", "EDDF");
		Airport losAngeles = new Airport("airports::lax", "lax", "KLAX");

		try {
			airportRepository.saveAll(asList(vienna, frankfurt, losAngeles)).as(StepVerifier::create)
					.expectNext(vienna, frankfurt, losAngeles).verifyComplete();

			airportRepository.deleteAllById(asList(vienna.getId(), losAngeles.getId())).as(StepVerifier::create)
					.verifyComplete();

			airportRepository.findAll().as(StepVerifier::create).expectNext(frankfurt).verifyComplete();
		} finally {
			airportRepository.deleteAll().block();
		}
	}

	@Configuration
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
