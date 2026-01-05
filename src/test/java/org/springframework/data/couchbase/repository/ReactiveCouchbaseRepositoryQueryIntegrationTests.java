/*
 * Copyright 2017-present the original author or authors.
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

import org.springframework.data.couchbase.domain.Config;
import org.springframework.test.annotation.DirtiesContext;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.test.StepVerifier;

import java.time.Instant;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.stream.Collectors;

import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.dao.DataRetrievalFailureException;
import org.springframework.dao.OptimisticLockingFailureException;
import org.springframework.data.couchbase.CouchbaseClientFactory;
import org.springframework.data.couchbase.domain.Airport;
import org.springframework.data.couchbase.domain.ReactiveAirportRepository;
import org.springframework.data.couchbase.domain.ReactiveUserRepository;
import org.springframework.data.couchbase.domain.User;
import org.springframework.data.couchbase.util.Capabilities;
import org.springframework.data.couchbase.util.ClusterType;
import org.springframework.data.couchbase.util.IgnoreWhen;
import org.springframework.data.couchbase.util.JavaIntegrationTests;
import org.springframework.data.domain.PageRequest;
import org.springframework.test.context.junit.jupiter.SpringJUnitConfig;


/**
 * template class for Reactive Couchbase operations
 *
 * @author Michael Nitschinger
 * @author Michael Reiche
 */
@SpringJUnitConfig(Config.class)
@DirtiesContext
@IgnoreWhen(missesCapabilities = Capabilities.QUERY, clusterTypes = ClusterType.MOCKED)
public class ReactiveCouchbaseRepositoryQueryIntegrationTests extends JavaIntegrationTests {

	@Autowired CouchbaseClientFactory clientFactory;

	@Autowired ReactiveAirportRepository reactiveAirportRepository; // intellij flags "Could not Autowire", runs ok.
	@Autowired ReactiveUserRepository userRepository; // intellij flags "Could not Autowire", but it runs ok.

	@Test
	void shouldSaveAndFindAll() {
		Airport vie = null;
		Airport jfk = null;
		try {
			vie = new Airport("airports::vie", "vie", "low1");
			reactiveAirportRepository.save(vie).block();
			jfk = new Airport("airports::jfk", "JFK", "xxxx");
			reactiveAirportRepository.save(jfk).block();

			List<Airport> all = reactiveAirportRepository.findAll().toStream().collect(Collectors.toList());

			assertFalse(all.isEmpty());
			assertTrue(all.stream().anyMatch(a -> a.getId().equals("airports::vie")));
			assertTrue(all.stream().anyMatch(a -> a.getId().equals("airports::jfk")));
		} finally {
			reactiveAirportRepository.delete(vie).block();
			reactiveAirportRepository.delete(jfk).block();
		}
	}

	@Test
	void testPrimitiveArgs() {
		int iint = 0;
		long llong = 0;
		double ddouble = 0.0;
		boolean bboolean = true;
		List<Airport> all = reactiveAirportRepository.withScope("_default")
				.findAllTestPrimitives(iint, llong, ddouble, bboolean).toStream().collect(Collectors.toList());
	}

	@Test
	void testQuery() {
		Airport vie = null;
		Airport jfk = null;
		try {
			vie = new Airport("airports::vie", "vie", "low1");
			reactiveAirportRepository.save(vie).block();
			jfk = new Airport("airports::jfk", "JFK", "xxxx");
			reactiveAirportRepository.save(jfk).block();

			List<String> all = reactiveAirportRepository.findIdByDynamicN1ql("", "").toStream().collect(Collectors.toList());
			System.out.println(all);
			assertFalse(all.isEmpty());
			assertTrue(all.stream().anyMatch(a -> a.equals("airports::vie")));
			assertTrue(all.stream().anyMatch(a -> a.equals("airports::jfk")));

		} finally {
			reactiveAirportRepository.delete(vie).block();
			reactiveAirportRepository.delete(jfk).block();
		}
	}

	@Test
	void findBySimpleProperty() {
		Airport vie = null;
		try {
			vie = new Airport("airports::vie", "vie", "low2");
			reactiveAirportRepository.save(vie).block();
			List<Airport> airports1 = reactiveAirportRepository.findAllByIata(Mono.just("vie")).collectList().block();
			assertEquals(1, airports1.size());
			List<Airport> airports2 = reactiveAirportRepository.findAllByIata(Mono.just("vie")).collectList().block();
			assertEquals(1, airports2.size());
			vie = reactiveAirportRepository.save(vie).block();
			List<Airport> airports = reactiveAirportRepository.findAllByIata(Mono.just("vie")).collectList().block();
			assertEquals(1, airports.size());
			Airport airport1 = reactiveAirportRepository.findById(airports.get(0).getId()).block();
			assertEquals(airport1.getIata(), vie.getIata());
			Airport airport2 = reactiveAirportRepository.findByIata(airports.get(0).getIata()).block();
			assertEquals(airport1.getId(), vie.getId());
		} finally {
			reactiveAirportRepository.delete(vie).block();
		}
	}

	@Test
	public void testCas() {
		User user = new User("1", "Dave", "Wilson");
		userRepository.save(user).block();
		long saveVersion = user.getVersion();
		user.setVersion(user.getVersion() - 1);
		assertThrows(OptimisticLockingFailureException.class, () -> userRepository.save(user).block());
		user.setVersion(saveVersion);
		userRepository.save(user).block();
		userRepository.delete(user).block();
	}

	@Test
	void limitTest() {
		Airport vie = new Airport("airports::vie", "vie", "low3");
		Airport saved1 = reactiveAirportRepository.save(vie).block();
		Airport saved2 = reactiveAirportRepository.save(vie.withId(UUID.randomUUID().toString())).block();
		try {
			reactiveAirportRepository.findAll().collectList().block(); // findAll has QueryScanConsistency;
			Mono<Airport> airport = reactiveAirportRepository.findPolicySnapshotByPolicyIdAndEffectiveDateTime("any", 0);
			System.out.println("------------------------------");
			System.out.println(airport.block());
			System.out.println("------------------------------");
			Flux<Airport> airports = reactiveAirportRepository.findPolicySnapshotAll();
			System.out.println(airports.collectList().block());
			System.out.println("------------------------------");
			Mono<Airport> ap = getPolicyByIdAndEffectiveDateTime("x", Instant.now());
			System.out.println(ap.block());
		} finally {
			reactiveAirportRepository.delete(saved1).block();
			reactiveAirportRepository.delete(saved2).block();
		}
	}

	public Mono<Airport> getPolicyByIdAndEffectiveDateTime(String policyId, Instant effectiveDateTime) {
		return reactiveAirportRepository
				.findPolicySnapshotByPolicyIdAndEffectiveDateTime(policyId, effectiveDateTime.toEpochMilli())
				// .map(Airport::getEntity)
				.doOnError(
						error -> System.out.println("MSG='Exception happened while retrieving Policy by Id and effectiveDateTime', "
								+ "policyId={}, effectiveDateTime={}"));
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
				reactiveAirportRepository.save(airport).block();
			}

			int page = 0;

			reactiveAirportRepository.findAllByIataLike("S%", PageRequest.of(page++, 2)).as(StepVerifier::create) //
					.expectNextMatches(a -> {
						return iatas.contains(a.getIata());
					}).expectNextMatches(a -> iatas.contains(a.getIata())).verifyComplete();

			reactiveAirportRepository.findAllByIataLike("S%", PageRequest.of(page++, 2)).as(StepVerifier::create) //
					.expectNextMatches(a -> iatas.contains(a.getIata())).verifyComplete();

			Long airportCount = reactiveAirportRepository.count().block();
			assertEquals(iatas.size(), airportCount);

			airportCount = reactiveAirportRepository.countByIataIn("JFK", "IAD", "SFO").block();
			assertEquals(3, airportCount);

			airportCount = reactiveAirportRepository.countByIcaoAndIataIn("jfk", "JFK", "IAD", "SFO", "XXX").block();
			assertEquals(1, airportCount);

			airportCount = reactiveAirportRepository.countByIataIn("XXX").block();
			assertEquals(0, airportCount);

		} finally {
			for (String iata : iatas) {
				Airport airport = new Airport("airports::" + iata, iata, iata.toLowerCase() /* lcao */);
				try {
					reactiveAirportRepository.delete(airport).block();
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
		Airport frankfurt = new Airport("airports::fra", "fra", "EDDX");
		Airport losAngeles = new Airport("airports::lax", "lax", "KLAX");

		try {
			// This failed once against Capella - not sure why.
			reactiveAirportRepository.saveAll(asList(vienna, frankfurt, losAngeles)).blockLast();

			reactiveAirportRepository.deleteAllById(asList(vienna.getId(), losAngeles.getId())).as(StepVerifier::create)
					.verifyComplete();

			reactiveAirportRepository.findAll().as(StepVerifier::create).expectNext(frankfurt).verifyComplete();

		} finally {
			List<Airport> airports = reactiveAirportRepository.findAll().collectList().block(); // .as(StepVerifier::create).expectNext(frankfurt).verifyComplete();
			System.out.println(airports);
			reactiveAirportRepository.deleteAll().block();
		}
	}

	@Test
	void deleteAll() {

		Airport vienna = new Airport("airports::vie", "vie", "LOWW");
		Airport frankfurt = new Airport("airports::fra", "fra", "EDDY");
		Airport losAngeles = new Airport("airports::lax", "lax", "KLAX");

		try {
			reactiveAirportRepository.saveAll(asList(vienna, frankfurt, losAngeles)).blockLast();

			reactiveAirportRepository.deleteAll().as(StepVerifier::create).verifyComplete();

			reactiveAirportRepository.findAll().as(StepVerifier::create).verifyComplete();

		} finally {
			reactiveAirportRepository.deleteAll().block();
		}
	}

	@Test
	void deleteOne() {

		Airport vienna = new Airport("airports::vie", "vie", "LOWW");

		try {
			Airport ap = reactiveAirportRepository.save(vienna).block();
			assertEquals(vienna.getId(), ap.getId(), "should have saved what was provided");
			reactiveAirportRepository.delete(vienna).as(StepVerifier::create).verifyComplete();

			reactiveAirportRepository.findAll().as(StepVerifier::create).verifyComplete();
		} finally {
			reactiveAirportRepository.deleteAll().block();
		}
	}

}
