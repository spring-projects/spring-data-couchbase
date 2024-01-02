/*
 * Copyright 2021-2024 the original author or authors
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
package org.springframework.data.couchbase.domain;

import static org.junit.jupiter.api.Assertions.assertEquals;

import org.springframework.test.annotation.DirtiesContext;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.core.publisher.ParallelFlux;
import reactor.core.scheduler.Schedulers;

import java.util.Arrays;
import java.util.LinkedList;
import java.util.List;
import java.util.Random;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicInteger;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Configuration;
import org.springframework.data.couchbase.config.AbstractCouchbaseConfiguration;
import org.springframework.data.couchbase.core.CouchbaseTemplate;
import org.springframework.data.couchbase.core.ReactiveCouchbaseTemplate;
import org.springframework.data.couchbase.core.RemoveResult;
import org.springframework.data.couchbase.domain.Config;
import org.springframework.data.couchbase.repository.config.EnableReactiveCouchbaseRepositories;
import org.springframework.data.couchbase.util.Capabilities;
import org.springframework.data.couchbase.util.ClusterType;
import org.springframework.data.couchbase.util.IgnoreWhen;
import org.springframework.data.couchbase.util.JavaIntegrationTests;
import org.springframework.data.util.Pair;
import org.springframework.test.context.junit.jupiter.SpringJUnitConfig;

import com.couchbase.client.core.deps.io.netty.handler.ssl.util.InsecureTrustManagerFactory;
import com.couchbase.client.core.env.SecurityConfig;
import com.couchbase.client.java.Collection;
import com.couchbase.client.java.ReactiveCollection;
import com.couchbase.client.java.env.ClusterEnvironment;
import com.couchbase.client.java.json.JsonObject;
import com.couchbase.client.java.kv.GetResult;
import com.couchbase.client.java.query.QueryOptions;
import com.couchbase.client.java.query.QueryProfile;
import com.couchbase.client.java.query.QueryResult;
import com.couchbase.client.java.query.QueryScanConsistency;

/**
 * @author Michael Reiche
 */
@SpringJUnitConfig(Config.class)
@DirtiesContext
@IgnoreWhen(clusterTypes = ClusterType.MOCKED)
public class FluxIntegrationTests extends JavaIntegrationTests {

	@Autowired public CouchbaseTemplate couchbaseTemplate;
	@Autowired public ReactiveCouchbaseTemplate reactiveCouchbaseTemplate;

	@BeforeEach
	@Override
	public void beforeEach() {
        super.beforeEach();
		/**
		 * The couchbaseTemplate inherited from JavaIntegrationTests uses org.springframework.data.couchbase.domain.Config
		 * It has typeName = 't' (instead of _class). Don't use it.
		 */
		collection = couchbaseTemplate.getCouchbaseClientFactory().getBucket().defaultCollection();
		rCollection = couchbaseTemplate.getCouchbaseClientFactory().getBucket().reactive().defaultCollection();
		for (String k : keyList) {
			couchbaseTemplate.getCouchbaseClientFactory().getBucket().defaultCollection().upsert(k,
					JsonObject.create().put("x", k));
		}
	}

	@AfterEach
	public void afterEach() {
		couchbaseTemplate.removeByQuery(Airport.class).withConsistency(QueryScanConsistency.REQUEST_PLUS).all();
		couchbaseTemplate.findByQuery(Airport.class).withConsistency(QueryScanConsistency.REQUEST_PLUS).all();
		super.afterEach();
		for (String k : keyList) {
			couchbaseTemplate.getCouchbaseClientFactory().getBucket().defaultCollection().remove(k);
		}
	}

	static List<String> keyList = Arrays.asList("a", "b", "c", "d", "e");
	static Collection collection;
	static ReactiveCollection rCollection;
	@Autowired ReactiveAirportRepository reactiveAirportRepository; // intellij flags "Could not Autowire", runs ok.

	AtomicInteger rCat = new AtomicInteger(0);
	AtomicInteger rFlat = new AtomicInteger(0);

	@Test
	public void concatMapCB() throws Exception {
		System.out.println("Start concatMapCB");
		System.out.println("\n******** Using concatMap() *********");
		ParallelFlux<GetResult> concat = Flux.fromIterable(keyList).parallel(2).runOn(Schedulers.parallel())
				.concatMap(item -> cbGet(item)
						/* rCollection.get(item) */.doOnSubscribe((x) -> System.out.println(" +" + rCat.incrementAndGet()))
						.doOnTerminate(() -> System.out.println(" -" + rCat.decrementAndGet())));
		System.out.println(concat.sequential().collectList().block());
	}

	@Test
	@IgnoreWhen(missesCapabilities = { Capabilities.QUERY, Capabilities.COLLECTIONS }, clusterTypes = ClusterType.MOCKED)
	public void cbse() {
		LinkedList<LinkedList<Airport>> listOfLists = new LinkedList<>();
		Airport a = new Airport(UUID.randomUUID().toString(), "iata", "lowp");
		String last = null;
		for (int i = 0; i < 5; i++) {
			LinkedList<Airport> list = new LinkedList<>();
			for (int j = 0; j < 10; j++) {
				list.add(a.withId(UUID.randomUUID().toString()));
				last = a.getId();
			}
			listOfLists.add(list);
		}
		Flux<Object> af = Flux.fromIterable(listOfLists).concatMap(catalogToStore -> Flux.fromIterable(catalogToStore)
				.parallel(4).runOn(Schedulers.parallel()).concatMap((entity) -> reactiveAirportRepository.save(entity)));
		List<Object> saved = af.collectList().block();
		System.out.println("results.size() : " + saved.size());

		String statement = "select * from `" + /*config().bucketname()*/ "_default" + "` where META().id >= '" + last + "'";
		System.out.println("statement: " + statement);
		try {
			QueryResult qr = couchbaseTemplate.getCouchbaseClientFactory().getScope().query(statement,
					QueryOptions.queryOptions().profile(QueryProfile.PHASES));
			List<RemoveResult> rr = couchbaseTemplate.removeByQuery(Airport.class)
					.withOptions(QueryOptions.queryOptions().scanConsistency(QueryScanConsistency.REQUEST_PLUS)).all();
			System.out.println(qr.metaData().profile().get());
		} catch (Exception e) {
			e.printStackTrace();
			throw e;
		}
		List<Airport> airports = reactiveAirportRepository.findAll().collectList().block();
		assertEquals(0, airports.size(), "should have been all deleted");
	}

	@Test
	@IgnoreWhen(missesCapabilities = { Capabilities.QUERY }, clusterTypes = ClusterType.MOCKED)
	public void pairIdAndResult() {
		LinkedList<Airport> list = new LinkedList<>();
		Airport a = new Airport(UUID.randomUUID().toString(), "iata", "lowp");
		for (int i = 0; i < 5; i++) {
			list.add(a.withId(UUID.randomUUID().toString()));
		}
		Flux<Object> af = Flux.fromIterable(list).concatMap((entity) -> reactiveAirportRepository.save(entity));
		List<Object> saved = af.collectList().block();
		System.out.println("results.size() : " + saved.size());
		Flux<Pair<String, Mono<Airport>>> pairFlux = Flux.fromIterable(list)
				.map((airport) -> Pair.of(airport.getId(), reactiveAirportRepository.findById(airport.getId())));
		List<Pair<String, Mono<Airport>>> airportPairs = pairFlux.collectList().block();
		for (Pair<String, Mono<Airport>> airportPair : airportPairs) {
			System.out.println("id: " + airportPair.getFirst() + " airport: " + airportPair.getSecond().block());
		}

	}

	@Test
	public void flatMapCB() throws Exception {
		System.out.println("Start flatMapCB");
		ParallelFlux<GetResult> concat = Flux.fromIterable(keyList).parallel(2).runOn(Schedulers.parallel())
				.flatMap(item -> cbGet(item) /* rCollection.get(item) */
						.doOnSubscribe((x) -> System.out.println(" +" + rCat.incrementAndGet()))
						.doOnTerminate(() -> System.out.println(" -" + rCat.decrementAndGet())));
		System.out.println(concat.sequential().collectList().block());
	}

	@Test
	public void flatMapSyncCB() throws Exception {
		System.out.println("Start flatMapSyncCB");
		System.out.println("\n******** Using flatSyncMap() *********");
		ParallelFlux<GetResult> concat = Flux.fromIterable(keyList).parallel(2).runOn(Schedulers.parallel())
				.flatMap(item -> Flux.just(cbGetSync(item) /* collection.get(item) */));
		System.out.println(concat.sequential().collectList().block());
		;
	}

	@Test
	public void flatMapVsConcatMapCB2() throws Exception {
		System.out.println("Start flatMapCB2");
		System.out.println("\n******** Using flatMap() *********");
		ParallelFlux<GetResult> flat = Flux.fromIterable(keyList).parallel(1).runOn(Schedulers.parallel())
				.flatMap(item -> rCollection.get(item).doOnSubscribe((x) -> System.out.println(" +" + rCat.incrementAndGet()))
						.doOnTerminate(() -> System.out.println(" -" + rCat.getAndDecrement())));
		System.out.println(flat.sequential().collectList().block());
		System.out.println("Start concatMapCB");
		System.out.println("\n******** Using concatMap() *********");
		ParallelFlux<GetResult> concat = Flux.fromIterable(keyList).parallel(2).runOn(Schedulers.parallel())
				.concatMap(item -> cbGet(item).doOnSubscribe((x) -> System.out.println(" +" + rCat.incrementAndGet()))
						.doOnTerminate(() -> System.out.println(" -" + rCat.getAndDecrement())));
		System.out.println(concat.sequential().collectList().block());
		;
	}

	static Random r = new Random();

	static void sleep(long sleepMs) {
		try {
			int random = Math.abs(r.nextInt() % 1000);
			Thread.sleep(sleepMs * random);
		} catch (InterruptedException e) {}
	}

	AtomicInteger cbCount = new AtomicInteger();

	Mono<GetResult> cbGet(String id) {
		// System.out.println(" =" + id);
		return rCollection.get(id);
	}

	GetResult cbGetSync(String id) {
		// System.out.println(id + " +" + rCat.incrementAndGet());
		GetResult result = collection.get(id);
		// System.out.println(id + " -" + rCat.getAndDecrement());
		return result;
	}

	static String tab(int len) {
		StringBuilder sb = new StringBuilder(len);
		for (int i = 0; i < len; i++)
			sb.append(" ");
		return sb.toString();
	}

}
