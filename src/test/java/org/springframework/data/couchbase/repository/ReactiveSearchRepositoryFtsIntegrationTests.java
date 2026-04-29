/*
 * Copyright 2025-present the original author or authors
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

import java.time.Duration;

import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.springframework.data.annotation.Id;
import org.springframework.data.couchbase.CouchbaseClientFactory;
import org.springframework.data.couchbase.SimpleCouchbaseClientFactory;
import org.springframework.data.couchbase.core.ReactiveCouchbaseTemplate;
import org.springframework.data.couchbase.core.convert.MappingCouchbaseConverter;
import org.springframework.data.couchbase.core.convert.translation.JacksonTranslationService;
import org.springframework.data.couchbase.core.mapping.CouchbaseMappingContext;
import org.springframework.data.couchbase.repository.config.ReactiveRepositoryOperationsMapping;
import org.springframework.data.couchbase.repository.support.ReactiveCouchbaseRepositoryFactory;
import org.springframework.data.couchbase.util.ClusterType;
import org.springframework.data.couchbase.util.IgnoreWhen;
import org.springframework.data.couchbase.util.JavaIntegrationTests;
import org.springframework.data.repository.Repository;

import com.couchbase.client.core.deps.com.fasterxml.jackson.databind.DeserializationFeature;
import com.couchbase.client.java.Cluster;
import com.couchbase.client.java.json.JacksonTransformers;
import com.couchbase.client.java.search.SearchQuery;
import com.couchbase.client.java.search.SearchRequest;

import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.test.StepVerifier;

/**
 * Reactive {@link Search} repository integration tests against {@code travel-sample.inventory.airport}.
 * <p>
 * Coverage for the wrapper-type adaptation in {@link
 * org.springframework.data.couchbase.repository.query.ReactiveSearchBasedCouchbaseQuery}: methods declared
 * to return {@link Mono} (count / exists / one) must surface as a {@code Mono} through the repository proxy,
 * not be coerced into a {@code Flux}. Methods declared to return {@link Flux} keep their multi-value semantics.
 *
 * @author Emilien Bevierre
 * @since 6.2
 */
@IgnoreWhen(clusterTypes = ClusterType.MOCKED)
class ReactiveSearchRepositoryFtsIntegrationTests extends JavaIntegrationTests {

	private static final String INDEX_NAME = "sd-fts-airport-idx";
	private static final String TS_BUCKET = "travel-sample";

	private static CouchbaseClientFactory travelSampleClientFactory;
	private static AirportSearchRepository repository;

	@BeforeAll
	static void setupRepository() {
		callSuperBeforeAll(new Object() {});

		JacksonTransformers.MAPPER.configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false);

		travelSampleClientFactory = new SimpleCouchbaseClientFactory(connectionString(), authenticator(), TS_BUCKET);
		Cluster cluster = travelSampleClientFactory.getCluster();
		cluster.bucket(TS_BUCKET).waitUntilReady(Duration.ofSeconds(30));

		CouchbaseMappingContext mappingContext = new CouchbaseMappingContext();
		mappingContext.setAutoIndexCreation(false);
		mappingContext.afterPropertiesSet();
		MappingCouchbaseConverter converter = new MappingCouchbaseConverter(mappingContext, "t");
		JacksonTranslationService translationService = new JacksonTranslationService();
		translationService.afterPropertiesSet();

		ReactiveCouchbaseTemplate reactiveTemplate = new ReactiveCouchbaseTemplate(travelSampleClientFactory, converter,
				translationService);

		ensureFtsIndex(cluster);

		ReactiveCouchbaseRepositoryFactory factory = new ReactiveCouchbaseRepositoryFactory(
				new ReactiveRepositoryOperationsMapping(reactiveTemplate));
		repository = factory.getRepository(AirportSearchRepository.class);
	}

	@AfterAll
	static void tearDownRepository() {
		if (travelSampleClientFactory != null) {
			try {
				travelSampleClientFactory.close();
			} catch (Exception e) {
				LOGGER.warn("Failed to close travel-sample client factory: {}", e.getMessage());
			}
		}
		callSuperAfterAll(new Object() {});
	}

	@Test
	void monoCountReturnsSingleLong() {
		Mono<Long> result = repository.countByCountry("United States");

		StepVerifier.create(result)
				.assertNext(count -> {
					if (count == null || count <= 0) {
						throw new AssertionError("Expected non-zero count, got " + count);
					}
				})
				.verifyComplete();
	}

	@Test
	void monoExistsReturnsSingleBoolean() {
		Mono<Boolean> hit = repository.existsByFaa("SFO");
		Mono<Boolean> miss = repository.existsByFaa("ZZZZZZZ");

		StepVerifier.create(hit).expectNext(Boolean.TRUE).verifyComplete();
		StepVerifier.create(miss).expectNext(Boolean.FALSE).verifyComplete();
	}

	@Test
	void monoOneReturnsSingleEntity() {
		Mono<TsAirport> result = repository.findOneByFaa("SFO");

		StepVerifier.create(result)
				.assertNext(airport -> {
					if (!"SFO".equals(airport.faa)) {
						throw new AssertionError("Expected SFO, got " + airport.faa);
					}
				})
				.verifyComplete();
	}

	@Test
	void fluxAllReturnsMultipleEntities() {
		Flux<TsAirport> result = repository.findAllByCountry("France");

		StepVerifier.create(result.collectList())
				.assertNext(list -> {
					if (list.isEmpty()) {
						throw new AssertionError("Expected airports in France");
					}
					for (TsAirport airport : list) {
						if (!"France".equals(airport.country)) {
							throw new AssertionError("Expected France, got " + airport.country);
						}
					}
				})
				.verifyComplete();
	}

	// --- Repository under test ---
	// Each ?N is substituted as a quoted FTS term (see SearchRepositoryQuerySupport#renderBindableValue),
	// so the field name must live in the template, not in the bound parameter.

	interface AirportSearchRepository extends Repository<TsAirport, String> {

		@Search("country:?0")
		Flux<TsAirport> findAllByCountry(String country);

		@Search("faa:?0")
		Mono<TsAirport> findOneByFaa(String faa);

		@Search("country:?0")
		Mono<Long> countByCountry(String country);

		@Search("faa:?0")
		Mono<Boolean> existsByFaa(String faa);
	}

	// --- Domain entity for travel-sample airports ---
	// @Scope/@Collection are required so ReactiveFindBySearchOperationSupport.findBySearch(domainType)
	// auto-resolves them via OptionsBuilder.getScopeFrom/getCollectionFrom, which routes the
	// search through the inventory scope where the FTS index actually lives. Without them the SDK
	// hits cluster-level by short name and fails with IndexNotFoundException.

	@Scope("inventory")
	@Collection("airport")
	@SearchIndex(INDEX_NAME)
	static class TsAirport {
		@Id String key;
		String airportname;
		String country;
		String faa;
	}

	// --- Helpers ---

	private static void ensureFtsIndex(Cluster cluster) {
		com.couchbase.client.java.Scope scope = cluster.bucket(TS_BUCKET).scope("inventory");
		try {
			scope.searchIndexes().getIndex(INDEX_NAME);
			LOGGER.info("FTS index '{}' already exists, reusing.", INDEX_NAME);
		} catch (com.couchbase.client.core.error.IndexNotFoundException ex) {
			com.couchbase.client.java.manager.search.SearchIndex index = new com.couchbase.client.java.manager.search.SearchIndex(
					INDEX_NAME, TS_BUCKET);
			scope.searchIndexes().upsertIndex(index);
			LOGGER.info("Created FTS index '{}'", INDEX_NAME);
		}
		waitForFtsIndex(scope);
	}

	private static void waitForFtsIndex(com.couchbase.client.java.Scope scope) {
		for (int i = 0; i < 30; i++) {
			try {
				scope.search(INDEX_NAME, SearchRequest.create(SearchQuery.matchAll()));
				break;
			} catch (Exception ex) {
				if (i == 29)
					throw new RuntimeException("FTS index did not become queryable in time", ex);
				sleepMs(2000);
			}
		}
		for (int i = 0; i < 60; i++) {
			try {
				long docCount = scope.searchIndexes().getIndexedDocumentsCount(INDEX_NAME);
				if (docCount > 0) {
					LOGGER.info("FTS index '{}' ready with {} indexed documents", INDEX_NAME, docCount);
					return;
				}
			} catch (Exception ignored) {}
			sleepMs(2000);
		}
		LOGGER.warn("FTS index '{}' may not have finished indexing", INDEX_NAME);
	}
}
