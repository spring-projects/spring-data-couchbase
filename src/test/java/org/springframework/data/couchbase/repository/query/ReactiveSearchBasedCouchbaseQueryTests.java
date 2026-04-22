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
package org.springframework.data.couchbase.repository.query;

import static org.junit.jupiter.api.Assertions.assertEquals;

import java.lang.reflect.Method;
import java.lang.reflect.Proxy;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicReference;

import org.junit.jupiter.api.Test;
import org.reactivestreams.Publisher;
import org.springframework.dao.InvalidDataAccessApiUsageException;
import org.springframework.data.couchbase.core.ReactiveCouchbaseOperations;
import org.springframework.data.couchbase.core.ReactiveFindBySearchOperation;
import org.springframework.data.couchbase.core.SearchResult;
import org.springframework.data.couchbase.core.mapping.CouchbaseMappingContext;
import org.springframework.data.couchbase.core.mapping.CouchbasePersistentEntity;
import org.springframework.data.couchbase.core.mapping.CouchbasePersistentProperty;
import org.springframework.data.core.TypedPropertyPath;
import org.springframework.data.couchbase.domain.User;
import org.springframework.data.couchbase.repository.Search;
import org.springframework.data.couchbase.repository.SearchIndex;
import org.springframework.data.domain.Page;
import org.springframework.data.domain.PageRequest;
import org.springframework.data.domain.Pageable;
import org.springframework.data.domain.Slice;
import org.springframework.data.domain.Sort;
import org.springframework.data.mapping.context.MappingContext;
import org.springframework.data.projection.SpelAwareProxyProjectionFactory;
import org.springframework.data.repository.reactive.ReactiveCrudRepository;
import org.springframework.data.repository.core.support.DefaultRepositoryMetadata;

import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.test.StepVerifier;

/**
 * Unit tests for {@link ReactiveSearchBasedCouchbaseQuery}.
 *
 * @author Emilien Bevierre
 * @since 6.2
 */
class ReactiveSearchBasedCouchbaseQueryTests {

	private final MappingContext<? extends CouchbasePersistentEntity<?>, CouchbasePersistentProperty> mappingContext;

	ReactiveSearchBasedCouchbaseQueryTests() {
		CouchbaseMappingContext ctx = new CouchbaseMappingContext();
		ctx.setInitialEntitySet(java.util.Collections.singleton(User.class));
		ctx.afterPropertiesSet();
		this.mappingContext = ctx;
	}

	@Test
	void executeProjectsReactiveResults() {
		AtomicReference<String> capturedQuery = new AtomicReference<>();
		ReactiveSearchBasedCouchbaseQuery query = new ReactiveSearchBasedCouchbaseQuery(
				createQueryMethod("searchProjected", "match"),
				createOperations(List.of(user("1", "alpha")), 1, capturedQuery));

		Publisher<?> result = (Publisher<?>) query.execute(new Object[] { "match" });

		StepVerifier.create(result)
				.assertNext(value -> assertEquals("alpha", ((NameOnly) value).getFirstname()))
				.verifyComplete();
		assertEquals("\"match\"", capturedQuery.get());
	}

	@Test
	void executeResolvesReactiveWrapperParameters() {
		AtomicReference<String> capturedQuery = new AtomicReference<>();
		ReactiveSearchBasedCouchbaseQuery query = new ReactiveSearchBasedCouchbaseQuery(
				createQueryMethod("searchReactiveParam", Mono.just("match")),
				createOperations(List.of(user("1", "alpha")), 1, capturedQuery));

		Publisher<?> result = (Publisher<?>) query.execute(new Object[] { Mono.just("match") });

		StepVerifier.create(result)
				.assertNext(value -> assertEquals("alpha", ((NameOnly) value).getFirstname()))
				.verifyComplete();
		assertEquals("\"match\"", capturedQuery.get());
	}

	@Test
	void rejectsPageReturnTypeAtConstruction() {
		Throwable thrown = org.junit.jupiter.api.Assertions.assertThrows(RuntimeException.class,
				() -> createQueryMethod("searchPaged", "match", PageRequest.of(0, 10)));
		org.junit.jupiter.api.Assertions.assertInstanceOf(InvalidDataAccessApiUsageException.class, thrown.getCause());
	}

	@Test
	void rejectsSliceReturnTypeAtConstruction() {
		Throwable thrown = org.junit.jupiter.api.Assertions.assertThrows(RuntimeException.class,
				() -> createQueryMethod("searchSliced", "match", PageRequest.of(0, 10)));
		org.junit.jupiter.api.Assertions.assertInstanceOf(InvalidDataAccessApiUsageException.class, thrown.getCause());
	}

	@Test
	void executeRejectsSpringSortParameters() {
		ReactiveSearchBasedCouchbaseQuery query = new ReactiveSearchBasedCouchbaseQuery(
				createQueryMethod("searchSorted", "match", Sort.by("firstname")),
				createOperations(List.of(user("1", "alpha")), 1, new AtomicReference<>()));

		StepVerifier.create((Publisher<?>) query.execute(new Object[] { "match", Sort.by("firstname") }))
				.expectError(InvalidDataAccessApiUsageException.class)
				.verify();
	}

	interface TestReactiveSearchRepository extends ReactiveCrudRepository<User, String> {

		@Search("?0")
		@SearchIndex("test-index")
		Flux<NameOnly> searchProjected(String query);

		@Search("?0")
		@SearchIndex("test-index")
		Flux<NameOnly> searchReactiveParam(Mono<String> query);

		@Search("?0")
		@SearchIndex("test-index")
		Flux<User> searchSorted(String query, Sort sort);

		@Search("?0")
		@SearchIndex("test-index")
		Mono<Page<User>> searchPaged(String query, Pageable pageable);

		@Search("?0")
		@SearchIndex("test-index")
		Mono<Slice<User>> searchSliced(String query, Pageable pageable);
	}

	interface NameOnly {
		String getFirstname();
	}

	private ReactiveCouchbaseQueryMethod createQueryMethod(String methodName, Object... args) {
		try {
			Method method = findMethod(methodName, args);
			DefaultRepositoryMetadata metadata = new DefaultRepositoryMetadata(TestReactiveSearchRepository.class);
			return new ReactiveCouchbaseQueryMethod(method, metadata, new SpelAwareProxyProjectionFactory(),
					mappingContext);
		} catch (Exception e) {
			throw new RuntimeException(e);
		}
	}

	private Method findMethod(String name, Object... args) {
		for (Method m : TestReactiveSearchRepository.class.getMethods()) {
			if (m.getName().equals(name) && m.getParameterCount() == args.length) {
				return m;
			}
		}
		throw new IllegalArgumentException("Method " + name + " with " + args.length + " params not found");
	}

	private ReactiveCouchbaseOperations createOperations(List<User> results, long totalCount,
			AtomicReference<String> capturedQuery) {
		return (ReactiveCouchbaseOperations) Proxy.newProxyInstance(ReactiveCouchbaseOperations.class.getClassLoader(),
				new Class<?>[] { ReactiveCouchbaseOperations.class }, (proxy, method, args) -> {
					if ("findBySearch".equals(method.getName())) {
						return new StubReactiveFindBySearch<>(results, totalCount, capturedQuery, null, null);
					}
					throw new UnsupportedOperationException(
							"Unexpected ReactiveCouchbaseOperations method: " + method.getName());
				});
	}

	private User user(String id, String firstname) {
		return new User(id, firstname, "lastname");
	}

	private static final class StubReactiveFindBySearch<T>
			implements ReactiveFindBySearchOperation.ReactiveFindBySearch<T> {

		private final List<?> results;
		private final long totalCount;
		private final AtomicReference<String> capturedQuery;
		private final Integer limit;
		private final Integer skip;

		private StubReactiveFindBySearch(List<?> results, long totalCount, AtomicReference<String> capturedQuery,
				Integer limit, Integer skip) {
			this.results = results;
			this.totalCount = totalCount;
			this.capturedQuery = capturedQuery;
			this.limit = limit;
			this.skip = skip;
		}

		@Override
		public Mono<T> one() {
			return Flux.fromIterable(values()).next();
		}

		@Override
		public Mono<T> first() {
			return one();
		}

		@Override
		public Flux<T> all() {
			return Flux.fromIterable(values());
		}

		@Override
		public Mono<Long> count() {
			return Mono.just(totalCount);
		}

		@Override
		public Mono<Boolean> exists() {
			return Mono.just(totalCount > 0);
		}

		@Override
		public Flux<com.couchbase.client.java.search.result.SearchRow> rows() {
			return Flux.empty();
		}

		@Override
		public Mono<SearchResult<T>> result() {
			return Mono.just(new SearchResult<>(values(), Collections.emptyList(), null, Collections.emptyMap()));
		}

		@Override
		public ReactiveFindBySearchOperation.TerminatingFindBySearch<T> matching(
				com.couchbase.client.java.search.SearchRequest searchRequest) {
			capturedQuery.set(searchRequest.toCore().toJson().get("query").get("query").asText());
			return this;
		}

		@Override
		public ReactiveFindBySearchOperation.FindBySearchWithProjection<T> withIndex(String indexName) {
			return this;
		}

		@Override
		public <R> ReactiveFindBySearchOperation.FindBySearchWithFields<R> as(Class<R> returnType) {
			return new StubReactiveFindBySearch<>(results, totalCount, capturedQuery, limit, skip);
		}

		@Override
		public ReactiveFindBySearchOperation.FindBySearchInScope<T> withConsistency(
				com.couchbase.client.java.search.SearchScanConsistency scanConsistency) {
			return this;
		}

		@Override
		public ReactiveFindBySearchOperation.FindBySearchInCollection<T> inScope(String scope) {
			return this;
		}

		@Override
		public ReactiveFindBySearchOperation.FindBySearchWithOptions<T> inCollection(String collection) {
			return this;
		}

		@Override
		public ReactiveFindBySearchOperation.FindBySearchWithQuery<T> withOptions(
				com.couchbase.client.java.search.SearchOptions options) {
			return this;
		}

		@Override
		public ReactiveFindBySearchOperation.FindBySearchWithConsistency<T> withLimit(int limit) {
			return new StubReactiveFindBySearch<>(results, totalCount, capturedQuery, limit, skip);
		}

		@Override
		public ReactiveFindBySearchOperation.FindBySearchWithLimit<T> withSkip(int skip) {
			return new StubReactiveFindBySearch<>(results, totalCount, capturedQuery, limit, skip);
		}

		@Override
		public ReactiveFindBySearchOperation.FindBySearchWithSkip<T> withSort(
				com.couchbase.client.java.search.sort.SearchSort... sort) {
			return this;
		}

		@Override
		public <P> ReactiveFindBySearchOperation.FindBySearchWithSkip<T> withSort(TypedPropertyPath<P, ?> property,
				TypedPropertyPath<P, ?>... additionalProperties) {
			return this;
		}

		@Override
		public ReactiveFindBySearchOperation.FindBySearchWithSort<T> withHighlight(
				com.couchbase.client.java.search.HighlightStyle style, String... fields) {
			return this;
		}

		@Override
		public <P> ReactiveFindBySearchOperation.FindBySearchWithSort<T> withHighlight(
				com.couchbase.client.java.search.HighlightStyle style, TypedPropertyPath<P, ?> field,
				TypedPropertyPath<P, ?>... additionalFields) {
			return this;
		}

		@Override
		public ReactiveFindBySearchOperation.FindBySearchWithHighlight<T> withFacets(
				Map<String, com.couchbase.client.java.search.facet.SearchFacet> facets) {
			return this;
		}

		@Override
		public ReactiveFindBySearchOperation.FindBySearchWithFacets<T> withFields(String... fields) {
			return this;
		}

		@Override
		public <P> ReactiveFindBySearchOperation.FindBySearchWithFacets<T> withFields(
				TypedPropertyPath<P, ?> field, TypedPropertyPath<P, ?>... additionalFields) {
			return this;
		}

		@SuppressWarnings("unchecked")
		private List<T> values() {
			int fromIndex = Math.min(skip != null ? skip : 0, results.size());
			int toIndex = results.size();
			if (limit != null) {
				toIndex = Math.min(fromIndex + limit, results.size());
			}
			return (List<T>) results.subList(fromIndex, toIndex);
		}
	}
}
