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

import static org.junit.jupiter.api.Assertions.*;

import java.lang.reflect.Proxy;
import java.lang.reflect.Method;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.stream.Stream;

import org.junit.jupiter.api.Test;
import org.springframework.dao.InvalidDataAccessApiUsageException;
import org.springframework.data.couchbase.core.CouchbaseOperations;
import org.springframework.data.couchbase.core.ExecutableFindBySearchOperation;
import org.springframework.data.couchbase.core.SearchResult;
import org.springframework.data.couchbase.core.mapping.CouchbaseMappingContext;
import org.springframework.data.couchbase.core.mapping.CouchbasePersistentEntity;
import org.springframework.data.couchbase.core.mapping.CouchbasePersistentProperty;
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
import org.springframework.data.repository.CrudRepository;
import org.springframework.data.repository.core.support.DefaultRepositoryMetadata;
import org.springframework.data.repository.query.DefaultParameters;
import org.springframework.data.repository.query.ParametersParameterAccessor;
import org.springframework.data.repository.query.ParametersSource;

/**
 * Unit tests for {@link SearchBasedCouchbaseQuery} parameter resolution logic.
 *
 * @author Emilien Bevierre
 * @since 6.2
 */
class SearchBasedCouchbaseQueryTests {

	private final MappingContext<? extends CouchbasePersistentEntity<?>, CouchbasePersistentProperty> mappingContext;

	SearchBasedCouchbaseQueryTests() {
		CouchbaseMappingContext ctx = new CouchbaseMappingContext();
		ctx.setInitialEntitySet(java.util.Collections.singleton(User.class));
		ctx.afterPropertiesSet();
		this.mappingContext = ctx;
	}

	@Test
	void resolveParametersWithNoPlaceholders() {
		ParametersParameterAccessor accessor = createAccessor("findByStaticQuery");
		String resolved = SearchBasedCouchbaseQuery.resolveParameters("description:pool", accessor);
		assertEquals("description:pool", resolved);
	}

	@Test
	void resolveParametersSinglePositional() {
		ParametersParameterAccessor accessor = createAccessor("searchByQuery", "hello world");
		String resolved = SearchBasedCouchbaseQuery.resolveParameters("?0", accessor);
		assertEquals("\"hello world\"", resolved);
	}

	@Test
	void resolveParametersMultiplePositional() {
		ParametersParameterAccessor accessor = createAccessor("searchByCityAndRating", "NYC", 5);
		String resolved = SearchBasedCouchbaseQuery.resolveParameters("city:?0 AND rating:>=?1", accessor);
		assertEquals("city:\"NYC\" AND rating:>=5", resolved);
	}

	@Test
	void resolveParametersWithSpecialCharacters() {
		ParametersParameterAccessor accessor = createAccessor("searchByQuery", "hello \"world\"");
		String resolved = SearchBasedCouchbaseQuery.resolveParameters("?0", accessor);
		assertEquals("\"hello \\\"world\\\"\"", resolved);
	}

	@Test
	void resolveParametersDoesNotModifyStringWithoutPlaceholders() {
		ParametersParameterAccessor accessor = createAccessor("findByStaticQuery");
		String template = "description:\"San Francisco\" AND rating:>3";
		String resolved = SearchBasedCouchbaseQuery.resolveParameters(template, accessor);
		assertEquals(template, resolved);
	}

	@Test
	void hasSearchAnnotationReturnsTrueForAnnotatedMethod() throws Exception {
		CouchbaseQueryMethod method = createQueryMethod("findByStaticQuery");
		assertTrue(method.hasSearchAnnotation());
	}

	@Test
	void hasSearchAnnotationReturnsFalseForUnannotatedMethod() throws Exception {
		CouchbaseQueryMethod method = createQueryMethod("findAll");
		assertFalse(method.hasSearchAnnotation());
	}

	@Test
	void resolveParametersRejectsNullValues() {
		ParametersParameterAccessor accessor = createAccessor("searchNullable", new Object[] { null });
		assertThrows(InvalidDataAccessApiUsageException.class,
				() -> SearchBasedCouchbaseQuery.resolveParameters("?0", accessor));
	}

	@Test
	void resolveParametersRejectsCollectionValues() {
		ParametersParameterAccessor accessor = createAccessor("searchCollection", List.of("a", "b"));
		assertThrows(InvalidDataAccessApiUsageException.class,
				() -> SearchBasedCouchbaseQuery.resolveParameters("?0", accessor));
	}

	@Test
	void executeProjectsPagedResults() {
		SearchBasedCouchbaseQuery query = new SearchBasedCouchbaseQuery(
				createQueryMethod("searchPage", "match", PageRequest.of(1, 1)),
				createOperations(List.of(user("1", "alpha"), user("2", "bravo"), user("3", "charlie")), 3));

		Object result = query.execute(new Object[] { "match", PageRequest.of(1, 1) });

		assertInstanceOf(Page.class, result);
		Page<?> page = (Page<?>) result;
		assertEquals(3, page.getTotalElements());
		assertEquals(1, page.getContent().size());
		assertInstanceOf(NameOnly.class, page.getContent().get(0));
		assertEquals("bravo", ((NameOnly) page.getContent().get(0)).getFirstname());
	}

	@Test
	void executeProjectsSlicedResults() {
		SearchBasedCouchbaseQuery query = new SearchBasedCouchbaseQuery(
				createQueryMethod("searchSlice", "match", PageRequest.of(0, 2)),
				createOperations(List.of(user("1", "alpha"), user("2", "bravo"), user("3", "charlie")), 3));

		Object result = query.execute(new Object[] { "match", PageRequest.of(0, 2) });

		assertInstanceOf(Slice.class, result);
		Slice<?> slice = (Slice<?>) result;
		assertTrue(slice.hasNext());
		assertEquals(2, slice.getContent().size());
		assertInstanceOf(NameOnly.class, slice.getContent().get(0));
	}

	@Test
	void executeSupportsDynamicProjection() {
		SearchBasedCouchbaseQuery query = new SearchBasedCouchbaseQuery(
				createQueryMethod("searchDynamicProjection", "match", NameOnly.class),
				createOperations(List.of(user("1", "alpha")), 1));

		Object result = query.execute(new Object[] { "match", NameOnly.class });

		assertInstanceOf(List.class, result);
		assertEquals("alpha", ((NameOnly) ((List<?>) result).get(0)).getFirstname());
	}

	@Test
	void executeRejectsSpringSortParameters() {
		SearchBasedCouchbaseQuery query = new SearchBasedCouchbaseQuery(
				createQueryMethod("searchSorted", "match", Sort.by("firstname")),
				createOperations(List.of(user("1", "alpha")), 1));

		assertThrows(InvalidDataAccessApiUsageException.class,
				() -> query.execute(new Object[] { "match", Sort.by("firstname") }));
	}

	// --- Test repository interface for reflection ---

	interface TestSearchRepository extends CrudRepository<User, String> {

		@Search("description:pool")
		@SearchIndex("test-index")
		List<User> findByStaticQuery();

		@Search("?0")
		@SearchIndex("test-index")
		List<User> searchByQuery(String query);

		@Search("city:?0 AND rating:>=?1")
		@SearchIndex("test-index")
		List<User> searchByCityAndRating(String city, int rating);

		@Search("?0")
		@SearchIndex("test-index")
		List<User> searchNullable(String query);

		@Search("?0")
		@SearchIndex("test-index")
		List<User> searchCollection(List<String> query);

		@Search("?0")
		@SearchIndex("test-index")
		Page<NameOnly> searchPage(String query, Pageable pageable);

		@Search("?0")
		@SearchIndex("test-index")
		Slice<NameOnly> searchSlice(String query, Pageable pageable);

		@Search("?0")
		@SearchIndex("test-index")
		<T> List<T> searchDynamicProjection(String query, Class<T> type);

		@Search("?0")
		@SearchIndex("test-index")
		List<User> searchSorted(String query, Sort sort);

		List<User> findAll();
	}

	interface NameOnly {
		String getFirstname();
	}

	// --- Helpers ---

	private ParametersParameterAccessor createAccessor(String methodName, Object... args) {
		try {
			Method method = findMethod(methodName, args);
			DefaultParameters params = new DefaultParameters(ParametersSource.of(method));
			return new ParametersParameterAccessor(params, args);
		} catch (Exception e) {
			throw new RuntimeException(e);
		}
	}

	private CouchbaseQueryMethod createQueryMethod(String methodName, Object... args) {
		try {
			Method method = findMethod(methodName, args);
			DefaultRepositoryMetadata metadata = new DefaultRepositoryMetadata(TestSearchRepository.class);
			return new CouchbaseQueryMethod(method, metadata, new SpelAwareProxyProjectionFactory(), mappingContext);
		} catch (Exception e) {
			throw new RuntimeException(e);
		}
	}

	private Method findMethod(String name, Object... args) {
		for (Method m : TestSearchRepository.class.getMethods()) {
			if (m.getName().equals(name) && m.getParameterCount() == args.length) {
				return m;
			}
		}
		throw new IllegalArgumentException("Method " + name + " with " + args.length + " params not found");
	}

	private CouchbaseOperations createOperations(List<User> results, long totalCount) {
		return (CouchbaseOperations) Proxy.newProxyInstance(CouchbaseOperations.class.getClassLoader(),
				new Class<?>[] { CouchbaseOperations.class }, (proxy, method, args) -> {
					if ("findBySearch".equals(method.getName())) {
						return new StubExecutableFindBySearch<>(results, totalCount, null, null);
					}
					throw new UnsupportedOperationException("Unexpected CouchbaseOperations method: " + method.getName());
				});
	}

	private User user(String id, String firstname) {
		return new User(id, firstname, "lastname");
	}

	private static final class StubExecutableFindBySearch<T>
			implements ExecutableFindBySearchOperation.ExecutableFindBySearch<T> {

		private final List<?> results;
		private final long totalCount;
		private final Integer limit;
		private final Integer skip;

		private StubExecutableFindBySearch(List<?> results, long totalCount, Integer limit, Integer skip) {
			this.results = results;
			this.totalCount = totalCount;
			this.limit = limit;
			this.skip = skip;
		}

		@Override
		public T oneValue() {
			List<T> values = values();
			return values.isEmpty() ? null : values.get(0);
		}

		@Override
		public T firstValue() {
			return oneValue();
		}

		@Override
		public List<T> all() {
			return values();
		}

		@Override
		public Stream<T> stream() {
			return values().stream();
		}

		@Override
		public long count() {
			return totalCount;
		}

		@Override
		public boolean exists() {
			return totalCount > 0;
		}

		@Override
		public List<com.couchbase.client.java.search.result.SearchRow> rows() {
			return Collections.emptyList();
		}

		@Override
		public SearchResult<T> result() {
			return new SearchResult<>(values(), Collections.emptyList(), null, Collections.emptyMap());
		}

		@Override
		public ExecutableFindBySearchOperation.TerminatingFindBySearch<T> matching(
				com.couchbase.client.java.search.SearchRequest searchRequest) {
			return this;
		}

		@Override
		public ExecutableFindBySearchOperation.FindBySearchWithProjection<T> withIndex(String indexName) {
			return this;
		}

		@Override
		public <R> ExecutableFindBySearchOperation.FindBySearchWithFields<R> as(Class<R> returnType) {
			return new StubExecutableFindBySearch<>(results, totalCount, limit, skip);
		}

		@Override
		public ExecutableFindBySearchOperation.FindBySearchInScope<T> withConsistency(
				com.couchbase.client.java.search.SearchScanConsistency scanConsistency) {
			return this;
		}

		@Override
		public ExecutableFindBySearchOperation.FindBySearchInCollection<T> inScope(String scope) {
			return this;
		}

		@Override
		public ExecutableFindBySearchOperation.FindBySearchWithOptions<T> inCollection(String collection) {
			return this;
		}

		@Override
		public ExecutableFindBySearchOperation.FindBySearchWithQuery<T> withOptions(
				com.couchbase.client.java.search.SearchOptions options) {
			return this;
		}

		@Override
		public ExecutableFindBySearchOperation.FindBySearchWithConsistency<T> withLimit(int limit) {
			return new StubExecutableFindBySearch<>(results, totalCount, limit, skip);
		}

		@Override
		public ExecutableFindBySearchOperation.FindBySearchWithLimit<T> withSkip(int skip) {
			return new StubExecutableFindBySearch<>(results, totalCount, limit, skip);
		}

		@Override
		public ExecutableFindBySearchOperation.FindBySearchWithSkip<T> withSort(
				com.couchbase.client.java.search.sort.SearchSort... sort) {
			return this;
		}

		@Override
		public ExecutableFindBySearchOperation.FindBySearchWithSort<T> withHighlight(
				com.couchbase.client.java.search.HighlightStyle style, String... fields) {
			return this;
		}

		@Override
		public ExecutableFindBySearchOperation.FindBySearchWithHighlight<T> withFacets(
				Map<String, com.couchbase.client.java.search.facet.SearchFacet> facets) {
			return this;
		}

		@Override
		public ExecutableFindBySearchOperation.FindBySearchWithFacets<T> withFields(String... fields) {
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
