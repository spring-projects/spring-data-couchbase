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

import java.lang.reflect.Method;
import java.util.List;

import org.junit.jupiter.api.Test;
import org.springframework.data.couchbase.core.mapping.CouchbaseMappingContext;
import org.springframework.data.couchbase.core.mapping.CouchbasePersistentEntity;
import org.springframework.data.couchbase.core.mapping.CouchbasePersistentProperty;
import org.springframework.data.couchbase.domain.User;
import org.springframework.data.couchbase.repository.ScanConsistency;
import org.springframework.data.couchbase.repository.Search;
import org.springframework.data.couchbase.repository.SearchIndex;
import org.springframework.data.mapping.context.MappingContext;
import org.springframework.data.projection.SpelAwareProxyProjectionFactory;
import org.springframework.data.repository.CrudRepository;
import org.springframework.data.repository.core.support.DefaultRepositoryMetadata;

import com.couchbase.client.java.search.SearchScanConsistency;

/**
 * Unit tests for the {@link ScanConsistency#search()} attribute and its resolution
 * through {@link CouchbaseQueryMethod#getSearchScanConsistency()}.
 *
 * @author Emilien Bevierre
 * @since 6.2
 */
class SearchScanConsistencyTests {

	private final MappingContext<? extends CouchbasePersistentEntity<?>, CouchbasePersistentProperty> mappingContext;

	SearchScanConsistencyTests() {
		CouchbaseMappingContext ctx = new CouchbaseMappingContext();
		ctx.setInitialEntitySet(java.util.Collections.singleton(User.class));
		ctx.afterPropertiesSet();
		this.mappingContext = ctx;
	}

	@Test
	void searchScanConsistencyIsResolvedFromMethodAnnotation() {
		CouchbaseQueryMethod method = createQueryMethod(TestScanConsistencyRepository.class, "searchWithConsistency");
		SearchScanConsistency consistency = method.getSearchScanConsistency();
		assertNotNull(consistency);
		assertEquals(SearchScanConsistency.NOT_BOUNDED, consistency);
	}

	@Test
	void searchScanConsistencyDefaultsToNotBoundedWhenAnnotatedWithoutSearchAttribute() {
		CouchbaseQueryMethod method = createQueryMethod(TestScanConsistencyRepository.class,
				"searchWithoutExplicitSearchConsistency");
		SearchScanConsistency consistency = method.getSearchScanConsistency();
		// @ScanConsistency is present (on the query attribute), so getSearchScanConsistency()
		// returns the default for the search attribute which is NOT_BOUNDED
		assertNotNull(consistency);
		assertEquals(SearchScanConsistency.NOT_BOUNDED, consistency);
	}

	@Test
	void searchScanConsistencyAnnotationValueAttribute() throws NoSuchMethodException {
		// Verify the search attribute exists on the ScanConsistency annotation
		ScanConsistency annotation = TestScanConsistencyRepository.class.getMethod("searchWithConsistency")
				.getAnnotation(ScanConsistency.class);
		// Just verify the annotation type has the search() attribute
		assertDoesNotThrow(() -> ScanConsistency.class.getMethod("search"));
	}

	interface TestScanConsistencyRepository extends CrudRepository<User, String> {

		@Search("test query")
		@SearchIndex("test-index")
		@ScanConsistency(search = SearchScanConsistency.NOT_BOUNDED)
		List<User> searchWithConsistency();

		@Search("test query")
		@SearchIndex("test-index")
		@ScanConsistency(query = com.couchbase.client.java.query.QueryScanConsistency.REQUEST_PLUS)
		List<User> searchWithoutExplicitSearchConsistency();
	}

	private CouchbaseQueryMethod createQueryMethod(Class<?> repoClass, String methodName) {
		try {
			Method method = null;
			for (Method m : repoClass.getMethods()) {
				if (m.getName().equals(methodName)) {
					method = m;
					break;
				}
			}
			assertNotNull(method, "Method " + methodName + " not found");
			DefaultRepositoryMetadata metadata = new DefaultRepositoryMetadata(repoClass);
			return new CouchbaseQueryMethod(method, metadata, new SpelAwareProxyProjectionFactory(), mappingContext);
		} catch (Exception e) {
			throw new RuntimeException(e);
		}
	}
}
