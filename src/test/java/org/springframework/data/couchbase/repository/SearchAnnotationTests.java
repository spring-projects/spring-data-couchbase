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

import static org.junit.jupiter.api.Assertions.*;

import org.junit.jupiter.api.Test;

/**
 * Unit tests for {@link Search} and {@link SearchIndex} annotations.
 *
 * @author Emilien Bevierre
 * @since 6.2
 */
class SearchAnnotationTests {

	@Test
	void searchAnnotationsAreRuntimeRetained() {
		assertTrue(Search.class.isAnnotation());
		assertTrue(SearchIndex.class.isAnnotation());
	}

	@Test
	void searchIndexAnnotationOnClass() {
		SearchIndex annotation = AnnotatedEntity.class.getAnnotation(SearchIndex.class);
		assertNotNull(annotation);
		assertEquals("my-search-index", annotation.value());
	}

	@Test
	void searchIndexAnnotationOnMethod() throws Exception {
		java.lang.reflect.Method method = AnnotatedMethod.class.getMethod("search");
		SearchIndex annotation = method.getAnnotation(SearchIndex.class);
		assertNotNull(annotation);
		assertEquals("method-level-index", annotation.value());
	}

	@Test
	void searchAnnotationOnMethod() throws Exception {
		java.lang.reflect.Method method = AnnotatedMethod.class.getMethod("search");
		Search annotation = method.getAnnotation(Search.class);
		assertNotNull(annotation);
		assertEquals("description:?0", annotation.value());
	}

	@SearchIndex("my-search-index")
	static class AnnotatedEntity {
	}

	interface AnnotatedMethod {
		@Search("description:?0")
		@SearchIndex("method-level-index")
		void search();
	}
}
