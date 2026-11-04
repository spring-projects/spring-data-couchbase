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
package org.springframework.data.couchbase.core;

import static org.junit.jupiter.api.Assertions.*;

import org.junit.jupiter.api.Test;
import org.springframework.data.core.TypedPropertyPath;

import com.couchbase.client.java.search.HighlightStyle;

/**
 * Unit tests verifying the type-safe {@link TypedPropertyPath} overloads are declared on the
 * {@link ExecutableFindBySearchOperation} and {@link ReactiveFindBySearchOperation} fluent interfaces.
 *
 * @author Emilien Bevierre
 * @since 6.2
 */
class ExecutableFindBySearchOperationTests {

	@Test
	void typedPropertyReferenceOverloadsAreDeclaredOnInterfaces() throws Exception {
		assertNotNull(ExecutableFindBySearchOperation.FindBySearchWithSort.class
				.getMethod("withSort", TypedPropertyPath.class, TypedPropertyPath[].class));
		assertNotNull(ExecutableFindBySearchOperation.FindBySearchWithHighlight.class
				.getMethod("withHighlight", HighlightStyle.class, TypedPropertyPath.class, TypedPropertyPath[].class));
		assertNotNull(ExecutableFindBySearchOperation.FindBySearchWithFields.class
				.getMethod("withFields", TypedPropertyPath.class, TypedPropertyPath[].class));
		assertNotNull(ReactiveFindBySearchOperation.FindBySearchWithSort.class
				.getMethod("withSort", TypedPropertyPath.class, TypedPropertyPath[].class));
		assertNotNull(ReactiveFindBySearchOperation.FindBySearchWithHighlight.class
				.getMethod("withHighlight", HighlightStyle.class, TypedPropertyPath.class, TypedPropertyPath[].class));
		assertNotNull(ReactiveFindBySearchOperation.FindBySearchWithFields.class
				.getMethod("withFields", TypedPropertyPath.class, TypedPropertyPath[].class));
	}
}
