/*
 * Copyright 2012-present the original author or authors
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

package org.springframework.data.couchbase.core.convert.translation;

import static org.junit.jupiter.api.Assertions.*;

import java.nio.charset.StandardCharsets;

import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.springframework.data.couchbase.core.mapping.CouchbaseDocument;

/**
 * Verifies the functionality of a {@link JacksonTranslationService}.
 *
 * @author Emilien Bevierre
 * @author Michael Nitschinger
 */
public class JacksonTranslationServiceTests {

	private static TranslationService service;

	@BeforeAll
	static void beforeAll() {
		service = new JacksonTranslationService();
		((JacksonTranslationService) service).afterPropertiesSet();
	}

	@Test
	void shouldEncodeNonASCII() {
		CouchbaseDocument doc = new CouchbaseDocument("key");
		doc.put("language", "русский");
		String expected = "{\"language\":\"русский\"}";
		assertEquals(expected, service.encode(doc));
	}

	@Test
	void shouldDecodeNonASCII() {
		String source = "{\"language\":\"русский\"}";
		CouchbaseDocument target = new CouchbaseDocument();
		service.decode(source, target);
		assertEquals("русский", target.get("language"));
	}

	@Test
	void shouldDecodeAdHocFragment() {
		String source = "{\"language\":\"french\"}";
		LanguageFragment f = service.decodeFragment(source, LanguageFragment.class);
		assertNotNull(f);
		assertEquals("french", f.language);
	}

	@Test
	void shouldDecodeFromBytes() {
		String source = "{\"language\":\"english\",\"count\":42}";
		byte[] bytes = source.getBytes(StandardCharsets.UTF_8);

		CouchbaseDocument targetFromString = new CouchbaseDocument();
		service.decode(source, targetFromString);

		CouchbaseDocument targetFromBytes = new CouchbaseDocument();
		service.decode(bytes, targetFromBytes);

		assertEquals(targetFromString.get("language"), targetFromBytes.get("language"));
		assertEquals(targetFromString.get("count"), targetFromBytes.get("count"));
		assertEquals("english", targetFromBytes.get("language"));
		assertEquals(42, targetFromBytes.get("count"));
	}

	@Test
	void shouldDecodeNonASCIIFromBytes() {
		String source = "{\"language\":\"русский\",\"greeting\":\"Привет мир\"}";
		byte[] bytes = source.getBytes(StandardCharsets.UTF_8);

		CouchbaseDocument targetFromString = new CouchbaseDocument();
		service.decode(source, targetFromString);

		CouchbaseDocument targetFromBytes = new CouchbaseDocument();
		service.decode(bytes, targetFromBytes);

		assertEquals(targetFromString.get("language"), targetFromBytes.get("language"));
		assertEquals(targetFromString.get("greeting"), targetFromBytes.get("greeting"));
		assertEquals("русский", targetFromBytes.get("language"));
		assertEquals("Привет мир", targetFromBytes.get("greeting"));
	}

	static class LanguageFragment {
		public String language;
	}

}
