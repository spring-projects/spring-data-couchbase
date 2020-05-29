/*
 * Copyright 2012-2020 the original author or authors
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

import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.springframework.data.couchbase.core.mapping.CouchbaseDocument;
import com.fasterxml.jackson.annotation.JsonInclude;

import java.lang.reflect.Field;
import java.util.Optional;

/**
 * Verifies the functionality of a {@link JacksonTranslationService}.
 *
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
	void shouldEncDecAdHocFragment() {
		String source = "{\"language\":\"french\"}";
		LanguageFragment f = service.decodeFragment(source, LanguageFragment.class);
		assertNotNull(f);
		assertEquals("french", f.language);
		CouchbaseDocument doc = docFromEntity(f);
		assertEquals(source, service.encode(doc));
	}

	@Test
	void shouldEncDecAdHocMissingFragment() {
		String source = "{}";
		LanguageFragment f = service.decodeFragment(source, LanguageFragment.class);
		assertNotNull(f);
		assertEquals(null, f.language);
		CouchbaseDocument doc = docFromEntity(f);
		assertEquals(source, service.encode(doc));
	}

	// for field that is not Optional<>,  missing and null decode to the same document
	// however, encode will encode both the same ->   field : null
	@Test
	void shouldEncDecAdHocNullFragment() {
		String source = "{\"language\":null}";
		LanguageFragment f = service.decodeFragment(source, LanguageFragment.class);
		assertNotNull(f);
		assertEquals(null, f.language);
		CouchbaseDocument doc = docFromEntity(f);
		assertEquals("{}", service.encode(doc));
	}

	@Test
	void shouldEncDecAdHocFragmentOptional() {
		String source = "{\"language\":\"french\"}";
		LanguageFragmentOptional f = service.decodeFragment(source, LanguageFragmentOptional.class); // needs Jdk8Module()
		assertNotNull(f);
		assertEquals(Optional.of("french"), f.language);
		CouchbaseDocument doc = docFromEntity(f);
		assertEquals(source, service.encode(doc));
	}

	// this will also encode to field : null, unless the encoder has _skipNulls == true
	@Test
	void shouldEncDecAdHocMissingFragmentOptional() {
		String source = "{}";
		LanguageFragmentOptional f = service.decodeFragment(source, LanguageFragmentOptional.class);
		assertNotNull(f);
		assertEquals(null, f.language);
		CouchbaseDocument doc = docFromEntity(f);
		assertEquals(source, service.encode(doc));
	}

	@Test
	void shouldEncDecAdHocNullFragmentOptional() {
		String source = "{\"language\":null}";
		// this is going to get  language == null instead of language = Optional.empty();
		// com.fasterxml.jackson.databind.deser.BeanDeserializer
		//    public void deserializeAndSet(JsonParser p, DeserializationContext ctxt, Object instance) throws IOException {
		//        Object value;
		//        if (p.hasToken(JsonToken.VALUE_NULL)) {
		//            if (this._skipNulls) {
		//                return;
		//            }
		//			  value = this._nullProvider.getNullValue(ctxt);
		LanguageFragmentOptional f = service.decodeFragment(source, LanguageFragmentOptional.class);
		assertNotNull(f);
		assertEquals(Optional.empty(), f.language);  //needs Jdk8Module() _nullProvider.getNullValue() return Optional.empty()
		CouchbaseDocument doc = docFromEntity(f);
		assertEquals(source, service.encode(doc));
	}

	@Test
	void shouldEncDecObjectFragmentOptional() {
		String source = "{\"language\":\"french\"}";
		CouchbaseDocument target = new CouchbaseDocument();
		service.decode(source, target);
		assertEquals("french", target.get("language"));
		assertEquals(source, service.encode(target));
	}

	// this will also encode to field : null, unless the encoder has _skipNulls == true
	@Test
	void shouldEncDecObjectMissingFragmentOptional() {
		String source = "{}";
		CouchbaseDocument target = new CouchbaseDocument();
		service.decode(source, target);
		assertEquals(null, target.get("language"));
		assertEquals(source, service.encode(target));
	}

	@Test
	void shouldEncDecObjectNullFragmentOptional() {
		String source = "{\"language\":null}";
		// this is going to get  language == null instead of language = Optional.empty();
		// com.fasterxml.jackson.databind.deser.BeanDeserializer
		//    public void deserializeAndSet(JsonParser p, DeserializationContext ctxt, Object instance) throws IOException {
		//        Object value;
		//        if (p.hasToken(JsonToken.VALUE_NULL)) {
		//            if (this._skipNulls) {
		//                return;
		//            }
		//			  value = this._nullProvider.getNullValue(ctxt);
		CouchbaseDocument target = new CouchbaseDocument();
		service.decode(source, target);
		assertEquals(null, target.get("language")); // this will be null. Decode treats as regular property
		// TODO: target gets encoded to { language:null } instead of {}
		assertEquals(source, service.encode(target));
	}

	static class LanguageFragment {
		public String language;
	}

	@JsonInclude(JsonInclude.Include.NON_NULL)
	static class LanguageFragmentOptional {
		@JsonInclude(JsonInclude.Include.NON_NULL)
		public Optional<String> language;
	}

	static class NestedLanguageFragment {
		static class Sublanguage {
			String country;
		}

		public String language;
		public Sublanguage sublanguage;
	}

	CouchbaseDocument docFromEntity(Object o) {
		CouchbaseDocument doc = new CouchbaseDocument("key");
		Field[] fields = o.getClass().getDeclaredFields();
		for (Field f : fields) {
			switch (f.getType().getName()) {
			case "java.lang.String":
				try {
					if (f.get(o) != null) {
						System.out.println("put " + f.getName() + " = " + f.get(o));
						doc.put(f.getName(), f.get(o));
					}
				} catch (IllegalAccessException iae) {
					throw new RuntimeException(iae);
				}
				break;
			case "java.util.Optional":
				try {
					Optional<Object> opt = (Optional<Object>) f.get(o);
					if (opt != null) {
						System.out.println("put " + f.getName() + " = " + opt);
						doc.put(f.getName(), opt.isPresent() ? opt.get() : null);
					}
				} catch (IllegalAccessException iae) {
					throw new RuntimeException(iae);
				}
				break;
			default:
				throw new RuntimeException("unhandled type " + f.getType().getName());
			}
		}
		return doc;

	}
}
