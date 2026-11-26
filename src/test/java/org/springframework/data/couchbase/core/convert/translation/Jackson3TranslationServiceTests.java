/*
 * Copyright 2026-present the original author or authors
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

import static org.assertj.core.api.Assertions.assertThat;

import java.math.BigDecimal;
import java.nio.charset.StandardCharsets;
import java.time.Month;
import java.util.Date;
import java.util.List;
import java.util.Locale;

import org.junit.jupiter.api.Test;
import org.springframework.data.couchbase.core.mapping.CouchbaseDocument;
import org.springframework.data.couchbase.core.mapping.CouchbaseList;
import org.springframework.data.couchbase.domain.Config;

import com.couchbase.client.java.codec.Jackson3JsonSerializer;
import com.couchbase.client.java.codec.JacksonJsonSerializer;
import com.fasterxml.jackson.databind.ObjectMapper;

/**
 * Checks that {@link Jackson3TranslationService} writes and reads the same JSON as the default Jackson 2 setup.
 *
 * @author Emilien Bevierre
 */
class Jackson3TranslationServiceTests {

	private final Config config = new Config();

	private final TranslationService jackson2 = config.couchbaseTranslationService();

	private final Jackson3TranslationService jackson3 = new Jackson3TranslationService();

	@Test
	void encodesLikeJackson2() {
		CouchbaseDocument doc = new CouchbaseDocument().put("text", "русский x😀y a/b \"q\" \\ \t \u0001  ").put("int", 42)
				.put("long", Long.MAX_VALUE).put("double", 1.5d).put("decimal", new BigDecimal("1.50")).put("bool", true)
				.put("lower", Lower.TWO).put("month", Month.MARCH)
				.put("nested", new CouchbaseDocument().put("k", "v").put("list", new CouchbaseList(List.of("a", 1))));

		assertThat(jackson3.encode(doc)).isEqualTo(jackson2.encode(doc));
	}

	@Test
	void serializesForSdkLikeJackson2() throws Exception {
		// The Jackson 2 translation service cannot encode these types, so compare with the SDK serializer.
		CouchbaseDocument doc = new CouchbaseDocument().put("char", 'c').put("locale", Locale.US).put("date", new Date(0))
				.put("lower", Lower.TWO).put("month", Month.MARCH).put("text", "x😀y");
		ObjectMapper objectMapper = config.getObjectMapper();

		byte[] expected = JacksonJsonSerializer.create(objectMapper).serialize(doc.export());
		byte[] actual = Jackson3JsonSerializer.create(Jackson3TranslationService.defaultJsonMapperBuilder().build())
				.serialize(doc.export());

		assertThat(objectMapper.readTree(actual)).isEqualTo(objectMapper.readTree(expected));
	}

	@Test
	void decodesLikeJackson2() {
		String json = "{\"text\":\"русский x😀y\",\"int\":42,\"long\":9223372036854775807,\"decimal\":1.50,"
				+ "\"bool\":false,\"none\":null,\"nested\":{\"list\":[\"a\",1,{\"k\":\"v\"},[true]]}}";

		Object expected = ((CouchbaseDocument) jackson2.decode(json, new CouchbaseDocument())).export();

		assertThat(((CouchbaseDocument) jackson3.decode(json, new CouchbaseDocument())).export()).isEqualTo(expected);
		assertThat(((CouchbaseDocument) jackson3.decode(json.getBytes(StandardCharsets.UTF_8), new CouchbaseDocument()))
				.export()).isEqualTo(expected);
	}

	@Test
	void decodesFragmentLikeJackson2() {
		String json = "{\"language\":\"french\",\"count\":null,\"unknown\":1}";

		Fragment expected = jackson2.decodeFragment(json, Fragment.class);
		Fragment actual = jackson3.decodeFragment(json, Fragment.class);

		assertThat(actual.language).isEqualTo(expected.language).isEqualTo("french");
		assertThat(actual.count).isEqualTo(expected.count).isZero();
	}

	@Test
	void deserializesForSdkLikeJackson2() {
		byte[] json = "{\"language\":\"french\",\"unknown\":1}".getBytes(StandardCharsets.UTF_8);

		Fragment expected = JacksonJsonSerializer.create(config.getObjectMapper()).deserialize(Fragment.class, json);
		Fragment actual = Jackson3JsonSerializer.create(Jackson3TranslationService.defaultJsonMapperBuilder().build())
				.deserialize(Fragment.class, json);

		assertThat(actual.language).isEqualTo(expected.language).isEqualTo("french");
	}

	enum Lower {
		ONE, TWO;

		@Override
		public String toString() {
			return name().toLowerCase(Locale.ROOT);
		}
	}

	static class Fragment {
		public String language;
		public int count;
	}

}
