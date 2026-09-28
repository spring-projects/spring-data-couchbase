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
package org.springframework.data.couchbase.config;

import static org.assertj.core.api.Assertions.assertThat;

import java.math.BigDecimal;
import java.nio.charset.StandardCharsets;
import java.time.Month;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;

import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.springframework.data.annotation.Id;
import org.springframework.data.annotation.TypeAlias;
import org.springframework.data.couchbase.core.convert.CouchbaseCustomConversions;
import org.springframework.data.couchbase.core.convert.MappingCouchbaseConverter;
import org.springframework.data.couchbase.core.convert.OtherConverters;
import org.springframework.data.couchbase.core.mapping.CouchbaseDocument;
import org.springframework.data.couchbase.core.mapping.Document;
import org.springframework.data.couchbase.domain.Config;
import org.springframework.data.couchbase.domain.ETurbulenceCategory;

import com.couchbase.client.core.encryption.CryptoManager;
import com.couchbase.client.encryption.AeadAes256CbcHmacSha512Provider;
import com.couchbase.client.encryption.DefaultCryptoManager;
import com.couchbase.client.encryption.Keyring;
import com.couchbase.client.java.codec.Jackson3JsonSerializer;
import com.couchbase.client.java.encryption.annotation.Encrypted;
import com.couchbase.client.java.json.JsonArray;
import com.couchbase.client.java.json.JsonObject;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

import tools.jackson.databind.JsonNode;
import tools.jackson.databind.json.JsonMapper;

/**
 * Checks that the Jackson 3 mapper of {@link AbstractCouchbaseConfiguration} writes and reads the same JSON as the
 * Jackson 2 mapper of earlier versions. The expected values come from a run with Jackson 2.
 *
 * @author Emilien Bevierre
 */
class CouchbaseObjectMapperTests {

	private static Config config;
	private static JsonMapper mapper;
	private static MappingCouchbaseConverter converter;

	@BeforeAll
	static void beforeAll() throws Exception {
		config = new Config();
		mapper = config.getObjectMapper();
		converter = config.mappingCouchbaseConverter(config.couchbaseMappingContext(config.customConversions()),
				(CouchbaseCustomConversions) config.customConversions());
		converter.afterPropertiesSet();
	}

	@Test
	void entityIsStoredAsWithJackson2() {
		CouchbaseDocument doc = new CouchbaseDocument();
		converter.write(new Entity(), doc);

		byte[] stored = Jackson3JsonSerializer.create(mapper).serialize(doc.export());

		assertThat(new String(stored, StandardCharsets.UTF_8)).isEqualTo("{\"bd\":1.50,\"cal\":0,\"ch\":\"c\",\"date\":0,"
				+ "\"emoji\":\"x\\uD83D\\uDE00y\",\"ja\":[\"a\",1],\"jo\":{\"k\":\"v\",\"n\":{\"x\":1}},\"locale\":\"en_US\","
				+ "\"lower\":\"TWO\",\"month\":\"MARCH\",\"slash\":\"a/b\",\"t\":\"objectMapperEntity\",\"turbulence\":\"\\\"10%\"}");

		Entity read = converter.read(Entity.class, doc);
		assertThat(read.locale).isEqualTo(Locale.US);
		assertThat(read.month).isEqualTo(Month.MARCH);
		assertThat(read.lower).isEqualTo(Lower.TWO);
		assertThat(read.turbulence).isEqualTo(ETurbulenceCategory.T10);
		assertThat(read.jo).isEqualTo(new Entity().jo);
		assertThat(read.ja).isEqualTo(new Entity().ja);
	}

	@Test
	void pojoIsWrittenAsWithJackson2() {
		Pojo pojo = new Pojo();
		pojo.b = "b";
		pojo.a = "a";
		pojo.date = new Date(0);
		pojo.lower = Lower.ONE;
		pojo.month = Month.MARCH;
		pojo.locale = Locale.US;

		assertThat(mapper.writeValueAsString(pojo))
				.isEqualTo("{\"b\":\"b\",\"a\":\"a\",\"date\":0,\"lower\":\"ONE\",\"month\":\"MARCH\",\"locale\":\"en_US\"}");
	}

	@Test
	void pojoIsReadAsWithJackson2() {
		Pojo pojo = mapper.readValue("{\"lower\":\"ONE\",\"month\":\"MARCH\",\"locale\":\"en_US\",\"unknown\":1} ",
				Pojo.class);
		assertThat(pojo.lower).isEqualTo(Lower.ONE);
		assertThat(pojo.month).isEqualTo(Month.MARCH);
		assertThat(pojo.locale).isEqualTo(Locale.US);

		assertThat(mapper.readValue("{\"number\":null}", Primitive.class).number).isEqualTo(0);
		assertThat(mapper.readValue("{\"name\":\"n\"}", FinalField.class).name).isEqualTo("n");
		assertThat(mapper.readValue("{\"items\":[\"a\"]}", GetterOnly.class).getItems()).containsExactly("a");
		assertThat(mapper.readValue("{} trailing", Map.class)).isEmpty();
	}

	@Test
	void encryptsFieldsWithCryptoManager() {
		JsonMapper encryptingMapper = new Config() {
			@Override
			protected CryptoManager cryptoManager() {
				Map<String, byte[]> keys = new HashMap<>();
				keys.put("myKey", new byte[64]);
				AeadAes256CbcHmacSha512Provider provider = AeadAes256CbcHmacSha512Provider.builder()
						.keyring(Keyring.fromMap(keys)).build();
				return DefaultCryptoManager.builder().decrypter(provider.decrypter())
						.defaultEncrypter(provider.encrypterForKey("myKey")).build();
			}
		}.getObjectMapper();

		String json = encryptingMapper.writeValueAsString(new Secret("s", "p"));
		assertThat(json).startsWith("{\"encrypted$secret\":{").endsWith(",\"plain\":\"p\"}").doesNotContain("\"s\"");

		Secret read = encryptingMapper.readValue(json, Secret.class);
		assertThat(read.secret).isEqualTo("s");
		assertThat(read.plain).isEqualTo("p");
	}

	@Test
	void jsonNodeStripsTrailingZerosAsWithJackson2() {
		JsonNode node = OtherConverters.MapToJsonNode.INSTANCE
				.convert(new CouchbaseDocument().put("value", new BigDecimal("1.50")));
		assertThat(node.toString()).isEqualTo("{\"value\":1.5}");
	}

	enum Lower {
		ONE, TWO;

		@Override
		public String toString() {
			return name().toLowerCase(Locale.ROOT);
		}
	}

	@Document
	@TypeAlias("objectMapperEntity")
	static class Entity {
		@Id String id = "id";
		String slash = "a/b";
		String emoji = "x😀y";
		Locale locale = Locale.US;
		Month month = Month.MARCH;
		Lower lower = Lower.TWO;
		ETurbulenceCategory turbulence = ETurbulenceCategory.T10;
		Date date = new Date(0);
		java.util.Calendar cal = calendarAtEpoch();
		BigDecimal bd = new BigDecimal("1.50");
		char ch = 'c';
		JsonObject jo = JsonObject.create().put("k", "v").put("n", JsonObject.create().put("x", 1));
		JsonArray ja = JsonArray.from("a", 1);

		private static java.util.Calendar calendarAtEpoch() {
			java.util.Calendar calendar = java.util.Calendar.getInstance();
			calendar.setTimeInMillis(0);
			return calendar;
		}
	}

	static class Pojo {
		public String b;
		public String a;
		public Date date;
		public Lower lower;
		public Month month;
		public Locale locale;
	}

	static class Secret {
		@Encrypted private final String secret;
		private final String plain;

		@JsonCreator
		Secret(@JsonProperty("secret") String secret, @JsonProperty("plain") String plain) {
			this.secret = secret;
			this.plain = plain;
		}

		public String getSecret() {
			return secret;
		}

		public String getPlain() {
			return plain;
		}
	}

	static class Primitive {
		public int number = 1;
	}

	static class FinalField {
		public final String name = null;
	}

	static class GetterOnly {
		private final List<String> items = new ArrayList<>();

		public List<String> getItems() {
			return items;
		}
	}
}
