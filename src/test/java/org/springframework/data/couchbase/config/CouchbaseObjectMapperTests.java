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
import org.springframework.data.couchbase.core.convert.translation.Jackson2TranslationService;
import org.springframework.data.couchbase.core.convert.translation.JacksonTranslationService;
import org.springframework.data.couchbase.core.mapping.CouchbaseDocument;
import org.springframework.data.couchbase.core.mapping.Document;
import org.springframework.data.couchbase.domain.Config;
import org.springframework.data.couchbase.domain.ETurbulenceCategory;

import com.couchbase.client.core.encryption.CryptoManager;
import com.couchbase.client.encryption.AeadAes256CbcHmacSha512Provider;
import com.couchbase.client.encryption.DefaultCryptoManager;
import com.couchbase.client.encryption.Keyring;
import com.couchbase.client.java.codec.Jackson3JsonSerializer;
import com.couchbase.client.java.codec.JsonSerializer;
import com.couchbase.client.java.encryption.annotation.Encrypted;
import com.couchbase.client.java.env.ClusterEnvironment;
import com.couchbase.client.java.json.JsonArray;
import com.couchbase.client.java.json.JsonObject;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

import tools.jackson.databind.JsonNode;
import tools.jackson.databind.json.JsonMapper;

/**
 * Checks that the Jackson 3 mappers of {@link AbstractCouchbaseConfiguration} and {@link JacksonTranslationService} write
 * and read the same JSON as the Jackson 2 mapper of earlier versions. The expected values come from a run with Jackson 2.
 *
 * @author Emilien Bevierre
 */
class CouchbaseObjectMapperTests {

	private static final String ENTITY_JSON = "{\"bd\":1.50,\"cal\":0,\"ch\":\"c\",\"date\":0,"
			+ "\"emoji\":\"x\\uD83D\\uDE00y\",\"ja\":[\"a\",1],\"jo\":{\"k\":\"v\",\"n\":{\"x\":1}},\"locale\":\"en_US\","
			+ "\"lower\":\"TWO\",\"month\":\"MARCH\",\"slash\":\"a/b\",\"t\":\"objectMapperEntity\",\"turbulence\":\"\\\"10%\"}";

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

		assertThat(new String(stored, StandardCharsets.UTF_8)).isEqualTo(ENTITY_JSON);

		Entity read = converter.read(Entity.class, doc);
		assertThat(read.locale).isEqualTo(Locale.US);
		assertThat(read.month).isEqualTo(Month.MARCH);
		assertThat(read.lower).isEqualTo(Lower.TWO);
		assertThat(read.turbulence).isEqualTo(ETurbulenceCategory.T10);
		assertThat(read.jo).isEqualTo(new Entity().jo);
		assertThat(read.ja).isEqualTo(new Entity().ja);
	}

	@Test
	void defaultTranslationServiceStoresEntityAsWithJackson2() {
		JacksonTranslationService service = new JacksonTranslationService();
		service.afterPropertiesSet();
		CouchbaseDocument doc = new CouchbaseDocument();
		converter.write(new Entity(), doc);

		String json = service.encode(doc);
		// encode() writes chars, not UTF-8 bytes, so the emoji is not escaped as in ENTITY_JSON
		assertThat(json).isEqualTo(mapper.writeValueAsString(doc.export()));
		assertThat(json).isEqualTo(ENTITY_JSON.replace("\\uD83D\\uDE00", "😀"));

		CouchbaseDocument decoded = (CouchbaseDocument) service.decode(json, new CouchbaseDocument());
		decoded.setId(doc.getId());
		Entity read = converter.read(Entity.class, decoded);
		assertThat(read.locale).isEqualTo(Locale.US);
		assertThat(read.month).isEqualTo(Month.MARCH);
		assertThat(read.lower).isEqualTo(Lower.TWO);
		assertThat(read.turbulence).isEqualTo(ETurbulenceCategory.T10);
		assertThat(read.date).isEqualTo(new Date(0));
		assertThat(read.bd).isEqualTo(new BigDecimal("1.50"));
		assertThat(read.emoji).isEqualTo("x😀y");
		assertThat(read.jo).isEqualTo(new Entity().jo);
		assertThat(read.ja).isEqualTo(new Entity().ja);
	}

	@Test
	@SuppressWarnings("removal")
	void jackson2TranslationServiceStoresEntityAsJacksonTranslationService() {
		Jackson2TranslationService jackson2 = new Jackson2TranslationService();
		jackson2.afterPropertiesSet();
		JacksonTranslationService jackson3 = new JacksonTranslationService();
		jackson3.afterPropertiesSet();
		CouchbaseDocument doc = new CouchbaseDocument();
		converter.write(new Entity(), doc);

		String json = jackson2.encode(doc);
		assertThat(json).isEqualTo(jackson3.encode(doc));

		CouchbaseDocument decoded = (CouchbaseDocument) jackson2.decode(json, new CouchbaseDocument());
		assertThat(decoded.export())
				.isEqualTo(((CouchbaseDocument) jackson3.decode(json, new CouchbaseDocument())).export());
		decoded.setId(doc.getId());
		Entity read = converter.read(Entity.class, decoded);
		assertThat(read.locale).isEqualTo(Locale.US);
		assertThat(read.month).isEqualTo(Month.MARCH);
		assertThat(read.lower).isEqualTo(Lower.TWO);
		assertThat(read.turbulence).isEqualTo(ETurbulenceCategory.T10);
		assertThat(read.date).isEqualTo(new Date(0));
		assertThat(read.bd).isEqualTo(new BigDecimal("1.50"));
		assertThat(read.emoji).isEqualTo("x😀y");
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
	void pojoWithJsonValuesRoundTrips() {
		JsonValuePojo pojo = new JsonValuePojo();
		pojo.jo = JsonObject.create().put("k", "v").put("n", JsonObject.create().put("x", 1)).put("a",
				JsonArray.from("y", 2));
		pojo.ja = JsonArray.from("a", 1, JsonObject.create().put("z", true));
		JsonNode expected = mapper
				.readTree("{\"jo\":{\"k\":\"v\",\"n\":{\"x\":1},\"a\":[\"y\",2]},\"ja\":[\"a\",1,{\"z\":true}]}");

		String json = mapper.writeValueAsString(pojo);
		assertThat(mapper.readTree(json)).isEqualTo(expected);
		JsonValuePojo read = mapper.readValue(json, JsonValuePojo.class);
		assertThat(read.jo).isEqualTo(pojo.jo);
		assertThat(read.ja).isEqualTo(pojo.ja);

		ClusterEnvironment env = ClusterEnvironment.builder().jsonSerializer(Jackson3JsonSerializer.create(mapper)).build();
		try {
			JsonSerializer serializer = env.jsonSerializer();
			byte[] bytes = serializer.serialize(pojo);
			assertThat(mapper.readTree(bytes)).isEqualTo(expected);
			JsonValuePojo fromSdk = serializer.deserialize(JsonValuePojo.class, bytes);
			assertThat(fromSdk.jo).isEqualTo(pojo.jo);
			assertThat(fromSdk.ja).isEqualTo(pojo.ja);
		} finally {
			env.shutdown();
		}
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

	static class JsonValuePojo {
		public JsonObject jo;
		public JsonArray ja;
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
