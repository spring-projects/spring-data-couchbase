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

import java.io.StringWriter;
import java.time.Month;
import java.util.Locale;
import java.util.Map;

import org.springframework.data.couchbase.core.mapping.CouchbaseDocument;
import org.springframework.data.couchbase.core.mapping.CouchbaseList;
import org.springframework.data.couchbase.core.mapping.CouchbaseStorable;
import org.springframework.data.mapping.MappingException;
import org.springframework.data.mapping.model.SimpleTypeHolder;

import com.couchbase.client.java.json.Jackson3JsonValueModule;

import tools.jackson.core.JacksonException;
import tools.jackson.core.JsonGenerator;
import tools.jackson.core.JsonParser;
import tools.jackson.core.JsonToken;
import tools.jackson.databind.DeserializationFeature;
import tools.jackson.databind.json.JsonMapper;
import tools.jackson.databind.module.SimpleModule;
import tools.jackson.databind.ser.std.ToStringSerializer;

/**
 * A {@link TranslationService} that uses Jackson 3. {@link JacksonTranslationService} stays the default.
 *
 * @author Emilien Bevierre
 * @since 6.3
 */
public class Jackson3TranslationService implements TranslationService {

	private final JsonMapper jsonMapper;

	/**
	 * Creates a service with a mapper built by {@link #defaultJsonMapperBuilder()}.
	 */
	public Jackson3TranslationService() {
		this(defaultJsonMapperBuilder().build());
	}

	/**
	 * Creates a service with the given mapper. Build it from {@link #defaultJsonMapperBuilder()} to keep the JSON format
	 * of {@link JacksonTranslationService}.
	 *
	 * @param jsonMapper the mapper to use.
	 */
	public Jackson3TranslationService(JsonMapper jsonMapper) {
		this.jsonMapper = jsonMapper;
	}

	/**
	 * Returns a builder for a mapper that writes and reads the same JSON as the default Jackson 2 setup. It registers
	 * {@link Jackson3JsonValueModule}, so the mapper also works with the SDK {@code Jackson3JsonSerializer}.
	 *
	 * @return a new builder.
	 */
	public static JsonMapper.Builder defaultJsonMapperBuilder() {
		return JsonMapper.builder().configureForJackson2()
				// configureForJackson2() enables it
				.disable(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES)
				.addModule(new Jackson3JsonValueModule())
				// Jackson 2 writes a Locale with toString() and a Month by name
				.addModule(new SimpleModule("CouchbaseJackson2Compatibility")
						.addSerializer(Locale.class, ToStringSerializer.instance)
						.addSerializer(Month.class, ToStringSerializer.instance));
	}

	@Override
	public final String encode(CouchbaseStorable source) {
		StringWriter writer = new StringWriter();

		try (JsonGenerator generator = jsonMapper.createGenerator(writer)) {
			encodeRecursive(source, generator);
		} catch (JacksonException ex) {
			throw new RuntimeException("Could not encode JSON", ex);
		}

		return writer.toString();
	}

	private void encodeRecursive(CouchbaseStorable source, JsonGenerator generator) {
		generator.writeStartObject();

		for (Map.Entry<String, Object> entry : ((CouchbaseDocument) source).export().entrySet()) {
			String key = entry.getKey();
			Object value = entry.getValue();
			generator.writeName(key);
			if (value instanceof CouchbaseDocument) {
				encodeRecursive((CouchbaseDocument) value, generator);
				continue;
			}

			Class<?> clazz = value.getClass();

			if (SimpleTypeHolder.DEFAULT.isSimpleType(clazz) && !isEnumOrClass(clazz)) {
				generator.writePOJO(value);
			} else {
				jsonMapper.writeValue(generator, value);
			}
		}

		generator.writeEndObject();
	}

	private boolean isEnumOrClass(Class<?> clazz) {
		return Enum.class.isAssignableFrom(clazz) || Class.class.isAssignableFrom(clazz);
	}

	@Override
	public final CouchbaseStorable decode(String source, CouchbaseStorable target) {
		try (JsonParser parser = jsonMapper.createParser(source)) {
			return decodeWithParser(parser, target);
		} catch (JacksonException ex) {
			throw new RuntimeException("Could not decode JSON", ex);
		}
	}

	@Override
	public final CouchbaseStorable decode(byte[] source, CouchbaseStorable target) {
		try (JsonParser parser = jsonMapper.createParser(source)) {
			return decodeWithParser(parser, target);
		} catch (JacksonException ex) {
			throw new RuntimeException("Could not decode JSON", ex);
		}
	}

	private CouchbaseStorable decodeWithParser(JsonParser parser, CouchbaseStorable target) {
		while (parser.nextToken() != null) {
			JsonToken currentToken = parser.currentToken();

			if (currentToken == JsonToken.START_OBJECT) {
				return decodeObject(parser, (CouchbaseDocument) target);
			} else if (currentToken == JsonToken.START_ARRAY) {
				return decodeArray(parser, new CouchbaseList());
			} else {
				throw new MappingException("JSON to decode needs to start as array or object!");
			}
		}
		return target;
	}

	private CouchbaseDocument decodeObject(JsonParser parser, CouchbaseDocument target) {
		JsonToken currentToken = parser.nextToken();

		String fieldName = "";
		while (currentToken != null && currentToken != JsonToken.END_OBJECT) {
			if (currentToken == JsonToken.START_OBJECT) {
				target.put(fieldName, decodeObject(parser, new CouchbaseDocument()));
			} else if (currentToken == JsonToken.START_ARRAY) {
				target.put(fieldName, decodeArray(parser, new CouchbaseList()));
			} else if (currentToken == JsonToken.PROPERTY_NAME) {
				fieldName = parser.currentName();
			} else {
				target.put(fieldName, decodePrimitive(currentToken, parser));
			}

			currentToken = parser.nextToken();
		}

		return target;
	}

	private CouchbaseList decodeArray(JsonParser parser, CouchbaseList target) {
		JsonToken currentToken = parser.nextToken();

		while (currentToken != null && currentToken != JsonToken.END_ARRAY) {
			if (currentToken == JsonToken.START_OBJECT) {
				target.put(decodeObject(parser, new CouchbaseDocument()));
			} else if (currentToken == JsonToken.START_ARRAY) {
				target.put(decodeArray(parser, new CouchbaseList()));
			} else {
				target.put(decodePrimitive(currentToken, parser));
			}

			currentToken = parser.nextToken();
		}

		return target;
	}

	private Object decodePrimitive(JsonToken token, JsonParser parser) {
		switch (token) {
			case VALUE_TRUE:
			case VALUE_FALSE:
				return parser.getBooleanValue();
			case VALUE_STRING:
				return parser.getValueAsString();
			case VALUE_NUMBER_INT:
				return parser.getNumberValue();
			case VALUE_NUMBER_FLOAT:
				return parser.getDecimalValue();
			case VALUE_NULL:
				return null;
			default:
				throw new MappingException("Could not decode primitive value " + token);
		}
	}

	@Override
	public <T> T decodeFragment(String source, Class<T> target) {
		try {
			return jsonMapper.readValue(source, target);
		} catch (JacksonException e) {
			throw new RuntimeException("Cannot decode ad-hoc JSON", e);
		}
	}

}
