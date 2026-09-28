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

import java.io.StringWriter;
import java.util.Map;

import org.springframework.beans.factory.InitializingBean;
import org.springframework.data.couchbase.core.mapping.CouchbaseDocument;
import org.springframework.data.couchbase.core.mapping.CouchbaseList;
import org.springframework.data.couchbase.core.mapping.CouchbaseStorable;
import org.springframework.data.mapping.MappingException;
import org.springframework.data.mapping.model.SimpleTypeHolder;

import tools.jackson.core.JacksonException;
import tools.jackson.core.JsonGenerator;
import tools.jackson.core.JsonParser;
import tools.jackson.core.JsonToken;
import tools.jackson.databind.DeserializationFeature;
import tools.jackson.databind.ObjectMapper;
import tools.jackson.databind.json.JsonMapper;

/**
 * A Jackson JSON Translator that implements the {@link TranslationService}
 * contract.
 *
 * @author Michael Nitschinger
 * @author Simon Baslé
 * @author Anastasiia Smirnova
 * @author Mark Paluch
 * @author Emilien Bevierre
 */
public class JacksonTranslationService implements TranslationService, InitializingBean {

	private static final ObjectMapper DEFAULT_OBJECT_MAPPER = JsonMapper.builder().build();

	/**
	 * Jackson Object Mapper;
	 */
	private ObjectMapper objectMapper = DEFAULT_OBJECT_MAPPER;

	/**
	 * Type holder to help easily identify simple types.
	 */
	private SimpleTypeHolder simpleTypeHolder = SimpleTypeHolder.DEFAULT;

	/**
	 * Encode a {@link CouchbaseStorable} to a JSON string.
	 *
	 * @param source the source document to encode.
	 * @return the encoded JSON String.
	 */
	@Override
	public final String encode(CouchbaseStorable source) {
		StringWriter writer = new StringWriter();

		try (JsonGenerator generator = objectMapper.createGenerator(writer)) {
			encodeRecursive(source, generator);
		} catch (JacksonException ex) {
			throw new RuntimeException("Could not encode JSON", ex);
		}

		return writer.toString();
	}

	/**
	 * Recursively iterates through the sources and adds it to the JSON generator.
	 *
	 * @param source    the source document
	 * @param generator the JSON generator.
	 */
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

			if (simpleTypeHolder.isSimpleType(clazz) && !isEnumOrClass(clazz)) {
				generator.writePOJO(value);
			} else {
				objectMapper.writeValue(generator, value);
			}

		}

		generator.writeEndObject();
	}

	private boolean isEnumOrClass(Class<?> clazz) {
		return Enum.class.isAssignableFrom(clazz) || Class.class.isAssignableFrom(clazz);
	}

	/**
	 * Decode a JSON string into the {@link CouchbaseStorable} structure.
	 *
	 * @param source the source formatted document.
	 * @param target the target of the populated data.
	 * @return the decoded structure.
	 */
	@Override
	public final CouchbaseStorable decode(String source, CouchbaseStorable target) {
		try (JsonParser parser = objectMapper.createParser(source)) {
			return decodeWithParser(parser, target);
		} catch (JacksonException ex) {
			throw new RuntimeException("Could not decode JSON", ex);
		}
	}

	/**
	 * Decode a JSON byte array into the {@link CouchbaseStorable} structure.
	 * This avoids the intermediate String allocation by parsing directly from
	 * bytes.
	 *
	 * @param source the source formatted document as bytes (UTF-8 encoded).
	 * @param target the target of the populated data.
	 * @return the decoded structure.
	 */
	@Override
	public final CouchbaseStorable decode(byte[] source, CouchbaseStorable target) {
		try (JsonParser parser = objectMapper.createParser(source)) {
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

	/**
	 * Helper method to decode an object recursively.
	 *
	 * @param parser the JSON parser with the content.
	 * @param target the target where the content should be stored.
	 * @returns the decoded object.
	 */
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

	/**
	 * Helper method to decode an array recusrively.
	 *
	 * @param parser the JSON parser with the content.
	 * @param target the target where the content should be stored.
	 * @returns the decoded list.
	 */
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

	/**
	 * Helper method to decode and assign a primitive.
	 *
	 * @param token  the type of token.
	 * @param parser the parser with the content.
	 * @return the decoded primitve.
	 */
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
			return objectMapper.readerFor(target).without(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES)
					.readValue(source);
		} catch (JacksonException e) {
			throw new RuntimeException("Cannot decode ad-hoc JSON", e);
		}
	}

	public void setObjectMapper(ObjectMapper objectMapper) {
		this.objectMapper = objectMapper;
	}

	@Override
	public void afterPropertiesSet() {
		if (objectMapper == null) {
			objectMapper = DEFAULT_OBJECT_MAPPER;
		}
	}

}
