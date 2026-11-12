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

package org.springframework.data.couchbase.core.convert;

import java.util.List;
import java.util.Map;

import org.springframework.core.convert.converter.Converter;
import org.springframework.data.convert.ReadingConverter;
import org.springframework.data.convert.WritingConverter;
import org.springframework.data.couchbase.core.mapping.CouchbaseDocument;

import com.couchbase.client.java.json.JsonValueModule;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;

/**
 * Converters for Jackson 2 {@link JsonNode} entity properties. {@link OtherConverters} registers them only when Jackson
 * 2 is on the class path.
 *
 * @author Michael Reiche
 * @author Emilien Bevierre
 * @since 6.2
 * @deprecated since 6.2, use Jackson 3 {@code tools.jackson.databind.JsonNode} properties instead.
 */
@Deprecated(since = "6.2", forRemoval = true)
public final class Jackson2JsonNodeConverters {

	private Jackson2JsonNodeConverters() {
	}

	/**
	 * Returns all converters by this class that can be registered.
	 *
	 * @return the list of converters to register.
	 */
	public static List<Converter<?, ?>> getConvertersToRegister() {
		return List.of(MapToJsonNode.INSTANCE, JsonNodeToMap.INSTANCE);
	}

	@WritingConverter
	public enum JsonNodeToMap implements Converter<JsonNode, CouchbaseDocument> {
		INSTANCE;

		static ObjectMapper mapper = new ObjectMapper().registerModule(new JsonValueModule());

		@Override
		public CouchbaseDocument convert(JsonNode source) {
			if (source == null) {
				return null;
			}
			return new CouchbaseDocument()
					.setContent((Map) mapper.convertValue(source, new TypeReference<Map<String, Object>>() {
					}));
		}
	}

	@ReadingConverter
	public enum MapToJsonNode implements Converter<CouchbaseDocument, JsonNode> {
		INSTANCE;

		static ObjectMapper mapper = new ObjectMapper().registerModule(new JsonValueModule());

		@Override
		public JsonNode convert(CouchbaseDocument source) {
			if (source == null) {
				return null;
			}
			return mapper.valueToTree(source.export());
		}
	}

}
