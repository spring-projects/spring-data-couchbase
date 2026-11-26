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

import tools.jackson.core.type.TypeReference;
import tools.jackson.databind.JsonNode;
import tools.jackson.databind.json.JsonMapper;

/**
 * Converters for Jackson 3 {@link JsonNode} properties. {@link OtherConverters} registers them only when Jackson 3 is
 * on the class path.
 *
 * @author Emilien Bevierre
 * @since 6.3
 */
final class Jackson3JsonNodeConverters {

	private static final JsonMapper MAPPER = new JsonMapper();

	private Jackson3JsonNodeConverters() {
	}

	static List<Converter<?, ?>> getConvertersToRegister() {
		return List.of(JsonNodeToMap.INSTANCE, MapToJsonNode.INSTANCE);
	}

	@WritingConverter
	enum JsonNodeToMap implements Converter<JsonNode, CouchbaseDocument> {
		INSTANCE;

		@Override
		public CouchbaseDocument convert(JsonNode source) {
			return new CouchbaseDocument()
					.setContent(MAPPER.convertValue(source, new TypeReference<Map<String, Object>>() {}));
		}
	}

	@ReadingConverter
	enum MapToJsonNode implements Converter<CouchbaseDocument, JsonNode> {
		INSTANCE;

		@Override
		public JsonNode convert(CouchbaseDocument source) {
			return MAPPER.valueToTree(source.export());
		}
	}

}
