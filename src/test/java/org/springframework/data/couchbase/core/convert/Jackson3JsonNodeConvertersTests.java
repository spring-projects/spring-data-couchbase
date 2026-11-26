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

import static org.assertj.core.api.Assertions.assertThat;

import java.util.List;
import java.util.Map;

import org.junit.jupiter.api.Test;
import org.springframework.data.annotation.Id;
import org.springframework.data.couchbase.core.mapping.CouchbaseDocument;
import org.springframework.data.couchbase.domain.Config;

import tools.jackson.databind.JsonNode;
import tools.jackson.databind.json.JsonMapper;

/**
 * Checks that the converter writes and reads an entity property of type Jackson 3 {@link JsonNode}.
 *
 * @author Emilien Bevierre
 */
class Jackson3JsonNodeConvertersTests {

	@Test
	void writesAndReadsJsonNode() throws Exception {
		Config config = new Config();
		CouchbaseCustomConversions conversions = (CouchbaseCustomConversions) config.customConversions();
		MappingCouchbaseConverter converter = config.mappingCouchbaseConverter(config.couchbaseMappingContext(conversions),
				conversions);
		converter.afterPropertiesSet();

		Holder holder = new Holder();
		String nodeJson = "{\"k\":\"v\",\"n\":1,\"list\":[true]}";
		holder.node = new JsonMapper().readTree(nodeJson);

		CouchbaseDocument written = new CouchbaseDocument();
		converter.write(holder, written);
		assertThat(written.export().get("node")).isEqualTo(Map.of("k", "v", "n", 1, "list", List.of(true)));

		CouchbaseDocument stored = (CouchbaseDocument) config.couchbaseTranslationService()
				.decode("{\"node\":" + nodeJson + "}", new CouchbaseDocument("id"));
		assertThat(converter.read(Holder.class, stored).node).isEqualTo(holder.node);
	}

	static class Holder {
		@Id String id = "id";
		JsonNode node;
	}

}
