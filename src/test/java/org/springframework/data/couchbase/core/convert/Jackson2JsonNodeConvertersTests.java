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

import org.junit.jupiter.api.Test;
import org.springframework.data.annotation.Id;
import org.springframework.data.annotation.TypeAlias;
import org.springframework.data.couchbase.core.mapping.CouchbaseDocument;
import org.springframework.data.couchbase.domain.Config;

import com.couchbase.client.java.codec.Jackson3JsonSerializer;

import tools.jackson.databind.json.JsonMapper;

/**
 * Checks that an entity can hold a Jackson 2 and a Jackson 3 {@code JsonNode} at the same time. Needs Jackson 2 on the
 * class path.
 *
 * @author Emilien Bevierre
 */
class Jackson2JsonNodeConvertersTests {

	@Test
	void jackson2AndJackson3JsonNodesRoundTrip() throws Exception {
		Config config = new Config();
		JsonMapper mapper = config.getObjectMapper();
		CouchbaseCustomConversions conversions = (CouchbaseCustomConversions) config.customConversions();
		MappingCouchbaseConverter converter = config.mappingCouchbaseConverter(config.couchbaseMappingContext(conversions),
				conversions);
		converter.afterPropertiesSet();

		JsonNodes entity = new JsonNodes();
		entity.jackson2 = new com.fasterxml.jackson.databind.ObjectMapper().createObjectNode().put("k", "v2").put("n", 2);
		entity.jackson3 = mapper.createObjectNode().put("k", "v3").put("n", 3);

		CouchbaseDocument doc = new CouchbaseDocument();
		converter.write(entity, doc);

		byte[] stored = Jackson3JsonSerializer.create(mapper).serialize(doc.export());
		assertThat(mapper.readTree(stored)).isEqualTo(mapper.readTree(
				"{\"t\":\"jsonNodes\",\"jackson2\":{\"k\":\"v2\",\"n\":2},\"jackson3\":{\"k\":\"v3\",\"n\":3}}"));

		JsonNodes read = converter.read(JsonNodes.class, doc);
		assertThat(read.jackson2).isEqualTo(entity.jackson2);
		assertThat(read.jackson3).isEqualTo(entity.jackson3);
	}

	// no @Document, so entity scans of other test configs skip this Jackson 2 type
	@TypeAlias("jsonNodes")
	static class JsonNodes {
		@Id String id = "jsonNodes";
		com.fasterxml.jackson.databind.JsonNode jackson2;
		tools.jackson.databind.JsonNode jackson3;
	}
}
