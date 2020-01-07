/*
 * Copyright 2013-2020 the original author or authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      https://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.springframework.data.couchbase.core;

import static org.junit.Assert.*;

import java.util.Map;

import com.couchbase.client.java.Bucket;
import com.couchbase.client.java.document.RawJsonDocument;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.junit.Test;
import org.junit.runner.RunWith;

import org.springframework.beans.factory.annotation.Autowired;
;
import org.springframework.data.couchbase.ContainerResourceRunner;
import org.springframework.data.couchbase.IntegrationTestCustomTypeKeyConfig;
import org.springframework.data.couchbase.core.convert.MappingCouchbaseConverter;
import org.springframework.test.context.ContextConfiguration;

/**
 * Tests the Java Config template around type key modification (DATACOUCH-134)
 *
 * @author Simon Baslé
 */
@RunWith(ContainerResourceRunner.class)
@ContextConfiguration(classes = IntegrationTestCustomTypeKeyConfig.class)
public class TypeKeyTests {

  @Autowired
  private Bucket client;

  @Autowired
  private CouchbaseTemplate template;

  private static final ObjectMapper MAPPER = new ObjectMapper();

  /**
   * see DATACOUCH-134
   */
  @Test
  public void saveSimpleEntityCorrectlyWithDifferentTypeKey() throws Exception {
    String id = "beers:awesome-stout";
    String name = "The Awesome Stout";
    boolean active = false;
    Beer beer = new Beer(id, name, active, "");

    template.save(beer);
    RawJsonDocument resultDoc = client.get(id, RawJsonDocument.class);
    assertNotNull(resultDoc);
    String result = resultDoc.content();
    assertNotNull(result);
    Map<String, Object> resultConv = MAPPER.readValue(result, new TypeReference<Map<String, Object>>() {});

    assertNull(resultConv.get(MappingCouchbaseConverter.TYPEKEY_DEFAULT));
    assertNotNull(resultConv.get("javaClass"));
    assertEquals("org.springframework.data.couchbase.core.Beer", resultConv.get("javaClass"));
    assertEquals(false, resultConv.get("is_active"));
    assertEquals("The Awesome Stout", resultConv.get("name"));
  }
}
