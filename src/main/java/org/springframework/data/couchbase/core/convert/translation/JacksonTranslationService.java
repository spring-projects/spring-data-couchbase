/*
 * Copyright 2013-2015 the original author or authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.springframework.data.couchbase.core.convert.translation;

import com.fasterxml.jackson.core.JsonFactory;
import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.core.JsonToken;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.springframework.beans.factory.InitializingBean;
import org.springframework.data.couchbase.core.mapping.CouchbaseDocument;
import org.springframework.data.couchbase.core.mapping.CouchbaseList;
import org.springframework.data.couchbase.core.mapping.CouchbaseStorable;
import org.springframework.data.mapping.model.MappingException;
import org.springframework.data.mapping.model.SimpleTypeHolder;

import java.io.IOException;
import java.io.StringWriter;
import java.io.Writer;
import java.util.Map;

/**
 * A Jackson JSON Translator that implements the {@link TranslationService} contract.
 *
 * @author Michael Nitschinger
 * @author Anastasiia Smirnova
 */
public class JacksonTranslationService implements TranslationService, InitializingBean {

  /**
   * Jackson Object Mapper;
   */
  private ObjectMapper objectMapper;

  /**
   * Type holder to help easily identify simple types.
   */
  private SimpleTypeHolder simpleTypeHolder = new SimpleTypeHolder();

  /**
   * JSON factory for Jackson.
   */
  private JsonFactory factory = new JsonFactory();

  /**
   * Encode a {@link CouchbaseStorable} to a JSON string.
   *
   * @param source the source document to encode.
   *
   * @return the encoded JSON String.
   */
  @Override
  public final Object encode(final CouchbaseStorable source) {
    Writer writer = new StringWriter();

    try {
      JsonGenerator generator = factory.createGenerator(writer);
      encodeRecursive(source, generator);
      generator.close();
      writer.close();
    } catch (IOException ex) {
      throw new RuntimeException("Could not encode JSON", ex);
    }

    return writer.toString();
  }

  /**
   * Recursively iterates through the sources and adds it to the JSON generator.
   *
   * @param source the source document
   * @param generator the JSON generator.
   *
   * @throws IOException
   */
  private void encodeRecursive(final CouchbaseStorable source, final JsonGenerator generator) throws IOException {
    generator.writeStartObject();

    for (Map.Entry<String, Object> entry : ((CouchbaseDocument) source).export().entrySet()) {
      String key = entry.getKey();
      Object value = entry.getValue();
      generator.writeFieldName(key);
      if (value instanceof CouchbaseDocument) {
        encodeRecursive((CouchbaseDocument) value, generator);
        continue;
      }

      final Class<?> clazz = value.getClass();

      if (simpleTypeHolder.isSimpleType(clazz) && !isEnumOrClass(clazz)) {
        generator.writeObject(value);
      } else {
        objectMapper.writeValue(generator, value);
      }

    }

    generator.writeEndObject();
  }

  private boolean isEnumOrClass(final Class<?> clazz) {
    return Enum.class.isAssignableFrom(clazz) || Class.class.isAssignableFrom(clazz);
  }

  /**
   * Decode a JSON string into the {@link CouchbaseStorable} structure.
   *
   * @param source the source formatted document.
   * @param target the target of the populated data.
   *
   * @return the decoded structure.
   */
  @Override
  public final CouchbaseStorable decode(final Object source, final CouchbaseStorable target) {
    try {
      JsonParser parser = factory.createParser((String) source);
      while (parser.nextToken() != null) {
        JsonToken currentToken = parser.getCurrentToken();

        if (currentToken == JsonToken.START_OBJECT) {
          return decodeObject(parser, (CouchbaseDocument) target);
        } else if (currentToken == JsonToken.START_ARRAY) {
          return decodeArray(parser, new CouchbaseList());
        } else {
          throw new MappingException("JSON to decode needs to start as array or object!");
        }
      }
      parser.close();
    } catch (IOException ex) {
      throw new RuntimeException("Could not decode JSON", ex);
    }
    return target;
  }

  /**
   * Helper method to decode an object recursively.
   *
   * @param parser the JSON parser with the content.
   * @param target the target where the content should be stored.
   *
   * @throws IOException
   * @returns the decoded object.
   */
  private CouchbaseDocument decodeObject(final JsonParser parser, final CouchbaseDocument target) throws IOException {
    JsonToken currentToken = parser.nextToken();

    String fieldName = "";
    while (currentToken != null && currentToken != JsonToken.END_OBJECT) {
      if (currentToken == JsonToken.START_OBJECT) {
        target.put(fieldName, decodeObject(parser, new CouchbaseDocument()));
      } else if (currentToken == JsonToken.START_ARRAY) {
        target.put(fieldName, decodeArray(parser, new CouchbaseList()));
      } else if (currentToken == JsonToken.FIELD_NAME) {
        fieldName = parser.getCurrentName();
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
   *
   * @throws IOException
   * @returns the decoded list.
   */
  private CouchbaseList decodeArray(final JsonParser parser, final CouchbaseList target) throws IOException {
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
   * @param token the type of token.
   * @param parser the parser with the content.
   *
   * @return the decoded primitve.
   *
   * @throws IOException
   */
  private Object decodePrimitive(final JsonToken token, final JsonParser parser) throws IOException {
    switch (token) {
      case VALUE_TRUE:
      case VALUE_FALSE:
        return parser.getBooleanValue();
      case VALUE_STRING:
        return parser.getValueAsString();
      case VALUE_NUMBER_INT:
        return parser.getNumberValue();
      case VALUE_NUMBER_FLOAT:
        return parser.getDoubleValue();
      case VALUE_NULL:
        return null;
      default:
        throw new MappingException("Could not decode primitive value " + token);
    }
  }

  public void setObjectMapper(final ObjectMapper objectMapper) {
    this.objectMapper = objectMapper;
  }

  @Override
  public void afterPropertiesSet() {
    if (objectMapper == null) {
      objectMapper = new ObjectMapper();
    }
  }

}
