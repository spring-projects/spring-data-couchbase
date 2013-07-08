/*
 * Copyright 2013 the original author or authors.
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

import com.fasterxml.jackson.core.*;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.springframework.data.couchbase.core.mapping.CouchbaseDocument;
import org.springframework.data.couchbase.core.mapping.CouchbaseList;
import org.springframework.data.couchbase.core.mapping.CouchbaseStorable;
import org.springframework.data.mapping.model.MappingException;
import org.springframework.data.mapping.model.SimpleTypeHolder;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.util.HashMap;
import java.util.Map;

/**
 * A Jackson JSON Translator.
 *
 * @author Michael Nitschinger
 */
public class JacksonTranslationService implements TranslationService {

  private SimpleTypeHolder simpleTypeHolder = new SimpleTypeHolder();
  private JsonFactory factory = new JsonFactory();

  @Override
  public final Object encode(final CouchbaseStorable source) {
    OutputStream stream = new ByteArrayOutputStream();

    try {
      JsonGenerator generator = factory.createGenerator(stream, JsonEncoding.UTF8);
      encodeRecursive(source, generator);
      generator.close();
    } catch (IOException ex) {
      throw new RuntimeException("Could not encode JSON", ex);
    }

    return stream.toString();
  }

  /**
   * Recursively iterates through the sources and adds it to the JSON generator.
   *
   * @param source the source document
   * @param generator the JSON generator.
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

      if (simpleTypeHolder.isSimpleType(value.getClass())) {
        generator.writeObject(value);
      } else {
        ObjectMapper mapper = new ObjectMapper();
        mapper.writeValue(generator, value);
      }

    }

    generator.writeEndObject();
  }

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

  private Object decodePrimitive(final JsonToken token, final JsonParser parser) throws IOException {
    switch (token) {
      case VALUE_TRUE:
      case VALUE_FALSE:
        return parser.getValueAsBoolean();
      case VALUE_STRING:
        return parser.getValueAsString();
      case VALUE_NUMBER_INT:
        return parser.getValueAsInt();
      case VALUE_NUMBER_FLOAT:
        return parser.getValueAsDouble();
      default:
        throw new MappingException("Could not decode primitve value " + token);
    }
  }


}
