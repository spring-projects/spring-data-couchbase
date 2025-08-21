package org.springframework.data.couchbase.config;
/*
 * Copyright (c) 2019 Couchbase, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */


// CHECKSTYLE:OFF IllegalImport - Allow unbundled Jackson

import com.couchbase.client.core.annotation.Stability;
import com.couchbase.client.core.encryption.CryptoManager;
import com.couchbase.client.core.error.DecodingFailureException;
import com.couchbase.client.core.error.EncodingFailureException;
import com.couchbase.client.java.codec.JsonSerializer;
import com.couchbase.client.java.codec.TypeRef;
import com.couchbase.client.java.encryption.annotation.Encrypted;
import com.couchbase.client.java.encryption.databind.jackson.EncryptionModule;
import com.couchbase.client.java.json.JsonObject;
import com.couchbase.client.java.json.JsonValueModule;
import com.fasterxml.jackson.annotation.JsonProperty;
import tools.jackson.core.JsonGenerator;
import tools.jackson.core.TokenStreamFactory;
import tools.jackson.core.json.JsonFactory;
import tools.jackson.core.util.JsonGeneratorDecorator;
import tools.jackson.databind.JavaType;
import tools.jackson.databind.ObjectMapper;
import tools.jackson.databind.cfg.MapperBuilder;
import tools.jackson.databind.cfg.MapperBuilderState;
import tools.jackson.databind.json.JsonMapper;

import static com.couchbase.client.core.logging.RedactableArgument.redactUser;
import static java.nio.charset.StandardCharsets.UTF_8;
import static java.util.Objects.requireNonNull;

/**
 * A serializer backed by a user-provided Jackson {@code ObjectMapper}.
 * <p>
 * In order to use this class you must add Jackson to your class path.
 * <p>
 * Make sure to register {@link JsonValueModule} with your {@code ObjectMapper}
 * so it can handle Couchbase {@link JsonObject} instances.
 * <p>
 * Likewise, if you're using the {@link Encrypted} annotation for
 * Couchbase Field-Level Encryption, make sure to register
 * {@link EncryptionModule}.
 * <p>
 * Example usage without Couchbase Field-Level Encryption:
 * <pre>
 * ObjectMapper mapper = new ObjectMapper();
 * mapper.registerModule(new JsonValueModule());
 *
 * ClusterEnvironment env = ClusterEnvironment.builder()
 *     .jsonSerializer(new JacksonJsonSerializer(mapper))
 *     .build();
 * </pre>
 * <p>
 * Example usage with Couchbase Field-Level Encryption:
 * <pre>
 * CryptoManager cryptoManager = ...
 *
 * ObjectMapper mapper = new ObjectMapper();
 * mapper.registerModule(new JsonValueModule());
 * mapper.registerModule(new EncryptionModule(cryptoManager));
 *
 * ClusterEnvironment env = ClusterEnvironment.builder()
 *     .cryptoManager(cryptoManager)
 *     .jsonSerializer(new JacksonJsonSerializer(mapper))
 *     .build();
 * </pre>
 *
 * @see JsonValueModule
 * @see EncryptionModule
 */
public class MyJacksonJsonSerializer implements JsonSerializer {
  private final ObjectMapper mapper;

  /**
   * Returns a new instance backed by the given ObjectMapper.
   *
   * @param mapper the custom ObjectMapper to use.
   * @return the Jackson JSON serializer with a custom object mapper.
   */
  public static MyJacksonJsonSerializer create(ObjectMapper mapper) {
    return new MyJacksonJsonSerializer(mapper);
  }

  /**
   * Returns a new instance backed by a default ObjectMapper without encryption support.
   *
   * @return the Jackson JSON serializer with the default object mapper.
   */
  public static MyJacksonJsonSerializer create() {
    return create((CryptoManager) null);
  }

  /**
   * Returns a new instance backed by a default ObjectMapper
   * with optional encryption support.
   *
   * @param cryptoManager (nullable) The manager to use for activating the
   * {@link Encrypted} annotation, or null to disable encryption support.
   @return the Jackson JSON serializer with a provided crypto manager.
   */
  public static MyJacksonJsonSerializer create(CryptoManager cryptoManager) {
    JsonMapper.Builder builder =  JsonMapper.builder();
    // builder.addModule(new JsonValueModule());
    if (cryptoManager != null) {
     //builder.addModule(new EncryptionModule(cryptoManager));
    }
    ObjectMapper mapper = builder.build();
    return new MyJacksonJsonSerializer(mapper);
  }

  private MyJacksonJsonSerializer(ObjectMapper mapper) {
    this.mapper = requireNonNull(mapper);
  }

  @Override
  public byte[] serialize(final Object input) {
    if (input instanceof byte[]) {
      return (byte[]) input;
    }

    try {
      return mapper.writeValueAsBytes(input);
    } catch (Throwable t) {
      throw new EncodingFailureException("Serializing of content + " + redactUser(input) + " to JSON failed.", t);
    }
  }

  @Override
  public <T> T deserialize(final Class<T> target, final byte[] input) {
    if (target.equals(byte[].class)) {
      return (T) input;
    }

    try {
      return mapper.readValue(input, target);
    } catch (Throwable e) {
      throw new DecodingFailureException("Deserialization of content into target " + target
        + " failed; encoded = " + redactUser(new String(input, UTF_8)), e);
    }
  }

  @Override
  public <T> T deserialize(final TypeRef<T> target, final byte[] input) {
    try {
      JavaType type = mapper.getTypeFactory().constructType(target.type());
      return mapper.readValue(input, type);
    } catch (Throwable e) {
      throw new DecodingFailureException("Deserialization of content into target " + target
        + " failed; encoded = " + redactUser(new String(input, UTF_8)), e);
    }
  }

  /**
   * Throws something if the user-provided Jackson library is absent or broken.
   *
   * @throws Throwable if JacksonJsonSerializer should not be used.
   * @implNote Looking for ObjectMapper on the class path is not sufficient;
   * we've seen at least one case where ObjectMapper was present but its
   * dependencies were missing.
   */
  @SuppressWarnings("RedundantThrows")
  @Stability.Internal
  public static void preflightCheck() throws Throwable {
    JsonSerializer serializer = com.couchbase.client.java.codec.JacksonJsonSerializer.create();
    com.couchbase.client.java.codec.JacksonJsonSerializer.PreflightCheckSubject serializeMe = new com.couchbase.client.java.codec.JacksonJsonSerializer.PreflightCheckSubject();
    serializeMe.name = "x";
    byte[] json = serializer.serialize(serializeMe);
    String expected = "{\"n\":\"x\"}";
    String actual = new String(json, UTF_8);
    if (!expected.equals(actual)) {
      throw new RuntimeException("Serialization failed; expected " + expected + " but got " + actual);
    }
    // could use PreflightCheckSubject.class instead of TypeRef, but let's test the exotic stuff
    com.couchbase.client.java.codec.JacksonJsonSerializer.PreflightCheckSubject deserialized = serializer.deserialize(new TypeRef<com.couchbase.client.java.codec.JacksonJsonSerializer.PreflightCheckSubject>() {
    }, json);
    if (!"x".equals(deserialized.name)) {
      throw new RuntimeException("Deserialization failed; expected 'x' but got '" + deserialized.name + "'");
    }
  }

  // public in case there's something about the runtime environment
  // that prevents Jackson from accessing private classes.
  @Stability.Internal
  public static class PreflightCheckSubject {
    @JsonProperty("n")
    public String name;
  }
}
