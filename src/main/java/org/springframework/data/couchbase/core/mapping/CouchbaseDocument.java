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

package org.springframework.data.couchbase.core.mapping;

import org.springframework.data.mapping.model.SimpleTypeHolder;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

/**
 * A {@link CouchbaseDocument} is an abstract representation of a document stored inside Couchbase Server.
 *
 * <p>It acts like a {@link HashMap}, but only allows those types to be written that are supported by the underlying
 * storage format, which is currently JSON. Note that JSON conversion is not happening here, but performed at a
 * different stage based on the payload stored in the {@link CouchbaseDocument}.</p>
 *
 * <p>In addition to the actual content, meta data is also stored. This especially refers to the document ID and its
 * expiration time. Note that this information is not mandatory, since documents can be nested and therefore only the
 * topmost document most likely has an ID.</p>
 *
 * @author Michael Nitschinger
 */
public class CouchbaseDocument implements CouchbaseStorable {

  /**
   * Defnes the default expiration time for the document.
   */
  public static final int DEFAULT_EXPIRATION_TIME = 0;

  /**
   * Contains the actual data to be stored.
   */
  private HashMap<String, Object> payload;

  /**
   * Represents the document ID used to identify the document in the bucket.
   */
  private String id;

  /**
   * Contains the expiration time of the document.
   */
  private int expiration;

  /**
   * Holds types considered simple and allowed to be stored.
   */
  private SimpleTypeHolder simpleTypeHolder;

  /**
   * Creates a completely empty {@link CouchbaseDocument}.
   */
  public CouchbaseDocument() {
    this(null);
  }

  /**
   * Creates a empty {@link CouchbaseDocument}, and set the ID immediately.
   *
   * @param id the document ID.
   */
  public CouchbaseDocument(final String id) {
    this(id, DEFAULT_EXPIRATION_TIME);
  }

  /**
   * Creates a empty {@link CouchbaseDocument} with ID and expiration time.
   *
   * @param id the document ID.
   * @param expiration the expiration time of the document.
   */
  public CouchbaseDocument(final String id, final int expiration) {
    this.id = id;
    this.expiration = expiration;
    payload = new HashMap<String, Object>();

    Set<Class<?>> additionalTypes = new HashSet<Class<?>>();
    additionalTypes.add(CouchbaseDocument.class);
    additionalTypes.add(CouchbaseList.class);
    simpleTypeHolder = new SimpleTypeHolder(additionalTypes, true);
  }

  /**
   * Store a value with the given key for later retreival.
   *
   * @param key the key of the attribute.
   * @param value the actual content to be stored.
   * @return the {@link CouchbaseDocument} for chaining.
   */
  public final CouchbaseDocument put(final String key, final Object value) {
    verifyValueType(value);

    payload.put(key, value);
    return this;
  }

  /**
   * Potentially get a value from the payload with the given key.
   *
   * @param key the key of the attribute.
   * @return the value to which the specified key is mapped, or
   *         null if does not contain a mapping for the key.
   */
  public final Object get(final String key) {
    return payload.get(key);
  }

  /**
   * Returns the current payload, including all recursive elements.
   *
   * It either returns the raw results or makes sure that the recusrive elements
   * are also exported properly.
   *
   * @return
   */
  public final HashMap<String, Object> export() {
    HashMap<String, Object> toExport = new HashMap<String, Object>(payload);
    for (Map.Entry<String, Object> entry : payload.entrySet()) {
      if (entry.getValue() instanceof CouchbaseDocument) {
        toExport.put(entry.getKey(), ((CouchbaseDocument) entry.getValue()).export());
      } else if (entry.getValue() instanceof CouchbaseList) {
        toExport.put(entry.getKey(), ((CouchbaseList) entry.getValue()).export());
      }
    }
    return toExport;
  }

  /**
   * Returns true if it contains a payload for the specified key.
   *
   * @param key the key of the attribute.
   * @return true if it contains a payload for the specified key.
   */
  public final boolean containsKey(final String key) {
    return payload.containsKey(key);
  }

  /**
   * Returns true if it contains the given value.
   *
   * @param value the value to check for.
   * @return true if it contains the specified value.
   */
  public final boolean containsValue(final Object value) {
    return payload.containsValue(value);
  }

  /**
   * Returns the size of the attributes in this document (not nested).
   *
   * @return the size of the attributes in this document (not nested).
   */
  public final int size() {
    return size(false);
  }

  /**
   * Retruns the size of the attributes in this and recursive documents.
   *
   * @param recursive wheter nested attributes should be taken into account.
   * @return the size of the attributes in this and recursive documents.
   */
  public final int size(final boolean recursive) {
    int thisSize = payload.size();

    if (!recursive || thisSize == 0) {
      return thisSize;
    }

    int totalSize = thisSize;
    for (Object value : payload.values()) {
      if (value instanceof CouchbaseDocument) {
        totalSize += ((CouchbaseDocument) value).size(true);
      } else if (value instanceof CouchbaseList) {
        totalSize += ((CouchbaseList) value).size(true);
      }
    }

    return totalSize;
  }

  /**
   * Returns the underlying payload.
   *
   * <p>Note that unlike {@link #export()}, the nested objects are not converted, so the "raw" map is returned.</p>
   *
   * @return the underlying payload.
   */
  public HashMap<String, Object> getPayload() {
    return payload;
  }

  /**
   * Returns the expiration time of the document.
   *
   * If the expiration time is 0, then the document will be persisted until
   * deleted manually ("forever").
   *
   * @return the expiration time of the document.
   */
  public int getExpiration() {
    return expiration;
  }

  /**
   * Set the expiration time of the document.
   *
   * If the expiration time is 0, then the document will be persisted until
   * deleted manually ("forever").
   *
   * @param expiration
   * @return the {@link CouchbaseDocument} for chaining.
   */
  public CouchbaseDocument setExpiration(int expiration) {
    this.expiration = expiration;
    return this;
  }

  /**
   * Returns the ID of the document.
   *
   * @return the ID of the document.
   */
  public String getId() {
    return id;
  }

  /**
   * Sets the unique ID of the document per bucket.
   *
   * @param id the ID of the document.
   * @return the {@link CouchbaseDocument} for chaining.
   */
  public CouchbaseDocument setId(String id) {
    this.id = id;
    return this;
  }

  /**
   * Verifies that only values of a certain and supported type
   * can be stored.
   *
   * <p>If this is not the case, a {@link IllegalArgumentException} is
   * thrown.</p>
   *
   * @param value the object to verify its type.
   */
  private void verifyValueType(final Object value) {
    if(value == null) {
      return;
    }
    final Class<?> clazz = value.getClass();
    if (simpleTypeHolder.isSimpleType(clazz)) {
        return;
    }
    throw new IllegalArgumentException("Attribute of type " + clazz.getCanonicalName() + " cannot be stored and must be converted.");
  }

  /**
   * A string representation of expiration, id and payload.
   *
   * @return the string representation of the object.
   */
  @Override
  public String toString() {
    return "CouchbaseDocument{" +
      "id=" + id +
      ", exp=" + expiration +
      ", payload=" + payload +
      '}';
  }
}
