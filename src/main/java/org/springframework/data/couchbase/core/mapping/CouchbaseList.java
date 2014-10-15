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

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

/**
 * A {@link CouchbaseList} is an abstract list that represents an array stored in a (most of the times JSON) document.
 *
 * <p>This {@link CouchbaseList} is part of the potentially nested structure inside one or more
 * {@link CouchbaseDocument}s. It can also contain them recursively, depending on how the document is modeled.</p>
 */
public class CouchbaseList implements CouchbaseStorable {

  /**
   * Contains the actual data to be stored.
   */
  private List<Object> payload;

  /**
   * Holds types considered simple and allowed to be stored.
   */
  private SimpleTypeHolder simpleTypeHolder;

  /**
   * Create a new (empty) list.
   */
  public CouchbaseList() {
    this(new ArrayList<Object>());
  }

  /**
   * Create a new list with a given payload on construction.
   *
   * @param initialPayload the initial data to store.
   */
  public CouchbaseList(final List<Object> initialPayload) {
    this(initialPayload, null);
  }

  /**
   * Create a new (empty) list with an existing {@link SimpleTypeHolder}.
   *
   * @param simpleTypeHolder context instance.
   */
  public CouchbaseList(final SimpleTypeHolder simpleTypeHolder) {
    this(new ArrayList<Object>(), simpleTypeHolder);
  }

  /**
   * Create a new list with a given payload on construction and an existing {@link SimpleTypeHolder}.
   *
   * @param initialPayload the initial data to store.
   * @param simpleTypeHolder context instance.
   */
  public CouchbaseList(final List<Object> initialPayload, final SimpleTypeHolder simpleTypeHolder) {
    this.payload = initialPayload;
    Set<Class<?>> additionalTypes = new HashSet<Class<?>>();
    additionalTypes.add(CouchbaseDocument.class);
    additionalTypes.add(CouchbaseList.class);
    if (simpleTypeHolder != null) {
        this.simpleTypeHolder = new SimpleTypeHolder(additionalTypes, simpleTypeHolder);
    } else {
        this.simpleTypeHolder = new SimpleTypeHolder(additionalTypes, true);
    }
  }

  /**
   * Add content to the underlying list.
   *
   * @param value the value to be added.
   * @return the {@link CouchbaseList} object for chaining purposes.
   */
  public final CouchbaseList put(final Object value) {
    verifyValueType(value);

    payload.add(value);
    return this;
  }

  /**
   * Return the stored element at the given index.
   *
   * @param index the index where the document is located.
   * @return the found object (or null if nothing found).
   */
  public final Object get(final int index) {
    return payload.get(index);
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
    for (Object value : payload) {
      if (value instanceof CouchbaseDocument) {
        totalSize += ((CouchbaseDocument) value).size(true);
      } else if (value instanceof CouchbaseList) {
        totalSize += ((CouchbaseList) value).size(true);
      }
    }

    return totalSize;
  }

  /**
   * Returns the current payload, including all recursive elements.
   *
   * It either returns the raw results or makes sure that the recusrive elements
   * are also exported properly.
   *
   * @return
   */
  public final List<Object> export() {
    List<Object> toExport = new ArrayList<Object>(payload);

    int elem = 0;
    for (Object entry : payload) {
      if (entry instanceof CouchbaseDocument) {
        toExport.remove(elem);
        toExport.add(elem, ((CouchbaseDocument) entry).export());
      } else if (entry instanceof CouchbaseList) {
        toExport.remove(elem);
        toExport.add(elem, ((CouchbaseList) entry).export());
      }
      elem++;
    }
    return toExport;
  }

  /**
   * Returns true if it contains the given value.
   *
   * @param value the value to check for.
   * @return true if it contains the specified value.
   */
  public final boolean containsValue(final Object value) {
    return payload.contains(value);
  }

  /**
   * Checks if the underlying payload is empty or not.
   *
   * @return whether the underlying payload is empty or not.
   */
  public final boolean isEmpty() {
    return payload.isEmpty();
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

    throw new IllegalArgumentException("Attribute of type "
      + clazz.getCanonicalName() + "can not be stored and must be converted.");
  }

  /**
   * A string reprensation of the payload.
   *
   * @return the underlying payload as a string representation for easier debugging.
   */
  @Override
  public String toString() {
    return "CouchbaseList{" +
      "payload=" + payload +
      '}';
  }
}
