package org.springframework.data.couchbase.core.mapping;

import org.springframework.data.mapping.model.SimpleTypeHolder;

import java.util.*;

/**
 * A {@link CouchbaseList} is an abstract list that represents an array stored
 * in a (most of the times JSON) document.
 *
 * This {@link CouchbaseList} is part of the potentially nested structure inside
 * one or more {@link CouchbaseDocument}s. It can also contain them recursively,
 * depending on how the document is modeled.
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

  public CouchbaseList() {
    this(new ArrayList<Object>());
  }

  public CouchbaseList(List<Object> initialPayload) {
    payload = initialPayload;

    Set<Class<?>> additionalTypes = new HashSet<Class<?>>();
    additionalTypes.add(CouchbaseDocument.class);
    additionalTypes.add(CouchbaseList.class);
    simpleTypeHolder = new SimpleTypeHolder(additionalTypes, true);
  }

  public final CouchbaseList put(Object value) {
    verifyValueType(value.getClass());

    payload.add(value);
    return this;
  }

  public final Object get(int index) {
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
   * @param clazz the class type to check and verify.
   */
  private void verifyValueType(final Class<?> clazz) {
    if (simpleTypeHolder.isSimpleType(clazz)) {
      return;
    }

    throw new IllegalArgumentException("Attribute of type "
      + clazz.getCanonicalName() + "can not be stored and must be converted.");
  }

  @Override
  public String toString() {
    return "CouchbaseList{" +
      "payload=" + payload +
      '}';
  }
}
