package org.springframework.data.couchbase.core.mapping;

import java.util.Collections;
import java.util.HashSet;
import java.util.Set;

import org.springframework.data.mapping.model.SimpleTypeHolder;

public class CouchbaseDocumentSimpleTypes {

  static {
    Set<Class<?>> simpleTypes = new HashSet<Class<?>>();
    simpleTypes.add(CouchbaseDocument.class);
    simpleTypes.add(CouchbaseList.class);
    COUCHBASE_SIMPLE_TYPES = Collections.unmodifiableSet(simpleTypes);
  }

  private static final Set<Class<?>> COUCHBASE_SIMPLE_TYPES;
  public static final SimpleTypeHolder HOLDER = new SimpleTypeHolder(COUCHBASE_SIMPLE_TYPES, true);

  private CouchbaseDocumentSimpleTypes() {}

}
