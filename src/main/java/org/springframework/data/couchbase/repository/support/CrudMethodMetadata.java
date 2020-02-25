package org.springframework.data.couchbase.repository.support;

import java.lang.reflect.Method;

public interface CrudMethodMetadata {

  /**
   * Returns the {@link Method} to be used.
   *
   * @return
   * @since 1.9
   */
  Method getMethod();

}
