package org.springframework.data.couchbase.core.view;

import java.lang.annotation.Documented;
import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

/**
 * Annotation to support the use of Views with Couchbase.
 */
@Documented
@Target(ElementType.METHOD)
@Retention(RetentionPolicy.RUNTIME)
public @interface View {

  /**
   * The name of the Design document to use.
   *
   * This field is mandatory.
   *
   * @return name of the Design document.
   */
  String design();

  /**
   * The name of the View to use.
   *
   * This field is mandatory.
   *
   * @return name of the View
   */
  String view();

}
