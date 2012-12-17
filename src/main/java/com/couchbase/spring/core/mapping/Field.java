/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package com.couchbase.spring.core.mapping;

import java.lang.annotation.Documented;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;

/**
 * Annotation to define custom metadata for document fields.
 */
@Documented
@Retention(RetentionPolicy.RUNTIME)
public @interface Field {

  /**
   * The key to be used to store the field inside the document.
   */
  String value() default "";

}
