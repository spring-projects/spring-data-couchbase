package org.springframework.data.couchbase.core.mapping;

import java.lang.annotation.Documented;
import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

import org.springframework.core.annotation.AliasFor;

/**
 * Annotation to define custom metadata for document fields.
 *
 * @author Oliver Gierke
 * @author Christoph Strobl
 */
@Documented
@Retention(RetentionPolicy.RUNTIME)
@Target({ ElementType.FIELD, ElementType.METHOD, ElementType.ANNOTATION_TYPE })
public @interface Field {

  /**
   * The key to be used to store the field inside the document. Alias for {@link #name()}.
   *
   * @return an empty {@link String} by default.
   */
  @AliasFor("name")
  String value() default "";

  /**
   * The key to be used to store the field inside the document. Alias for {@link #value()}.
   *
   * @return an empty {@link String} by default.
   */
  @AliasFor("value")
  String name() default "";

  /**
   * The order in which various fields shall be stored. Has to be a positive integer.
   *
   * @return the order the field shall have in the document or -1 if undefined.
   */
  int order() default Integer.MAX_VALUE;

}
