/*
 * Copyright 2013-2015 the original author or authors.
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

package org.springframework.data.couchbase.core.query;

import java.lang.annotation.Documented;
import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

/**
 * Annotation to support the use of Views with Couchbase.
 *
 * @author David Harrigan.
 * @author Simon Baslé
 */
@Documented
@Target(ElementType.METHOD)
@Retention(RetentionPolicy.RUNTIME)
//don't set @QueryAnnotation, as it causes problems with reduce and replacing count() method, the reduce detection needs to be improved
public @interface View {

  /**
   * The name of the Design Document to use. If omitted, defaults to one derived from the entity class name.
   *
   * @return name of the Design Document.
   */
  String designDocument() default "";

  /**
   * The name of the View to use. If omitted, defaults to one derived from the method name (stripped of prefix "find" or
   * "count"). This is mandatory to trigger a query derivation from the method name (ie. a View query with parameters
   * like limit, startkey, etc...).
   *
   * @return name of the View
   */
  String viewName() default "";

  boolean reduce() default false;

}
