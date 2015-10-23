/*
 * Copyright 2012-2015 the original author or authors
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *        http://www.apache.org/licenses/LICENSE-2.0
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

import org.springframework.data.annotation.QueryAnnotation;
import org.springframework.data.couchbase.core.CouchbaseTemplate;
import org.springframework.data.couchbase.repository.query.StringN1qlBasedQuery;

/**
 * Annotation to support the use of N1QL queries with Couchbase.
 * <p/>
 * Using it without parameter will resolve the query from the method name. Providing a value
 * (an inline N1QL statement) will execute that statement instead.
 * <p/>
 * In this case, one can use a placeholder notation of {@code ?0}, {@code ?1} and so on.
 * <p/>
 * Also, SpEL in the form <code>${spelExpression}</code> is supported, including the
 * following N1QL variables that will be replaced by the underlying {@link CouchbaseTemplate}
 * associated information:
 * <ul>
 *  <li>
 *    {@value StringN1qlBasedQuery#SPEL_SELECT_FROM_CLAUSE}
 *    (see {@link StringN1qlBasedQuery#SPEL_SELECT_FROM_CLAUSE})
 *  </li>
 *  <li>
 *    {@value StringN1qlBasedQuery#SPEL_BUCKET}
 *    (see {@link StringN1qlBasedQuery#SPEL_BUCKET})
 *  </li>
 *  <li>
 *    {@value StringN1qlBasedQuery#SPEL_ENTITY}
 *    (see {@link StringN1qlBasedQuery#SPEL_ENTITY})
 *  </li>
 *  <li>
 *    {@value StringN1qlBasedQuery#SPEL_FILTER}
 *    (see {@link StringN1qlBasedQuery#SPEL_FILTER})
 *  </li>
 * </ul>
 *
 * @author Simon Baslé.
 */
@Documented
@Target(ElementType.METHOD)
@Retention(RetentionPolicy.RUNTIME)
@QueryAnnotation
public @interface Query {

  /**
   * Takes a N1QL statement string to define the actual query to be executed. This one will take precedence over the
   * method name then.
   */
  String value() default "";

}
