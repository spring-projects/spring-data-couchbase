/*
 * Copyright 2025-present the original author or authors
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *        https://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.springframework.data.couchbase.repository;

import java.lang.annotation.Documented;
import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

import org.springframework.data.annotation.QueryAnnotation;

/**
 * Annotation to define a Full-Text Search (FTS) query on a repository method.
 * <p>
 * The value is an FTS query string (as used by {@link com.couchbase.client.java.search.queries.QueryStringQuery}).
 * The FTS index name must be specified via {@link SearchIndex} on the method or entity class.
 * <p>
 * Supports positional parameter placeholders ({@code ?0}, {@code ?1}, etc.) which are replaced with method argument
 * values at execution time. Example:
 * <pre>
 * &#64;Search("description:pool AND city:\"San Francisco\"")
 * &#64;SearchIndex("hotel-search-index")
 * List&lt;Hotel&gt; findHotelsWithPool();
 *
 * &#64;Search("?0")
 * &#64;SearchIndex("hotel-search-index")
 * List&lt;Hotel&gt; searchHotels(String query);
 *
 * &#64;Search("city:?0 AND rating:>=?1")
 * &#64;SearchIndex("hotel-search-index")
 * List&lt;Hotel&gt; findByCityAndMinRating(String city, int minRating);
 * </pre>
 *
 * @author Emilien Bevierre
 * @since 6.2
 */
@Retention(RetentionPolicy.RUNTIME)
@Target({ ElementType.METHOD, ElementType.ANNOTATION_TYPE })
@Documented
@QueryAnnotation
public @interface Search {

	/**
	 * An FTS query string. Supports positional parameter placeholders ({@code ?0}, {@code ?1}, etc.)
	 * which will be replaced with the corresponding method argument values.
	 * This will be used to create a {@link com.couchbase.client.java.search.queries.QueryStringQuery}.
	 */
	String value();
}
