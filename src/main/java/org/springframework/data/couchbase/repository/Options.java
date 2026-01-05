/*
 * Copyright 2012-present the original author or authors
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

import static com.couchbase.client.core.io.CollectionIdentifier.DEFAULT_COLLECTION;
import static com.couchbase.client.core.io.CollectionIdentifier.DEFAULT_SCOPE;

import java.lang.annotation.Documented;
import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;
import java.util.concurrent.TimeUnit;

import org.springframework.data.annotation.QueryAnnotation;

import com.couchbase.client.java.analytics.AnalyticsScanConsistency;
import com.couchbase.client.java.query.QueryScanConsistency;

/**
 * Scope Annotation
 *
 * @author Michael Reiche
 */
@Retention(RetentionPolicy.RUNTIME)
@Target({ ElementType.METHOD, ElementType.ANNOTATION_TYPE, ElementType.TYPE })
@Documented
@QueryAnnotation
public @interface Options {

	/**
	 * Specifies the scope name
	 *
	 * @return the scope name configured, defaults to not DEFAULT_SCOPE.
	 */
	String scope() default DEFAULT_SCOPE;

	/**
	 * Specifies the scope name
	 *
	 * @return the scope name configured, defaults to not DEFAULT_SCOPE.
	 */
	String collection() default DEFAULT_COLLECTION;

	/**
	 * Specifies a custom scan consistency for N1QL queries.
	 *
	 * @return the scan consistency configured, defaults to not bounded.
	 */
	QueryScanConsistency query() default QueryScanConsistency.NOT_BOUNDED;

	/**
	 * Specifies a custom scan consistency for analytics queries.
	 *
	 * @return the scan consistency configured, defaults to not bounded.
	 */
	AnalyticsScanConsistency analytics() default AnalyticsScanConsistency.NOT_BOUNDED;

	/**
	 * Specifies a custom projection.
	 *
	 * @return the projection configured, defaults to an empty array (project everything).
	 */
	String[] project() default {};

	/**
	 * Specifies a custom array of distinct fields.
	 *
	 * @return the projection configured, we need to do something tricky with the default. We need to default to<br>
	 *         no distinct, which is specified by a null array (an empty array means distinct on everything). We'll make
	 *         an array of a single element "-" mean no distinct.
	 */
	String[] distinct() default { "-" };

	/**
	 * An optional expiry time for the document. Default is no expiry. Only one of two might might be set at the same
	 * time: either {@link #expiry()} or {@link #expiryExpression()}
	 */
	int expiry() default 0;

	/**
	 * Same as {@link #expiry} but allows the actual value to be set using standard Spring property sources mechanism.
	 * Only one might be set at the same time: either {@link #expiry()} or {@link #expiryExpression()}. <br />
	 * Syntax is the same as for {@link org.springframework.core.env.Environment#resolveRequiredPlaceholders(String)}.
	 * <br />
	 * <br />
	 * The value will be recalculated for every {@link org.springframework.data.couchbase.core.CouchbaseTemplate}
	 * save/insert/update call, thus allowing actual expiration to reflect changes on-the-fly as soon as property sources
	 * change. <br />
	 * <br />
	 * SpEL is NOT supported.
	 */
	String expiryExpression() default "";

	/**
	 * An optional time unit for the document's {@link #expiry()}, if set. Default is {@link TimeUnit#SECONDS}.
	 */
	TimeUnit expiryUnit() default TimeUnit.SECONDS;

	/**
	 * An timeout for the operation. Default is no timeout.
	 */
	long timeoutMs() default 0;
}
