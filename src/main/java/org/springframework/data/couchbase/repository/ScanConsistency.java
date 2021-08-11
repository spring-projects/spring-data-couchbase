/*
 * Copyright 2012-2020 the original author or authors
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

import com.couchbase.client.java.analytics.AnalyticsScanConsistency;
import com.couchbase.client.java.query.QueryScanConsistency;

@Retention(RetentionPolicy.RUNTIME)
@Target({ ElementType.METHOD, ElementType.ANNOTATION_TYPE })
@Documented
public @interface ScanConsistency {

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

}
