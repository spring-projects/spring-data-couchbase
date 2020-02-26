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
