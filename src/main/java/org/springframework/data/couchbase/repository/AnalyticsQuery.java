package org.springframework.data.couchbase.repository;

import com.couchbase.client.java.analytics.AnalyticsScanConsistency;
import com.couchbase.client.java.query.QueryScanConsistency;
import org.springframework.data.annotation.QueryAnnotation;

import java.lang.annotation.Documented;
import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

@Retention(RetentionPolicy.RUNTIME)
@Target({ ElementType.METHOD, ElementType.ANNOTATION_TYPE })
@Documented
@QueryAnnotation
public @interface AnalyticsQuery {

  /**
   * Allows to customize the scan consistency for the analytics query.
   *
   * @return
   */
  AnalyticsScanConsistency scanConsistency() default AnalyticsScanConsistency.NOT_BOUNDED;

}
