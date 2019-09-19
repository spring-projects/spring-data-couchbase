package org.springframework.data.couchbase.repository;

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
 * Using it without parameter will resolve the query from the method name. Providing a value (an inline N1QL statement)
 * will execute that statement instead.
 * <p/>
 * In this case, one can use a placeholder notation of {@code ?0}, {@code ?1} and so on.
 * <p/>
 * Also, SpEL in the form <code>#{spelExpression}</code> is supported, including the following N1QL variables that will
 * be replaced by the underlying {@link CouchbaseTemplate} associated information:
 * <ul>
 * <li>{@value StringN1qlBasedQuery#SPEL_SELECT_FROM_CLAUSE} (see {@link StringN1qlBasedQuery#SPEL_SELECT_FROM_CLAUSE})
 * </li>
 * <li>{@value StringN1qlBasedQuery#SPEL_BUCKET} (see {@link StringN1qlBasedQuery#SPEL_BUCKET})</li>
 * <li>{@value StringN1qlBasedQuery#SPEL_ENTITY} (see {@link StringN1qlBasedQuery#SPEL_ENTITY})</li>
 * <li>{@value StringN1qlBasedQuery#SPEL_FILTER} (see {@link StringN1qlBasedQuery#SPEL_FILTER})</li>
 * </ul>
 *
 * @author Simon Baslé.
 */
@Retention(RetentionPolicy.RUNTIME)
@Target({ ElementType.METHOD, ElementType.ANNOTATION_TYPE })
@Documented
@QueryAnnotation
public @interface Query {

	/**
	 * Takes a N1QL statement string to define the actual query to be executed. This one will take precedence over the
	 * method name.
	 */
	String value() default "";

}
