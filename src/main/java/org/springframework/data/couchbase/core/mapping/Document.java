/*
 * Copyright 2012-2023 the original author or authors
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

package org.springframework.data.couchbase.core.mapping;

import java.lang.annotation.ElementType;
import java.lang.annotation.Inherited;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;
import java.util.concurrent.TimeUnit;

import com.couchbase.client.core.msg.kv.DurabilityLevel;
import com.couchbase.client.java.kv.PersistTo;
import com.couchbase.client.java.kv.ReplicateTo;
import org.springframework.core.annotation.AliasFor;
import org.springframework.data.annotation.Persistent;
import org.springframework.data.couchbase.repository.Collection;
import org.springframework.data.couchbase.repository.ScanConsistency;
import org.springframework.data.couchbase.repository.Scope;

import com.couchbase.client.java.query.QueryScanConsistency;

/**
 * Identifies a domain object to be persisted to Couchbase.
 *
 * @author Michael Nitschinger
 * @author Andrey Rubtsov
 * @author Tigran Babloyan
 */
@Persistent
@Inherited
@Retention(RetentionPolicy.RUNTIME)
@Target({ ElementType.TYPE })
@Expiry
@ScanConsistency
public @interface Document {

	/**
	 * An optional expiry time for the document. Default is no expiry. Only one of two might might be set at the same
	 * time: either {@link #expiry()} or {@link #expiryExpression()}
	 */
	@AliasFor(annotation = Expiry.class, attribute = "expiry")
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
	@AliasFor(annotation = Expiry.class, attribute = "expiryExpression")
	String expiryExpression() default "";

	/**
	 * An optional time unit for the document's {@link #expiry()}, if set. Default is {@link TimeUnit#SECONDS}.
	 */
	@AliasFor(annotation = Expiry.class, attribute = "expiryUnit")
	TimeUnit expiryUnit() default TimeUnit.SECONDS;

	/**
	 * An optional flag associated with {@link #expiry()} indicating whether the expiry timer should be reset whenever the
	 * document is directly read (eg. findByOne, findById).
	 */
	boolean touchOnRead() default false;

	/**
	 * An optional string indicating the query scan consistency
	 */
	@AliasFor(annotation = ScanConsistency.class, attribute = "query")
	QueryScanConsistency queryScanConsistency() default QueryScanConsistency.NOT_BOUNDED;

	/**
	 * How many persisted copies of the modified record must exist on the given document. Default is {@link PersistTo#NONE}. 
	 * For Couchbase version >= 6.5 see {@link #durabilityLevel()}.
	 */
	PersistTo persistTo() default PersistTo.NONE;

	/**
	 * How many replicas must this documents operations be propagated to. Default is {@link ReplicateTo#NONE}.
	 * For Couchbase version >= 6.5 see {@link #durabilityLevel()}.
	 */
	ReplicateTo replicateTo() default ReplicateTo.NONE;

	/**
	 * The optional durabilityLevel for all mutating operations, allows the application to wait until this replication
	 * (or persistence) is successful before proceeding
	 */
	DurabilityLevel durabilityLevel() default DurabilityLevel.NONE;
}
