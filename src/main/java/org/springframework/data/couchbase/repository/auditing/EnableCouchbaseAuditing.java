/*
 * Copyright 2012-2025 the original author or authors
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

package org.springframework.data.couchbase.repository.auditing;

import java.lang.annotation.Documented;
import java.lang.annotation.ElementType;
import java.lang.annotation.Inherited;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

import org.springframework.context.annotation.Import;
import org.springframework.data.auditing.DateTimeProvider;
import org.springframework.data.domain.AuditorAware;

/**
 * Annotation to enable auditing in Couchbase via annotation configuration.
 *
 * @author Thomas Darimont
 * @author Oliver Gierke
 * @author Simon Baslé
 * @author Michael Reiche
 */
@Inherited
@Documented
@Target(ElementType.TYPE)
@Retention(RetentionPolicy.RUNTIME)
@Import(CouchbaseAuditingRegistrar.class)
public @interface EnableCouchbaseAuditing {
	/**
	 * Configures the {@link AuditorAware} bean to be used to lookup the current principal.
	 */
	String auditorAwareRef() default "";

	/**
	 * Configures whether the creation and modification dates are set. Defaults to {@literal true}.
	 */
	boolean setDates() default true;

	/**
	 * Configures whether the entity shall be marked as modified on creation. Defaults to {@literal true}.
	 */
	boolean modifyOnCreate() default true;

	/**
	 * Configures a {@link DateTimeProvider} bean name that allows customizing the {@link org.joda.time.DateTime} to be
	 * used for setting creation and modification dates.
	 */
	String dateTimeProviderRef() default "";
}
