/*
 * Copyright 2018-present the original author or authors
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

package org.springframework.data.couchbase.core.query;

import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

/**
 * This annotation is targeted for entity field which is a list of the associated entities fetched by ANSI Join across
 * the entities available from Couchbase Server 5.5
 *
 * @author Subhashni Balakrishnan
 */
@Target({ ElementType.FIELD })
@Retention(RetentionPolicy.RUNTIME)
public @interface N1qlJoin {
	/**
	 * Join Criteria can be a simple equi join or multiple conditions combined using AND or OR. Array based equi joins
	 * with unnest is also possible. To reference fields in entity use prefix "lks." (left key space) and for referencing
	 * fields in associated entities use "rks." (right key space)
	 */
	String on();

	/**
	 * Fetch type specifies how the associated entities are fetched {@link FetchType}
	 */
	FetchType fetchType() default FetchType.IMMEDIATE;

	/**
	 * Where clause for the join. To reference fields in entity use prefix "lks." and for referencing fields in associated
	 * entities use "rks."
	 */
	String where() default "";

	/**
	 * Hint index for entity for indexed nested loop join
	 */
	String index() default "";

	/**
	 * Hint index for associated entity for indexed nested loop join
	 */
	String rightIndex() default "";

	/**
	 * Hash side specification for the associated entity for hash join Note: Supported on enterprise edition only
	 */
	HashSide hashside() default HashSide.NONE;

	/**
	 * Use keys query hint
	 */
	String[] keys() default {};
}
