/*
 * Copyright 2017-2019 the original author or authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      https://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.springframework.data.couchbase.core.query;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.*;
import static org.springframework.data.couchbase.core.query.QueryCriteria.*;

class QueryCriteriaTests {

	@Test
	void testSimpleCriteria() {
		QueryCriteria c = where("name").is("Bubba");
		assertEquals("`name` = \"Bubba\"", c.export());
	}

	@Test
	public void testNullValue() {
		QueryCriteria c = where("name").is(null);
		assertEquals("`name` = null", c.export());
	}

	@Test
	void testSimpleNumber() {
		QueryCriteria c = where("name").is(5);
		assertEquals("`name` = 5", c.export());
	}

	@Test
	void testNotEqualCriteria() {
		QueryCriteria c = where("name").ne("Bubba");
		assertEquals("`name` != \"Bubba\"", c.export());
	}

	@Test
	void testChainedCriteria() {
		QueryCriteria c = where("name").is("Bubba").and("age").lt(21).or("country").is("Austria");
		assertEquals("`name` = \"Bubba\" and `age` < 21 or `country` = \"Austria\"", c.export());
	}

	@Test
	void testNestedAndCriteria() {
		QueryCriteria c = where("name").is("Bubba").and(where("age").gt(12).or("country").is("Austria"));
		assertEquals("`name` = \"Bubba\" and (`age` > 12 or `country` = \"Austria\")", c.export());
	}

	@Test
	void testNestedOrCriteria() {
		QueryCriteria c = where("name").is("Bubba").or(where("age").gt(12).or("country").is("Austria"));
		assertEquals("`name` = \"Bubba\" or (`age` > 12 or `country` = \"Austria\")", c.export());
	}

}