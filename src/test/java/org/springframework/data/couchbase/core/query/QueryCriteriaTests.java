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

import static org.junit.jupiter.api.Assertions.*;
import static org.springframework.data.couchbase.core.query.QueryCriteria.*;

import org.junit.jupiter.api.Test;

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

	@Test
	void testNestedNotIn() {
		QueryCriteria c = where("name").is("Bubba").or(where("age").gt(12).or("country").is("Austria"))
				.and(where("state").notIn(new String[] { "Alabama", "Florida" }));
		assertEquals("`name` = \"Bubba\" or (`age` > 12 or `country` = \"Austria\") and "
				+ "(not( (`state` in ( [ \"Alabama\", \"Florida\" ] )) ))", c.export());
	}

	@Test
	void testLt() {
		QueryCriteria c = where("name").lt("Couch");
		assertEquals("`name` < \"Couch\"", c.export());
	}

	@Test
	void testLte() {
		QueryCriteria c = where("name").lte("Couch");
		assertEquals("`name` <= \"Couch\"", c.export());
	}

	@Test
	void testGt() {
		QueryCriteria c = where("name").gt("Couch");
		assertEquals("`name` > \"Couch\"", c.export());
	}

	@Test
	void testGte() {
		QueryCriteria c = where("name").gte("Couch");
		assertEquals("`name` >= \"Couch\"", c.export());
	}

	@Test
	void testNe() {
		QueryCriteria c = where("name").ne("Couch");
		assertEquals("`name` != \"Couch\"", c.export());
	}

	@Test
	void testStartingWith() {
		QueryCriteria c = where("name").startingWith("Cou");
		assertEquals("`name` like \"Cou%\"", c.export());
	}

	@Test
	void testStartingWithExpr() {
		QueryCriteria c = where("name").startingWith(where("name").plus(""));
		assertEquals("`name` like ((\"%\" + ((`name` + \"\"))))", c.export());
	}

	@Test
	void testEndingWith() {
		QueryCriteria c = where("name").endingWith("ouch");
		assertEquals("`name` like \"%ouch\"", c.export());
	}

	@Test
	void testRegex() {
		QueryCriteria c = where("name").regex("C.*h");
		assertEquals("regexp_like(`name`, \"C.*h\")", c.export());
	}

	@Test
	void testContaining() {
		QueryCriteria c = where("name").containing("ouch");
		assertEquals("contains(`name`, \"ouch\")", c.export());
	}

	@Test
	void testNotContaining() {
		QueryCriteria c = where("name").notContaining("Elvis");
		assertEquals("not( (contains(`name`, \"Elvis\")) )", c.export());
	}

	@Test
	void testLike() {
		QueryCriteria c = where("name").like("%ouch%");
		assertEquals("`name` like \"%ouch%\"", c.export());
	}

	@Test
	void testNotLike() {
		QueryCriteria c = where("name").notLike("%Elvis%");
		assertEquals("not( (`name` like \"%Elvis%\") )", c.export());
	}

	@Test
	void testIsNull() {
		QueryCriteria c = where("name").isNull();
		assertEquals("`name` is null", c.export());
	}

	@Test
	void testIsNotNull() {
		QueryCriteria c = where("name").isNotNull();
		assertEquals("`name` is not null", c.export());
	}

	@Test
	void testIsMissing() {
		QueryCriteria c = where("name").isMissing();
		assertEquals("`name` is missing", c.export());
	}

	@Test
	void testIsNotMissing() {
		QueryCriteria c = where("name").isNotMissing();
		assertEquals("`name` is not missing", c.export());
	}

	@Test
	void testIsValued() {
		QueryCriteria c = where("name").isValued();
		assertEquals("`name` is valued", c.export());
	}

	@Test
	void testIsNotValued() {
		QueryCriteria c = where("name").isNotValued();
		assertEquals("`name` is not valued", c.export());
	}

	@Test
	void testBetween() {
		QueryCriteria c = where("name").between("Davis", "Gump");
		assertEquals("`name` between \"Davis\" and \"Gump\"", c.export());
	}

	@Test
	void testIn() {
		QueryCriteria c = where("name").in(new String[] { "gump", "davis" });
		assertEquals("`name` in ( [ \"gump\", \"davis\" ] )", c.export());
	}

	@Test
	void testNotIn() {
		QueryCriteria c = where("name").notIn(new String[] { "gump", "davis" });
		assertEquals("not( (`name` in ( [ \"gump\", \"davis\" ] )) )", c.export());
	}

	@Test
	void testTrue() {
		QueryCriteria c = where("name").TRUE();
		assertEquals("`name`", c.export());
	}

	@Test
	void testFalse() {
		QueryCriteria c = where("name").FALSE();
		assertEquals("not( (`name`) )", c.export());
	}
}
