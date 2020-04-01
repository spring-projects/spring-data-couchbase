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


	@Test
	void testNestedNotIn() {
		QueryCriteria c =
				where("name").is("Bubba").or(
				where("age").gt(12).or("country").is("Austria")).and(
				where("state").notin(new String[]{"Alabama","Florida"}));
		assertEquals("`name` = \"Bubba\" or (`age` > 12 or `country` = \"Austria\") and "+
				"(NOT( (`state` IN ( [ \"Alabama\", \"Florida\" ] )) ))",
				c.export());
	}

	/*
	public QueryCriteria child(QueryCriteria criteria) {
	public QueryCriteria eq(@Nullable Object o) {
	public QueryCriteria is(@Nullable Object o) {
	public QueryCriteria ne(@Nullable Object o) {
	public QueryCriteria lt(@Nullable Object o) {
	public QueryCriteria lte(@Nullable Object o) {
	public QueryCriteria gt(@Nullable Object o) {
	public QueryCriteria gte(@Nullable Object o) {
	public QueryCriteria startingWith(@Nullable Object o) {
	public QueryCriteria endingWith(@Nullable Object o) {
	public QueryCriteria regex(@Nullable Object o) {
	public QueryCriteria containing(@Nullable Object o) {
	public QueryCriteria notcontaining(@Nullable Object o) {
	public QueryCriteria like(@Nullable Object o) {
	public QueryCriteria notlike(@Nullable Object o) {
	public QueryCriteria isnull(@Nullable Object o) {
	public QueryCriteria isnotnull(@Nullable Object o) {
	public QueryCriteria ismissing(@Nullable Object o) {
	public QueryCriteria isnotmissing(@Nullable Object o) {
	public QueryCriteria isvalued(@Nullable Object o) {
	public QueryCriteria isnotvalued(@Nullable Object o) {
	x public QueryCriteria between(@Nullable Object o1, @Nullable Object o2) {
	x public QueryCriteria in(@Nullable Object[] o) {
	x public QueryCriteria notin(@Nullable Object[] o) {
	x public QueryCriteria TRUE(@Nullable Object[] o) { // true/false are reserved, use TRUE/FALSE
	x public QueryCriteria FALSE(@Nullable Object[] o) {
	 */

	@Test
	void testIsNull() {
		QueryCriteria c = where("name").isnull();
		assertEquals("`name` is null", c.export());
	}

	@Test
	void testIsNotNull() {
		QueryCriteria c = where("name").isnotnull();
		assertEquals("`name` is not null", c.export());
	}

	@Test
	void testIsMissing() {
		QueryCriteria c = where("name").ismissing();
		assertEquals("`name` is missing", c.export());
	}

	@Test
	void testIsNotMissing() {
		QueryCriteria c = where("name").isnotmissing();
		assertEquals("`name` is not missing", c.export());
	}

	@Test
	void testIsValued() {
		QueryCriteria c = where("name").isvalued();
		assertEquals("`name` is valued", c.export());
	}

	@Test
	void testIsNotValued() {
		QueryCriteria c = where("name").isnotvalued();
		assertEquals("`name` is not valued", c.export());
	}

	@Test
	void testBetween() {
		QueryCriteria c = where("name").between("Davis","Gump");
		assertEquals("`name` BETWEEN \"Davis\" AND \"Gump\"", c.export());
	}

	@Test
	void testIn() {
		QueryCriteria c = where("name").in(new String[]{"gump","davis"});
		assertEquals("`name` IN ( [ \"gump\", \"davis\" ] )", c.export());
	}

	@Test
	void testNotin() {
		QueryCriteria c = where("name").notin(new String[]{"gump","davis"});
		assertEquals("NOT( (`name` IN ( [ \"gump\", \"davis\" ] )) )", c.export());
	}

	@Test
	void testTrue() {
		QueryCriteria c = where("name").TRUE();
		assertEquals("`name`", c.export());
	}

	@Test
	void testFalse() {
		QueryCriteria c = where("name").FALSE();
		assertEquals("NOT( (`name`) )", c.export());
	}
}