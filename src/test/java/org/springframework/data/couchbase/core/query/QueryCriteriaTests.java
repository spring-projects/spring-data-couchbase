package org.springframework.data.couchbase.core.query;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.*;

class QueryCriteriaTests {

	@Test
	void testSimpleCriteria() {
		QueryCriteria c = new QueryCriteria("name").is("Bubba");
		assertEquals("`name` = \"Bubba\"", c.export());
	}

	@Test
	public void testNullValue() {
		QueryCriteria c = new QueryCriteria("name").is(null);
		assertEquals("`name` = null", c.export());
	}

	@Test
	void testSimpleNumber() {
		QueryCriteria c = new QueryCriteria("name").is(5);
		assertEquals("`name` = 5", c.export());
	}

	@Test
	void testNotEqualCriteria() {
		QueryCriteria c = new QueryCriteria("name").ne("Bubba");
		assertEquals("`name` != \"Bubba\"", c.export());
	}

	@Test
	void testChainedCriteria() {
		QueryCriteria c = new QueryCriteria("name").is("Bubba").and("age").lt(21).or("country").is("Austria");
		assertEquals("`name` = \"Bubba\" and `age` < 21 or `country` = \"Austria\"", c.export());
	}

}