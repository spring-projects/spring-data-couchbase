/*
 * Copyright 2017-2023 the original author or authors.
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

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.springframework.data.couchbase.core.query.N1QLExpression.i;
import static org.springframework.data.couchbase.core.query.N1QLExpression.meta;
import static org.springframework.data.couchbase.core.query.N1QLExpression.path;
import static org.springframework.data.couchbase.core.query.N1QLExpression.x;
import static org.springframework.data.couchbase.core.query.QueryCriteria.where;
import static org.springframework.data.couchbase.repository.query.support.N1qlUtils.escapedBucket;

import java.math.BigInteger;
import java.util.Arrays;

import org.junit.jupiter.api.Test;

import com.couchbase.client.java.json.JsonArray;
import org.springframework.data.couchbase.core.convert.CouchbaseCustomConversions;
import org.springframework.data.couchbase.core.convert.MappingCouchbaseConverter;
import org.springframework.data.couchbase.core.mapping.CouchbaseMappingContext;
import org.springframework.data.couchbase.domain.Config;

/**
 * @author Mauro Monti
 * @author Michael Reiche
 */
class QueryCriteriaTests {

	@Test
	void testSimpleCriteria() {
		QueryCriteria c = where(i("name")).is("Bubba");
		assertEquals("`name` = \"Bubba\"", c.export());
	}

	@Test
	public void testNullValue() {
		QueryCriteria c = where(i("name")).is(null);
		assertEquals("`name` = null", c.export());
	}

	@Test
	void testSimpleNumber() {
		QueryCriteria c = where(i("name")).is(5);
		assertEquals("`name` = 5", c.export());
	}

	@Test
	void testNotEqualCriteria() {
		QueryCriteria c = where(i("name")).ne("Bubba");
		assertEquals("`name` != \"Bubba\"", c.export());
	}

	@Test
	void testChainedCriteria() {
		QueryCriteria c = where(i("name")).is("Bubba").and(i("age")).lt(21).or(i("country")).is("Austria");
		assertEquals("`name` = \"Bubba\" and `age` < 21 or `country` = \"Austria\"", c.export());
	}

	@Test
	void testNestedAndCriteria() {
		QueryCriteria c = where(i("name")).is("Bubba").and(where(i("age")).gt(12).or(i("country")).is("Austria"));
		JsonArray parameters = JsonArray.create();
		assertEquals("  (`name` = $1) and   (`age` > $2 or `country` = $3)", c.export(new int[1], parameters, null));
		assertEquals("[\"Bubba\",12,\"Austria\"]", parameters.toString());
	}

	@Test
	void testNestedOrCriteria() {
		QueryCriteria c = where(i("name")).is("Bubba").or(where(i("age")).gt(12).or(i("country")).is("Austria"));
		JsonArray parameters = JsonArray.create();
		assertEquals("  (`name` = $1) or   (`age` > $2 or `country` = $3)", c.export(new int[1], parameters, null));
		assertEquals("[\"Bubba\",12,\"Austria\"]", parameters.toString());
	}

	@Test
	void testNestedNotIn() {
		QueryCriteria c = where(i("name")).is("Bubba").or(where(i("age")).gt(12).and(i("country")).is("Austria"))
				.and(where(i("state")).notIn(new String[] { "Alabama", "Florida" }));
		JsonArray parameters = JsonArray.create();
		assertEquals("  (  (`name` = $1) or   (`age` > $2 and `country` = $3)) and   (not( (`state` in $4) ))",
				c.export(new int[1], parameters, null));
	}

	@Test
	void testNestedNotIn2() {
		QueryCriteria c = where(i("name")).is("Bubba").or(where(i("age")).gt(12)).and(where(i("state")).eq("1"));
		JsonArray parameters = JsonArray.create();
		assertEquals("  (  (`name` = $1) or   (`age` > $2)) and   (`state` = $3)", c.export(new int[1], parameters, null));
	}

	@Test
	void testNestedNotIn3() {
		QueryCriteria c = where(i("name")).is("Bubba").or(where(i("age")).gt(12)).and(i("state")).eq("1");
		JsonArray parameters = JsonArray.create();
		assertEquals("  (`name` = $1) or   (`age` > $2) and `state` = $3", c.export(new int[1], parameters, null));
	}

	@Test
	void testLt() {
		QueryCriteria c = where(i("name")).lt("Couch");
		assertEquals("`name` < \"Couch\"", c.export());
	}

	@Test
	void testLte() {
		QueryCriteria c = where(i("name")).lte("Couch");
		assertEquals("`name` <= \"Couch\"", c.export());
	}

	@Test
	void testGt() {
		QueryCriteria c = where(i("name")).gt("Couch");
		assertEquals("`name` > \"Couch\"", c.export());
	}

	@Test
	void testGte() {
		QueryCriteria c = where(i("name")).gte("Couch");
		assertEquals("`name` >= \"Couch\"", c.export());
	}

	@Test
	void testNe() {
		QueryCriteria c = where(i("name")).ne("Couch");
		assertEquals("`name` != \"Couch\"", c.export());
	}

	@Test
	void testStartingWith() {
		QueryCriteria c = where(i("name")).startingWith("Cou");
		assertEquals("`name` like (\"Cou\"||\"%\")", c.export());
	}

	@Test
	void testStartingWithExpr() {
		QueryCriteria c = where(i("name")).startingWith(where(i("name")).plus("xxx"));
		assertEquals("`name` like (((`name` || \"xxx\"))||\"%\")", c.export());
	}

	@Test
	void testEndingWith() {
		QueryCriteria c = where(i("name")).endingWith("ouch");
		assertEquals("`name` like (\"%\"||\"ouch\")", c.export());
	}

	@Test
	void testEndingWithExpr() {
		QueryCriteria c = where(i("name")).endingWith(where(i("name")).plus("xxx"));
		assertEquals("`name` like (\"%\"||((`name` || \"xxx\")))", c.export());
	}

	@Test
	void testRegex() {
		QueryCriteria c = where(i("name")).regex("C.*h");
		assertEquals("regexp_like(`name`, \"C.*h\")", c.export());
	}

	@Test
	void testContaining() {
		QueryCriteria c = where(i("name")).containing("ouch");
		assertEquals("contains(`name`, \"ouch\")", c.export());
	}

	@Test
	void testNotContaining() {
		QueryCriteria c = where(i("name")).notContaining("Elvis");
		assertEquals("not (contains(`name`, \"Elvis\"))", c.export());
	}

	@Test
	void testArrayContaining() {
		QueryCriteria c = where(i("name")).arrayContaining("Elvis");
		assertEquals("array_contains(`name`, \"Elvis\")", c.export());
	}

	@Test
	void testLike() {
		QueryCriteria c = where(i("name")).like("%ouch%");
		assertEquals("`name` like \"%ouch%\"", c.export());
	}

	@Test
	void testNotLike() {
		QueryCriteria c = where(i("name")).notLike("%Elvis%");
		assertEquals("not( (  (`name` like \"%Elvis%\")) )", c.export());
	}

	@Test
	void testIsNull() {
		QueryCriteria c = where(i("name")).isNull();
		assertEquals("`name` is null", c.export());
	}

	@Test
	void testIsNotNull() {
		QueryCriteria c = where(i("name")).isNotNull();
		assertEquals("`name` is not null", c.export());
	}

	@Test
	void testIsMissing() {
		QueryCriteria c = where(i("name")).isMissing();
		assertEquals("`name` is missing", c.export());
	}

	@Test
	void testIsNotMissing() {
		QueryCriteria c = where(i("name")).isNotMissing();
		assertEquals("`name` is not missing", c.export());
	}

	@Test
	void testIsValued() {
		QueryCriteria c = where(i("name")).isValued();
		assertEquals("`name` is valued", c.export());
	}

	@Test
	void testIsNotValued() {
		QueryCriteria c = where(i("name")).isNotValued();
		assertEquals("`name` is not valued", c.export());
	}

	@Test
	void testBetween() {
		QueryCriteria c = where(i("name")).between("Davis", "Gump");
		assertEquals("`name` between \"Davis\" and \"Gump\"", c.export());
		JsonArray parameters = JsonArray.create();
		assertEquals("`name` between $1 and $2", c.export(new int[1], parameters, converter));
		assertEquals("Davis", parameters.get(0).toString());
		assertEquals("Gump", parameters.get(1).toString());
	}

	@Test
	void testIn() {
		String[] args = new String[] { "gump", "davis" };
		QueryCriteria c = where(i("name")).in((Object) args); // the first arg is an array
		assertEquals("`name` in [\"gump\",\"davis\"]", c.export());
		JsonArray parameters = JsonArray.create();
		assertEquals("`name` in $1", c.export(new int[1], parameters, converter));
		assertEquals(arrayToString(args), parameters.get(0).toString());
	}

	@Test
	void testInInteger() {
		Integer[] args = new Integer[]{1, 2};
		QueryCriteria c = where(i("name")).in((Object) args); // the first arg is an array
		assertEquals("`name` in [1,2]", c.export());
		JsonArray parameters = JsonArray.create();
		assertEquals("`name` in $1", c.export(new int[1], parameters, converter));
		assertEquals(arrayToString(args), parameters.get(0).toString());
	}

	@Test
	void testInBigBoolean() {
		Boolean[] args = new Boolean[]{true, false};
		QueryCriteria c = where(i("name")).in((Object) args); // the first arg is an array
		assertEquals("`name` in ["+true+","+false+"]", c.export());
		JsonArray parameters = JsonArray.create();
		assertEquals("`name` in $1", c.export(new int[1], parameters, converter));
		assertEquals(arrayToString(args), parameters.get(0).toString());
	}

	@Test
	void testInBigInteger() {
		BigInteger[] args = new BigInteger[]{BigInteger.TEN, BigInteger.ONE};
		QueryCriteria c = where(i("name")).in((Object) args); // the first arg is an array
		assertEquals("`name` in ["+BigInteger.TEN+","+BigInteger.ONE+"]", c.export(null, null, converter));
		JsonArray parameters = JsonArray.create();
		assertEquals("`name` in $1", c.export(new int[1], parameters, converter));
		assertEquals(arrayToString(args), parameters.get(0).toString());
	}

	@Test
	void testNotIn() {
		String[] args = new String[] { "gump", "davis" };
		QueryCriteria c = where(i("name")).notIn((Object) args); // the first arg is an array
		assertEquals("not( (`name` in [\"gump\",\"davis\"]) )", c.export());
		// this tests creating parameters from the args.
		JsonArray parameters = JsonArray.create();
		assertEquals("not( (`name` in $1) )", c.export(new int[1], parameters, converter));
		assertEquals(arrayToString(args), parameters.get(0).toString());
	}

	@Test
	void testTrue() {
		QueryCriteria c = where(i("name")).TRUE();
		assertEquals("`name` = true", c.export());

		JsonArray parameters1 = JsonArray.create();
		QueryCriteria c1 = where(i("name")).is(true);
		assertEquals("`name` = $1", c1.export(new int[1], parameters1, converter));
		assertEquals("true", parameters1.get(0).toString());
	}

	@Test
	void testFalse() {
		QueryCriteria c = where(i("name")).FALSE();
		assertEquals("`name` = false", c.export());

		JsonArray parameters1 = JsonArray.create();
		QueryCriteria c1 = where(i("name")).is(false);
		assertEquals("`name` = $1", c1.export(new int[1], parameters1, converter));
		assertEquals("false", parameters1.get(0).toString());
	}

	@Test
	void testKeys() {
		N1QLExpression expression = N1QLExpression.x("");
		assertEquals(" USE KEYS [\"a\",\"b\"]", expression.keys(Arrays.asList("a", "b")).toString());
		assertEquals(" USE KEYS [\"a\"]", expression.keys(Arrays.asList("a")).toString());
		assertEquals(" USE KEYS []", expression.keys(Arrays.asList()).toString());
	}

	@Test // https://github.com/spring-projects/spring-data-couchbase/issues/1066
	void testCriteriaCorrectlyEscapedWhenUsingMetaOnLHS() {
		final String bucketName = "sample-bucket";
		final String version = "1611287177404088320";
		QueryCriteria criteria = QueryCriteria.where(path(meta(escapedBucket(bucketName)), "cas")).eq(x(version));
		assertEquals("META(`" + bucketName + "`).cas = " + x(version), criteria.export());
	}

	@Test // https://github.com/spring-projects/spring-data-couchbase/issues/1066
	void testCriteriaCorrectlyEscapedWhenUsingMetaOnRHS() {
		final String bucketName = "sample-bucket";
		final String version = "1611287177404088320";
		QueryCriteria criteria = QueryCriteria.where(x(version)).eq(path(meta(escapedBucket(bucketName)), "cas"));
		assertEquals(x(version) + " = META(`" + bucketName + "`).cas", criteria.export());
	}

	private String arrayToString(Object[] array) {
		StringBuilder sb = new StringBuilder();
		if (array != null) {
			sb.append("[");
			boolean first = true;
			for (Object e : array) {
				if (!first) {
					sb.append(",");
				}
				first = false;
				sb.append(convert(e));
			}
			sb.append("]");
		}
		return sb.toString();
	}

	private static Config config = new Config();
	private static CouchbaseMappingContext mappingContext;
	static {
		try {
			mappingContext = config.couchbaseMappingContext(config.customConversions());
		} catch (Exception e) {
			throw new RuntimeException(e);
		}
	}

	private static MappingCouchbaseConverter converter = (new Config()).mappingCouchbaseConverter(mappingContext,(CouchbaseCustomConversions)config.customConversions());
	Object convert(Object e){
		Object o = converter.convertForWriteIfNeeded(e);
		if(o instanceof String){
			return "\""+o+"\"";
		}
		return o;
	}
}
