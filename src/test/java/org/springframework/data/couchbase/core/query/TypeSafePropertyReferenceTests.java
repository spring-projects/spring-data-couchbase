/*
 * Copyright 2026-present the original author or authors.
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
import static org.springframework.data.couchbase.core.query.QueryCriteria.where;

import org.junit.jupiter.api.Test;

import org.springframework.data.core.PropertyPath;
import org.springframework.data.core.TypedPropertyPath;
import org.springframework.data.couchbase.domain.Address;
import org.springframework.data.couchbase.domain.Airport;
import org.springframework.data.couchbase.domain.Person;
import org.springframework.data.couchbase.domain.User;
import org.springframework.data.domain.Sort;

/**
 * Unit tests for type-safe property references in QueryCriteria, Query, and Sort.
 *
 * @author Emilien Bevierre
 */
class TypeSafePropertyReferenceTests {

	// --- QueryCriteria.where() ---

	@Test
	void whereWithMethodReference() {
		QueryCriteria c = where(User::getFirstname).is("Cynthia");
		assertEquals("firstname = \"Cynthia\"", c.export());
	}

	@Test
	void whereWithNestedPropertyPath() {
		TypedPropertyPath<Person, String> cityPath = PropertyPath.of(Person::getAddress).then(Address::getCity);
		QueryCriteria c = where(cityPath).is("Paris");
		assertEquals("address.city = \"Paris\"", c.export());
	}

	// --- QueryCriteria.and() ---

	@Test
	void andWithMethodReference() {
		QueryCriteria c = where(User::getFirstname).is("Charles")
				.and(User::getLastname).is("Darwin");
		assertEquals("firstname = \"Charles\" and lastname = \"Darwin\"", c.export());
	}

	@Test
	void andWithNestedPropertyPath() {
		TypedPropertyPath<Person, String> streetPath = PropertyPath.of(Person::getAddress).then(Address::getStreet);
		QueryCriteria c = where(Person::getFirstname).is("Oliver")
				.and(streetPath).is("Main St");
		assertEquals("firstname = \"Oliver\" and address.street = \"Main St\"", c.export());
	}

	// --- QueryCriteria.or() ---

	@Test
	void orWithMethodReference() {
		QueryCriteria c = where(User::getFirstname).is("Oliver")
				.or(User::getLastname).is("Gierke");
		assertEquals("firstname = \"Oliver\" or lastname = \"Gierke\"", c.export());
	}

	@Test
	void orWithNestedPropertyPath() {
		TypedPropertyPath<Person, String> cityPath = PropertyPath.of(Person::getAddress).then(Address::getCity);
		QueryCriteria c = where(Person::getFirstname).is("Oliver")
				.or(cityPath).is("Berlin");
		assertEquals("firstname = \"Oliver\" or address.city = \"Berlin\"", c.export());
	}

	// --- Query.distinct() with TypedPropertyPath ---

	@Test
	void queryDistinctWithTypedPropertyPath() {
		Query q = new Query();
		q.distinct(PropertyPath.of(Airport::getIata));
		assertEquals(1, q.getDistinctFields().length);
		assertEquals("iata", q.getDistinctFields()[0]);
	}

	@Test
	void queryDistinctWithNestedTypedPropertyPath() {
		Query q = new Query();
		q.distinct(PropertyPath.of(Person::getAddress).then(Address::getCity));
		assertEquals(1, q.getDistinctFields().length);
		assertEquals("address.city", q.getDistinctFields()[0]);
	}

	@Test
	void queryDistinctWithMultipleTypedPropertyPaths() {
		Query q = new Query();
		q.distinct(PropertyPath.of(Airport::getIata), PropertyPath.of(Airport::getIcao));
		assertEquals(2, q.getDistinctFields().length);
		assertEquals("iata", q.getDistinctFields()[0]);
		assertEquals("icao", q.getDistinctFields()[1]);
	}

	// --- Sort.by() with TypedPropertyPath (Spring Data Commons API) ---

	@Test
	void sortByTypedPropertyPath() {
		Sort sort = Sort.by(PropertyPath.of(User::getFirstname));
		assertEquals("firstname: ASC", sort.iterator().next().toString());
	}

	@Test
	void sortByMultipleTypedPropertyPaths() {
		Sort sort = Sort.by(PropertyPath.of(User::getFirstname), PropertyPath.of(User::getLastname));
		var orders = sort.toList();
		assertEquals(2, orders.size());
		assertEquals("firstname: ASC", orders.get(0).toString());
		assertEquals("lastname: ASC", orders.get(1).toString());
	}

	@Test
	void sortByNestedTypedPropertyPath() {
		Sort sort = Sort.by(PropertyPath.of(Person::getAddress).then(Address::getCity));
		assertEquals("address.city: ASC", sort.iterator().next().toString());
	}

	@Test
	void sortOrderWithTypedPropertyPath() {
		Sort.Order order = Sort.Order.desc(PropertyPath.of(User::getFirstname));
		assertEquals("firstname: DESC", order.toString());
	}

	// --- Sort with Query ---

	@Test
	void querySortWithTypedPropertyPath() {
		Query query = new Query();
		query.with(Sort.by(PropertyPath.of(Airport::getIata)));
		StringBuilder sb = new StringBuilder();
		query.appendSort(sb);
		assertEquals(" ORDER BY iata ASC", sb.toString());
	}

	@Test
	void querySortDescWithTypedPropertyPath() {
		Query query = new Query();
		query.with(Sort.by(Sort.Direction.DESC, PropertyPath.of(Airport::getIata)));
		StringBuilder sb = new StringBuilder();
		query.appendSort(sb);
		assertEquals(" ORDER BY iata DESC", sb.toString());
	}

	@Test
	void backwardsCompatibilityStringBased() {
		QueryCriteria c = where("firstname").is("Oliver");
		assertEquals("firstname = \"Oliver\"", c.export());

		Query q = new Query();
		q.distinct(new String[] { "iata", "icao" });
		assertEquals(2, q.getDistinctFields().length);
	}

	@Test
	void complexChainWithMixedApproach() {
		TypedPropertyPath<Person, String> cityPath = PropertyPath.of(Person::getAddress).then(Address::getCity);
		QueryCriteria c = where(Person::getFirstname).is("Oliver")
				.and("lastname").is("Gierke")
				.or(cityPath).is("Berlin");
		assertEquals("firstname = \"Oliver\" and lastname = \"Gierke\" or address.city = \"Berlin\"", c.export());
	}
}
