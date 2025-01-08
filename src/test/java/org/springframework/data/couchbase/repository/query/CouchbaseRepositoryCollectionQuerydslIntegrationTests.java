/*
 * Copyright 2017-2025 the original author or authors.
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

package org.springframework.data.couchbase.repository.query;

import com.querydsl.core.types.Predicate;
import com.querydsl.core.types.dsl.BooleanExpression;
import org.junit.jupiter.api.*;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.ApplicationContext;
import org.springframework.context.annotation.AnnotationConfigApplicationContext;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.data.couchbase.core.CouchbaseTemplate;
import org.springframework.data.couchbase.core.mapping.event.ValidatingCouchbaseEventListener;
import org.springframework.data.couchbase.core.query.QueryCriteriaDefinition;
import org.springframework.data.couchbase.domain.*;
import org.springframework.data.couchbase.repository.auditing.EnableCouchbaseAuditing;
import org.springframework.data.couchbase.repository.auditing.EnableReactiveCouchbaseAuditing;
import org.springframework.data.couchbase.repository.config.EnableCouchbaseRepositories;
import org.springframework.data.couchbase.repository.support.BasicQuery;
import org.springframework.data.couchbase.repository.support.SpringDataCouchbaseSerializer;
import org.springframework.data.couchbase.util.*;
import org.springframework.data.domain.Sort;
import org.springframework.test.annotation.DirtiesContext;
import org.springframework.test.context.junit.jupiter.SpringJUnitConfig;
import org.springframework.validation.beanvalidation.LocalValidatorFactoryBean;

import java.util.Arrays;
import java.util.Comparator;
import java.util.Locale;
import java.util.Optional;
import java.util.stream.StreamSupport;

import static com.couchbase.client.java.query.QueryScanConsistency.REQUEST_PLUS;
import static org.junit.jupiter.api.Assertions.*;
import static org.springframework.data.couchbase.util.Util.comprises;
import static org.springframework.data.couchbase.util.Util.exactly;

/**
 * Repository tests
 *
 * @author Tigran Babloyan
 */
@SpringJUnitConfig(CouchbaseRepositoryCollectionQuerydslIntegrationTests.Config.class)
@DirtiesContext
@IgnoreWhen(missesCapabilities = Capabilities.QUERY, clusterTypes = ClusterType.MOCKED)
public class CouchbaseRepositoryCollectionQuerydslIntegrationTests extends CollectionAwareDefaultScopeIntegrationTests {

	@Autowired
	AirlineCollectionedRepository airlineRepository;

	static QAirlineCollectioned airline = QAirlineCollectioned.airlineCollectioned;
	// saved
	static AirlineCollectioned united = new AirlineCollectioned("1", "United Airlines", "US");
	static AirlineCollectioned lufthansa = new AirlineCollectioned("2", "Lufthansa", "DE");
	static AirlineCollectioned emptyStringAirline = new AirlineCollectioned("3", "Empty String", "");
	static AirlineCollectioned nullStringAirline = new AirlineCollectioned("4", "Null String", null);
	static AirlineCollectioned unitedLowercase = new AirlineCollectioned("5", "united airlines", "US");
	static AirlineCollectioned[] saved = new AirlineCollectioned[] { united, lufthansa, emptyStringAirline, nullStringAirline, unitedLowercase };
	// not saved
	static AirlineCollectioned flyByNight = new AirlineCollectioned("1001", "Fly By Night", "UK");
	static AirlineCollectioned sleepByDay = new AirlineCollectioned("1002", "Sleep By Day", "CA");
	static AirlineCollectioned[] notSaved = new AirlineCollectioned[] { flyByNight, sleepByDay };

    @Autowired CouchbaseTemplate couchbaseTemplate;

    SpringDataCouchbaseSerializer serializer = null;

    @BeforeEach
    public void beforeEach() {
        super.beforeEach();
        serializer = new SpringDataCouchbaseSerializer(couchbaseTemplate.getConverter());
    }

	@BeforeAll
	static public void beforeAll() {
		callSuperBeforeAll(new Object() {});
		ApplicationContext ac = new AnnotationConfigApplicationContext(
				CouchbaseRepositoryCollectionQuerydslIntegrationTests.Config.class);
		CouchbaseTemplate template = (CouchbaseTemplate) ac.getBean("couchbaseTemplate");
		for (AirlineCollectioned airline : saved) {
			template.insertById(AirlineCollectioned.class).one(airline);
		}
		template.findByQuery(Airline.class).withConsistency(REQUEST_PLUS).all();
        logDisconnect(template.getCouchbaseClientFactory().getCluster(), "queryDsl-before");
	}

	@AfterAll
	static public void afterAll() {
		ApplicationContext ac = new AnnotationConfigApplicationContext(
				CouchbaseRepositoryCollectionQuerydslIntegrationTests.Config.class);
		CouchbaseTemplate template = (CouchbaseTemplate) ac.getBean("couchbaseTemplate");
		for (AirlineCollectioned airline : saved) {
			template.removeById(AirlineCollectioned.class).one(airline.getId());
		}
		template.findByQuery(Airline.class).withConsistency(REQUEST_PLUS).all();
        logDisconnect(template.getCouchbaseClientFactory().getCluster(), "queryDsl-after");
		callSuperAfterAll(new Object() {});
	}

	@Test
	void testEq() {
		{
			BooleanExpression predicate = airline.name.eq(flyByNight.getName());
			Iterable<AirlineCollectioned> result = airlineRepository.findAll(predicate);
			assertNull(
					comprises(result,
							Arrays.stream(saved).filter(a -> a.getName().equals(flyByNight.getName())).toArray(AirlineCollectioned[]::new)),
					"[unexpected] -> [missing]");
			assertEquals(" WHERE name = $1", bq(predicate));

		}
		{
			BooleanExpression predicate = airline.name.eq(united.getName());
			Iterable<AirlineCollectioned> result = airlineRepository.findAll(predicate);
			assertNull(
					comprises(result,
							Arrays.stream(saved).filter(a -> a.getName().equals(united.getName())).toArray(AirlineCollectioned[]::new)),
					"[unexpected] -> [missing]");
			assertEquals(" WHERE name = $1", bq(predicate));
		}
	}

	// this gives hqCountry == "" and hqCountry is missing
	// @Test
	void testStringIsEmpty() {
		{
			BooleanExpression predicate = airline.hqCountry.isEmpty();
			Iterable<AirlineCollectioned> result = airlineRepository.findAll(predicate);
			assertNull(comprises(result, emptyStringAirline, nullStringAirline), "[unexpected] -> [missing]");
			assertEquals(" WHERE UPPER(name) like $1", bq(predicate));
		}
	}

	@Test
	void testNot() {
		{
			BooleanExpression predicate = airline.name.eq(united.getName()).and(airline.hqCountry.eq(united.getHqCountry()))
					.not();
			Iterable<AirlineCollectioned> result = airlineRepository.findAll(predicate);
			assertNull(comprises(result,
					Arrays.stream(saved)
							.filter(a -> !(a.getName().equals(united.getName()) && a.getHqCountry().equals(united.getHqCountry())))
							.toArray(AirlineCollectioned[]::new)),
					"[unexpected] -> [missing]");
			assertEquals(" WHERE not( (  (hqCountry = $1) and   (name = $2)) )", bq(predicate));
		}
		{
			BooleanExpression predicate = airline.name.in(Arrays.asList(united.getName())).not();
			Iterable<AirlineCollectioned> result = airlineRepository.findAll(predicate);
			assertNull(
					comprises(result,
							Arrays.stream(saved).filter(a -> !(a.getName().equals(united.getName()))).toArray(AirlineCollectioned[]::new)),
					"[unexpected] -> [missing]");
			assertEquals(" WHERE not( (name = $1) )", bq(predicate));
		}
		{
			BooleanExpression predicate = airline.name.eq(united.getName()).not();
			Iterable<AirlineCollectioned> result = airlineRepository.findAll(predicate);
			assertNull(
					comprises(result,
							Arrays.stream(saved).filter(a -> !(a.getName().equals(united.getName()))).toArray(AirlineCollectioned[]::new)),
					"[unexpected] -> [missing]");
			assertEquals(" WHERE not( (name = $1) )", bq(predicate));
		}
	}

	@Test
	void testAnd() {
		{
			BooleanExpression predicate = airline.name.eq(united.getName()).and(airline.hqCountry.eq(united.getHqCountry()));
			Iterable<AirlineCollectioned> result = airlineRepository.findAll(predicate);
			assertNull(comprises(result,
					Arrays.stream(saved)
							.filter(a -> a.getName().equals(united.getName()) && a.getHqCountry().equals(united.getHqCountry()))
							.toArray(AirlineCollectioned[]::new)),
					"[unexpected] -> [missing]");
			assertEquals(" WHERE   (name = $1) and   (hqCountry = $2)", bq(predicate));
		}
		{
			BooleanExpression predicate = airline.name.eq(united.getName())
					.and(airline.hqCountry.eq(lufthansa.getHqCountry()));
			Iterable<AirlineCollectioned> result = airlineRepository.findAll(predicate);
			assertNull(comprises(result,
					Arrays.stream(saved)
							.filter(a -> a.getName().equals(united.getName()) && a.getHqCountry().equals(lufthansa.getHqCountry()))
							.toArray(AirlineCollectioned[]::new)),
					"[unexpected] -> [missing]");
			assertEquals(" WHERE   (name = $1) and   (hqCountry = $2)", bq(predicate));
		}
	}

	@Test
	void testOr() {
		{
			BooleanExpression predicate = airline.name.eq(united.getName())
					.or(airline.hqCountry.eq(lufthansa.getHqCountry()));
			Iterable<AirlineCollectioned> result = airlineRepository.findAll(predicate);
			assertNull(comprises(result,
					Arrays.stream(saved)
							.filter(a -> a.getName().equals(united.getName()) || a.getName().equals(lufthansa.getName()))
							.toArray(AirlineCollectioned[]::new)),
					"[unexpected] -> [missing]");
			assertEquals(" WHERE   (name = $1) or   (hqCountry = $2)", bq(predicate));
		}
	}

	@Test
	void testNe() {
		{
			BooleanExpression predicate = airline.name.ne(united.getName());
			Iterable<AirlineCollectioned> result = airlineRepository.findAll(predicate);
			assertNull(
					comprises(result,
							Arrays.stream(saved).filter(a -> !a.getName().equals(united.getName())).toArray(AirlineCollectioned[]::new)),
					"[unexpected] -> [missing]");
			assertEquals(" WHERE name != $1", bq(predicate));
		}
	}

	@Test
	void testStartsWith() {
		{
			BooleanExpression predicate = airline.name.startsWith(united.getName().substring(0, 5));
			Iterable<AirlineCollectioned> result = airlineRepository.findAll(predicate);
			assertNull(comprises(result, Arrays.stream(saved)
					.filter(a -> a.getName().startsWith(united.getName().substring(0, 5))).toArray(AirlineCollectioned[]::new)),
					"[unexpected] -> [missing]");
			assertEquals(" WHERE name like ($1||\"%\")", bq(predicate));
		}
	}

	@Test
	void testStartsWithIgnoreCase() {
		{
			BooleanExpression predicate = airline.name.startsWithIgnoreCase(united.getName().substring(0, 5));
			Iterable<AirlineCollectioned> result = airlineRepository.findAll(predicate);
			assertNull(comprises(result,
					Arrays.stream(saved)
							.filter(a -> a.getName().toUpperCase().startsWith(united.getName().toUpperCase().substring(0, 5)))
							.toArray(AirlineCollectioned[]::new)),
					"[unexpected] -> [missing]");
			assertEquals(" WHERE lower(name) like ($1||\"%\")", bq(predicate));
		}
	}

	@Test
	void testEndsWith() {
		{
			BooleanExpression predicate = airline.name.endsWith(united.getName().substring(1));
			Iterable<AirlineCollectioned> result = airlineRepository.findAll(predicate);
			assertNull(comprises(result, Arrays.stream(saved).filter(a -> a.getName().endsWith(united.getName().substring(1)))
					.toArray(AirlineCollectioned[]::new)), "[unexpected] -> [missing]");
			assertEquals(" WHERE name like (\"%\"||$1)", bq(predicate));

		}
	}

	@Test
	void testEndsWithIgnoreCase() {
		{
			BooleanExpression predicate = airline.name.endsWithIgnoreCase(united.getName().substring(1));
			Iterable<AirlineCollectioned> result = airlineRepository.findAll(predicate);
			assertNull(comprises(result,
					Arrays.stream(saved)
							.filter(a -> a.getName().toUpperCase().endsWith(united.getName().toUpperCase().substring(1)))
							.toArray(AirlineCollectioned[]::new)),
					"[unexpected] -> [missing]");
			assertEquals(" WHERE lower(name) like (\"%\"||$1)", bq(predicate));
		}
	}

	@Test
	void testEqIgnoreCase() {
		{
			BooleanExpression predicate = airline.name.equalsIgnoreCase(flyByNight.getName());
			Iterable<AirlineCollectioned> result = airlineRepository.findAll(predicate);
			assertNull(comprises(result,
					Arrays.stream(saved).filter(a -> a.getName().equalsIgnoreCase(flyByNight.getName())).toArray(AirlineCollectioned[]::new)),
					"[unexpected] -> [missing]");
			assertEquals(" WHERE lower(name) = $1", bq(predicate));
		}
		{
			BooleanExpression predicate = airline.name.equalsIgnoreCase(united.getName());
			Iterable<AirlineCollectioned> result = airlineRepository.findAll(predicate);
			assertNull(
					comprises(result,
							Arrays.stream(saved).filter(a -> a.getName().equalsIgnoreCase(united.getName())).toArray(AirlineCollectioned[]::new)),
					"[unexpected] -> [missing]");
			assertEquals(" WHERE lower(name) = $1", bq(predicate));
		}

	}

	@Test
	void testContains() {
		{
			BooleanExpression predicate = airline.name.contains("United");
			Iterable<AirlineCollectioned> result = airlineRepository.findAll(predicate);
			assertNull(
					comprises(result, Arrays.stream(saved).filter(a -> a.getName().contains("United")).toArray(AirlineCollectioned[]::new)),
					"[unexpected] -> [missing]");
			assertEquals(" WHERE contains(name, $1)", bq(predicate));
		}
	}

	@Test
	void testContainsIgnoreCase() {
		{
			BooleanExpression predicate = airline.name.containsIgnoreCase("united");
			Iterable<AirlineCollectioned> result = airlineRepository.findAll(predicate);
			assertNull(comprises(result,
					Arrays.stream(saved)
							.filter(a -> a.getName().toUpperCase(Locale.ROOT).contains("united".toUpperCase(Locale.ROOT)))
							.toArray(AirlineCollectioned[]::new)),
					"[unexpected] -> [missing]");
			assertEquals(" WHERE contains(lower(name), $1)", bq(predicate));

		}
	}

	@Test
	void testLike() {
		{
			BooleanExpression predicate = airline.name.like("%nited%");
			Iterable<AirlineCollectioned> result = airlineRepository.findAll(predicate);
			assertNull(
					comprises(result, Arrays.stream(saved).filter(a -> a.getName().contains("nited")).toArray(AirlineCollectioned[]::new)),
					"[unexpected] -> [missing]");
			assertEquals(" WHERE name like $1", bq(predicate));
		}
	}

	@Test
	void testLikeIgnoreCase() {
		{
			BooleanExpression predicate = airline.name.likeIgnoreCase("%Airlines");
			Iterable<AirlineCollectioned> result = airlineRepository.findAll(predicate);
			assertNull(comprises(result,
					Arrays.stream(saved)
							.filter(a -> a.getName().toUpperCase(Locale.ROOT).endsWith("Airlines".toUpperCase(Locale.ROOT)))
							.toArray(AirlineCollectioned[]::new)),
					"[unexpected] -> [missing]");
			assertEquals(" WHERE lower(name) like $1", bq(predicate));
		}
	}

	// This is 'between' is inclusive
	@Test
	void testBetween() {
		{
			BooleanExpression predicate = airline.name.between(flyByNight.getName(), united.getName());
			Iterable<AirlineCollectioned> result = airlineRepository.findAll(predicate);
			assertNull(comprises(result,
					Arrays.stream(saved)
							.filter(
									a -> a.getName().compareTo(flyByNight.getName()) >= 0 && a.getName().compareTo(united.getName()) <= 0)
							.toArray(AirlineCollectioned[]::new)),
					"[unexpected] -> [missing]");
			assertEquals(" WHERE name between $1 and $2", bq(predicate));
		}
	}

	@Test
	void testIn() {
		{
			BooleanExpression predicate = airline.name.in(Arrays.asList(united.getName()));
			Iterable<AirlineCollectioned> result = airlineRepository.findAll(predicate);
			assertNull(
					comprises(result,
							Arrays.stream(saved).filter(a -> a.getName().equals(united.getName())).toArray(AirlineCollectioned[]::new)),
					"[unexpected] -> [missing]");
			assertEquals(" WHERE name = $1", bq(predicate));
		}

		{
			BooleanExpression predicate = airline.name.in(Arrays.asList(united.getName(), lufthansa.getName()));
			Iterable<AirlineCollectioned> result = airlineRepository.findAll(predicate);
			assertNull(comprises(result,
					Arrays.stream(saved)
							.filter(a -> a.getName().equals(united.getName()) || a.getName().equals(lufthansa.getName()))
							.toArray(AirlineCollectioned[]::new)),
					"[unexpected] -> [missing]");
			assertEquals(" WHERE name in $1", bq(predicate));
		}

		{
			BooleanExpression predicate = airline.name.in("Fly By Night", "Sleep By Day");
			Iterable<AirlineCollectioned> result = airlineRepository.findAll(predicate);
			assertNull(comprises(result,
					Arrays.stream(saved)
							.filter(a -> a.getName().equals(flyByNight.getName()) || a.getName().equals(sleepByDay.getName()))
							.toArray(AirlineCollectioned[]::new)),
					"[unexpected] -> [missing]");
			assertEquals(" WHERE name in $1", bq(predicate));
		}
	}
	
	@Test
	void testSort(){
		{
			BooleanExpression predicate = airline.name.in(Arrays.stream(saved).map(AirlineCollectioned::getName).toList());
			Iterable<AirlineCollectioned> result = airlineRepository.findAll(predicate, Sort.by("name").ascending());
			assertArrayEquals(StreamSupport.stream(result.spliterator(), false).toArray(AirlineCollectioned[]::new),
							Arrays.stream(saved)
									.sorted(Comparator.comparing(AirlineCollectioned::getName))
									.toArray(AirlineCollectioned[]::new),
					"Order of airlines does not match");
		}

		{
			BooleanExpression predicate = airline.name.in(Arrays.stream(saved).map(AirlineCollectioned::getName).toList());
			Iterable<AirlineCollectioned> result = airlineRepository.findAll(predicate, Sort.by("name").descending());
			assertArrayEquals(StreamSupport.stream(result.spliterator(), false).toArray(AirlineCollectioned[]::new),
							Arrays.stream(saved)
									.sorted(Comparator.comparing(AirlineCollectioned::getName).reversed())
									.toArray(AirlineCollectioned[]::new),
					"Order of airlines does not match");
		}

		{
			BooleanExpression predicate = airline.name.in(Arrays.stream(saved).map(AirlineCollectioned::getName).toList());
			Iterable<AirlineCollectioned> result = airlineRepository.findAll(predicate, airline.name.asc());
			assertArrayEquals(StreamSupport.stream(result.spliterator(), false).toArray(AirlineCollectioned[]::new),
							Arrays.stream(saved)
									.sorted(Comparator.comparing(AirlineCollectioned::getName))
									.toArray(AirlineCollectioned[]::new),
					"Order of airlines does not match");
		}

		{
			BooleanExpression predicate = airline.name.in(Arrays.stream(saved).map(AirlineCollectioned::getName).toList());
			Iterable<AirlineCollectioned> result = airlineRepository.findAll(predicate, airline.name.desc());
			assertArrayEquals(StreamSupport.stream(result.spliterator(), false).toArray(AirlineCollectioned[]::new),
							Arrays.stream(saved)
									.sorted(Comparator.comparing(AirlineCollectioned::getName).reversed())
									.toArray(AirlineCollectioned[]::new),
					"Order of airlines does not match");
		}

		{
			Comparator<String> nullSafeStringComparator = Comparator
					.nullsFirst(String::compareTo);
			Iterable<AirlineCollectioned> result = airlineRepository.findAll(airline.hqCountry.asc().nullsFirst());
			assertArrayEquals(StreamSupport.stream(result.spliterator(), false).toArray(AirlineCollectioned[]::new),
					Arrays.stream(saved)
							.sorted(Comparator.comparing(AirlineCollectioned::getHqCountry, nullSafeStringComparator))
							.toArray(AirlineCollectioned[]::new),
					"Order of airlines does not match");
		}

		{
			Comparator<String> nullSafeStringComparator = Comparator
					.nullsFirst(String::compareTo);
			Iterable<AirlineCollectioned> result = airlineRepository.findAll(airline.hqCountry.desc().nullsLast());
			assertArrayEquals(StreamSupport.stream(result.spliterator(), false).toArray(AirlineCollectioned[]::new),
					Arrays.stream(saved)
							.sorted(Comparator.comparing(AirlineCollectioned::getHqCountry, nullSafeStringComparator).reversed())
							.toArray(AirlineCollectioned[]::new),
					"Order of airlines does not match");
		}
	}
	

	@Test
	void testNotIn() {
		{
			BooleanExpression predicate = airline.name.notIn("Fly By Night", "Sleep By Day");
			Iterable<AirlineCollectioned> result = airlineRepository.findAll(predicate);
			assertNull(comprises(result,
					Arrays.stream(saved)
							.filter(a -> !(a.getName().equals(flyByNight.getName()) || a.getName().equals(sleepByDay.getName())))
							.toArray(AirlineCollectioned[]::new)),
					"[unexpected] -> [missing]");
			assertEquals(" WHERE not( (name in $1) )", bq(predicate));
		}
		{
			BooleanExpression predicate = airline.name.notIn(united.getName());
			Iterable<AirlineCollectioned> result = airlineRepository.findAll(predicate);
			assertNull(
					comprises(result,
							Arrays.stream(saved).filter(a -> !a.getName().equals(united.getName())).toArray(AirlineCollectioned[]::new)),
					"[unexpected] -> [missing]");
			assertEquals(" WHERE name != $1", bq(predicate));
		}
	}

	@Test
	@Disabled
	void testColIsEmpty() {}

	@Test
	void testLt() {
		{
			BooleanExpression predicate = airline.name.lt(lufthansa.getName());
			Iterable<AirlineCollectioned> result = airlineRepository.findAll(predicate);
			assertNull(
					comprises(result,
							Arrays.stream(saved).filter(a -> a.getName().compareTo(lufthansa.getName()) < 0).toArray(AirlineCollectioned[]::new)),
					"[unexpected] -> [missing]");
			assertEquals(" WHERE name < $1", bq(predicate));
		}
	}

	@Test
	void testGt() {
		{
			BooleanExpression predicate = airline.name.gt(lufthansa.getName());
			Iterable<AirlineCollectioned> result = airlineRepository.findAll(predicate);
			assertNull(
					comprises(result,
							Arrays.stream(saved).filter(a -> a.getName().compareTo(lufthansa.getName()) > 0).toArray(AirlineCollectioned[]::new)),
					"[unexpected] -> [missing]");
			assertEquals(" WHERE name > $1", bq(predicate));
		}
	}

	@Test
	void testLoe() {
		{
			BooleanExpression predicate = airline.name.loe(lufthansa.getName());
			Iterable<AirlineCollectioned> result = airlineRepository.findAll(predicate);
			assertNull(comprises(result,
					Arrays.stream(saved).filter(a -> a.getName().compareTo(lufthansa.getName()) <= 0).toArray(AirlineCollectioned[]::new)),
					"[unexpected] -> [missing]");
			assertEquals(" WHERE name <= $1", bq(predicate));
		}
	}

	@Test
	void testGoe() {
		{
			BooleanExpression predicate = airline.name.goe(lufthansa.getName());
			Iterable<AirlineCollectioned> result = airlineRepository.findAll(predicate);
			assertNull(comprises(result,
					Arrays.stream(saved).filter(a -> a.getName().compareTo(lufthansa.getName()) >= 0).toArray(AirlineCollectioned[]::new)),
					"[unexpected] -> [missing]");
			assertEquals(" WHERE name >= $1", bq(predicate));
		}
	}

	// when hqCountry == null, no value is stored therefore isNull is false. Only hqCountry:null gives isNull
	// and we don't have that. Conversely, only hqCountry has a value (which is not 'null') gives isNotNull
	// so isNull and isNotNull are *not* compliments
	@Test
	@Disabled
	void testIsNull() {
		{
			BooleanExpression predicate = airline.hqCountry.isNull();
			Optional<AirlineCollectioned> result = airlineRepository.findOne(predicate);
			assertNull(exactly(result, nullStringAirline), "[unexpected] -> [missing]");
			assertEquals(" WHERE name = $1", bq(predicate));
		}
	}

	// when hqCountry == null, no value is stored therefore isNull is false. Only hqCountry:null gives isNull
	// and we don't have that. Conversely, only hqCountry has a value (which is not 'null') gives isNotNull
	// so isNull and isNotNull are *not* compliments
	@Test
	void testIsNotNull() {
		{
			BooleanExpression predicate = airline.hqCountry.isNotNull();
			Iterable<AirlineCollectioned> result = airlineRepository.findAll(predicate);
			assertNull(comprises(result, Arrays.stream(saved).filter(a -> a.getHqCountry() != null).toArray(AirlineCollectioned[]::new)),
					"[unexpected] -> [missing]");
			assertEquals(" WHERE hqCountry is not null", bq(predicate));
		}
	}

	@Test
	@Disabled
	void testContainsKey() {}

	@Test
	void testStringLength() {
		{
			BooleanExpression predicate = airline.name.length().eq(united.getName().length());
			Iterable<AirlineCollectioned> result = airlineRepository.findAll(predicate);
			assertNull(comprises(result,
					Arrays.stream(saved).filter(a -> a.getName().length() == united.getName().length()).toArray(AirlineCollectioned[]::new)),
					"[unexpected] -> [missing]");
			assertEquals(" WHERE LENGTH(name) = $1", bq(predicate));
		}
		{
			BooleanExpression predicate = airline.name.length().eq(flyByNight.getName().length());
			Iterable<AirlineCollectioned> result = airlineRepository.findAll(predicate);
			assertNull(comprises(result, Arrays.stream(saved)
					.filter(a -> a.getName().length() == flyByNight.getName().length()).toArray(AirlineCollectioned[]::new)),
					"[unexpected] -> [missing]");
			assertEquals(" WHERE LENGTH(name) = $1", bq(predicate));
		}
	}

	@Configuration
	@EnableCouchbaseRepositories("org.springframework.data.couchbase")
	@EnableCouchbaseAuditing(auditorAwareRef = "auditorAwareRef", dateTimeProviderRef = "dateTimeProviderRef")
	@EnableReactiveCouchbaseAuditing(auditorAwareRef = "reactiveAuditorAwareRef", dateTimeProviderRef = "dateTimeProviderRef")

	static class Config extends org.springframework.data.couchbase.domain.Config {

		@Bean
		public LocalValidatorFactoryBean validator() {
			return new LocalValidatorFactoryBean();
		}

		@Bean
		public ValidatingCouchbaseEventListener validationEventListener() {
			return new ValidatingCouchbaseEventListener(validator());
		}
	}

	String bq(Predicate predicate) {
		BasicQuery basicQuery = new BasicQuery((QueryCriteriaDefinition) serializer.handle(predicate), null);
		return basicQuery.export(new int[1]);
	}

}
