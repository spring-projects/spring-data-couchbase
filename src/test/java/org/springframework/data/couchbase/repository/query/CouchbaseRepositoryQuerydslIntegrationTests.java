/*
 * Copyright 2017-2022 the original author or authors.
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

import static com.couchbase.client.java.query.QueryScanConsistency.REQUEST_PLUS;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.springframework.data.couchbase.util.Util.comprises;
import static org.springframework.data.couchbase.util.Util.exactly;

import java.time.Duration;
import java.util.Arrays;
import java.util.Locale;
import java.util.Optional;

import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.ApplicationContext;
import org.springframework.context.annotation.AnnotationConfigApplicationContext;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.data.auditing.DateTimeProvider;
import org.springframework.data.couchbase.config.AbstractCouchbaseConfiguration;
import org.springframework.data.couchbase.core.CouchbaseTemplate;
import org.springframework.data.couchbase.core.mapping.event.ValidatingCouchbaseEventListener;
import org.springframework.data.couchbase.core.query.QueryCriteriaDefinition;
import org.springframework.data.couchbase.domain.Airline;
import org.springframework.data.couchbase.domain.AirlineRepository;
import org.springframework.data.couchbase.domain.NaiveAuditorAware;
import org.springframework.data.couchbase.domain.QAirline;
import org.springframework.data.couchbase.domain.time.AuditingDateTimeProvider;
import org.springframework.data.couchbase.repository.auditing.EnableCouchbaseAuditing;
import org.springframework.data.couchbase.repository.config.EnableCouchbaseRepositories;
import org.springframework.data.couchbase.repository.support.BasicQuery;
import org.springframework.data.couchbase.repository.support.SpringDataCouchbaseSerializer;
import org.springframework.data.couchbase.util.Capabilities;
import org.springframework.data.couchbase.util.ClusterType;
import org.springframework.data.couchbase.util.IgnoreWhen;
import org.springframework.data.couchbase.util.JavaIntegrationTests;
import org.springframework.test.context.junit.jupiter.SpringJUnitConfig;
import org.springframework.validation.beanvalidation.LocalValidatorFactoryBean;

import com.couchbase.client.java.env.ClusterEnvironment;
import com.couchbase.client.java.query.QueryScanConsistency;
import com.querydsl.core.types.Predicate;
import com.querydsl.core.types.dsl.BooleanExpression;

/**
 * Repository tests
 *
 * @author Michael Reiche
 */
@SpringJUnitConfig(CouchbaseRepositoryQuerydslIntegrationTests.Config.class)
@IgnoreWhen(missesCapabilities = Capabilities.QUERY, clusterTypes = ClusterType.MOCKED)
public class CouchbaseRepositoryQuerydslIntegrationTests extends JavaIntegrationTests {

	@Autowired AirlineRepository airlineRepository;

	static QAirline airline = QAirline.airline;
	// saved
	static Airline united = new Airline("1", "United Airlines", "US");
	static Airline lufthansa = new Airline("2", "Lufthansa", "DE");
	static Airline emptyStringAirline = new Airline("3", "Empty String", "");
	static Airline nullStringAirline = new Airline("4", "Null String", null);
	static Airline unitedLowercase = new Airline("5", "united airlines", "US");
	static Airline[] saved = new Airline[] { united, lufthansa, emptyStringAirline, nullStringAirline, unitedLowercase };
	// not saved
	static Airline flyByNight = new Airline("1001", "Fly By Night", "UK");
	static Airline sleepByDay = new Airline("1002", "Sleep By Day", "CA");
	static Airline[] notSaved = new Airline[] { flyByNight, sleepByDay };

	SpringDataCouchbaseSerializer serializer = new SpringDataCouchbaseSerializer(couchbaseTemplate.getConverter());

	@BeforeAll
	static public void beforeAll() {
		callSuperBeforeAll(new Object() {});
		ApplicationContext ac = new AnnotationConfigApplicationContext(
				CouchbaseRepositoryQuerydslIntegrationTests.Config.class);
		CouchbaseTemplate template = (CouchbaseTemplate) ac.getBean("couchbaseTemplate");
		for (Airline airline : saved) {
			template.insertById(Airline.class).one(airline);
		}
		template.findByQuery(Airline.class).withConsistency(REQUEST_PLUS).all();
	}

	@AfterAll
	static public void afterAll() {
		ApplicationContext ac = new AnnotationConfigApplicationContext(
				CouchbaseRepositoryQuerydslIntegrationTests.Config.class);
		CouchbaseTemplate template = (CouchbaseTemplate) ac.getBean("couchbaseTemplate");
		for (Airline airline : saved) {
			template.removeById(Airline.class).one(airline.getId());
		}
		template.findByQuery(Airline.class).withConsistency(REQUEST_PLUS).all();
		callSuperAfterAll(new Object() {});
	}

	@Test
	void testEq() {
		{
			BooleanExpression predicate = airline.name.eq(flyByNight.getName());
			Iterable<Airline> result = airlineRepository.findAll(predicate);
			assertNull(
					comprises(result,
							Arrays.stream(saved).filter(a -> a.getName().equals(flyByNight.getName())).toArray(Airline[]::new)),
					"[unexpected] -> [missing]");
			assertEquals(" WHERE name = $1", bq(predicate));

		}
		{
			BooleanExpression predicate = airline.name.eq(united.getName());
			Iterable<Airline> result = airlineRepository.findAll(predicate);
			assertNull(
					comprises(result,
							Arrays.stream(saved).filter(a -> a.getName().equals(united.getName())).toArray(Airline[]::new)),
					"[unexpected] -> [missing]");
			assertEquals(" WHERE name = $1", bq(predicate));
		}
	}

	// this gives hqCountry == "" and hqCountry is missing
	// @Test
	void testStringIsEmpty() {
		{
			BooleanExpression predicate = airline.hqCountry.isEmpty();
			Iterable<Airline> result = airlineRepository.findAll(predicate);
			assertNull(comprises(result, emptyStringAirline, nullStringAirline), "[unexpected] -> [missing]");
			assertEquals(" WHERE UPPER(name) like $1", bq(predicate));
		}
	}

	@Test
	void testNot() {
		{
			BooleanExpression predicate = airline.name.eq(united.getName()).and(airline.hqCountry.eq(united.getHqCountry()))
					.not();
			Iterable<Airline> result = airlineRepository.findAll(predicate);
			assertNull(comprises(result,
					Arrays.stream(saved)
							.filter(a -> !(a.getName().equals(united.getName()) && a.getHqCountry().equals(united.getHqCountry())))
							.toArray(Airline[]::new)),
					"[unexpected] -> [missing]");
			assertEquals(" WHERE not( (  (hqCountry = $1) and   (name = $2)) )", bq(predicate));
		}
		{
			BooleanExpression predicate = airline.name.in(Arrays.asList(united.getName())).not();
			Iterable<Airline> result = airlineRepository.findAll(predicate);
			assertNull(
					comprises(result,
							Arrays.stream(saved).filter(a -> !(a.getName().equals(united.getName()))).toArray(Airline[]::new)),
					"[unexpected] -> [missing]");
			assertEquals(" WHERE not( (name = $1) )", bq(predicate));
		}
		{
			BooleanExpression predicate = airline.name.eq(united.getName()).not();
			Iterable<Airline> result = airlineRepository.findAll(predicate);
			assertNull(
					comprises(result,
							Arrays.stream(saved).filter(a -> !(a.getName().equals(united.getName()))).toArray(Airline[]::new)),
					"[unexpected] -> [missing]");
			assertEquals(" WHERE not( (name = $1) )", bq(predicate));
		}
	}

	@Test
	void testAnd() {
		{
			BooleanExpression predicate = airline.name.eq(united.getName()).and(airline.hqCountry.eq(united.getHqCountry()));
			Iterable<Airline> result = airlineRepository.findAll(predicate);
			assertNull(comprises(result,
					Arrays.stream(saved)
							.filter(a -> a.getName().equals(united.getName()) && a.getHqCountry().equals(united.getHqCountry()))
							.toArray(Airline[]::new)),
					"[unexpected] -> [missing]");
			assertEquals(" WHERE   (name = $1) and   (hqCountry = $2)", bq(predicate));
		}
		{
			BooleanExpression predicate = airline.name.eq(united.getName())
					.and(airline.hqCountry.eq(lufthansa.getHqCountry()));
			Iterable<Airline> result = airlineRepository.findAll(predicate);
			assertNull(comprises(result,
					Arrays.stream(saved)
							.filter(a -> a.getName().equals(united.getName()) && a.getHqCountry().equals(lufthansa.getHqCountry()))
							.toArray(Airline[]::new)),
					"[unexpected] -> [missing]");
			assertEquals(" WHERE   (name = $1) and   (hqCountry = $2)", bq(predicate));
		}
	}

	@Test
	void testOr() {
		{
			BooleanExpression predicate = airline.name.eq(united.getName())
					.or(airline.hqCountry.eq(lufthansa.getHqCountry()));
			Iterable<Airline> result = airlineRepository.findAll(predicate);
			assertNull(comprises(result,
					Arrays.stream(saved)
							.filter(a -> a.getName().equals(united.getName()) || a.getName().equals(lufthansa.getName()))
							.toArray(Airline[]::new)),
					"[unexpected] -> [missing]");
			assertEquals(" WHERE   (name = $1) or   (hqCountry = $2)", bq(predicate));
		}
	}

	@Test
	void testNe() {
		{
			BooleanExpression predicate = airline.name.ne(united.getName());
			Iterable<Airline> result = airlineRepository.findAll(predicate);
			assertNull(
					comprises(result,
							Arrays.stream(saved).filter(a -> !a.getName().equals(united.getName())).toArray(Airline[]::new)),
					"[unexpected] -> [missing]");
			assertEquals(" WHERE name != $1", bq(predicate));
		}
	}

	@Test
	void testStartsWith() {
		{
			BooleanExpression predicate = airline.name.startsWith(united.getName().substring(0, 5));
			Iterable<Airline> result = airlineRepository.findAll(predicate);
			assertNull(comprises(result, Arrays.stream(saved)
					.filter(a -> a.getName().startsWith(united.getName().substring(0, 5))).toArray(Airline[]::new)),
					"[unexpected] -> [missing]");
			assertEquals(" WHERE name like ($1||\"%\")", bq(predicate));
		}
	}

	@Test
	void testStartsWithIgnoreCase() {
		{
			BooleanExpression predicate = airline.name.startsWithIgnoreCase(united.getName().substring(0, 5));
			Iterable<Airline> result = airlineRepository.findAll(predicate);
			assertNull(comprises(result,
					Arrays.stream(saved)
							.filter(a -> a.getName().toUpperCase().startsWith(united.getName().toUpperCase().substring(0, 5)))
							.toArray(Airline[]::new)),
					"[unexpected] -> [missing]");
			assertEquals(" WHERE UPPER(name) like ($1||\"%\")", bq(predicate));
		}
	}

	@Test
	void testEndsWith() {
		{
			BooleanExpression predicate = airline.name.endsWith(united.getName().substring(1));
			Iterable<Airline> result = airlineRepository.findAll(predicate);
			assertNull(comprises(result, Arrays.stream(saved).filter(a -> a.getName().endsWith(united.getName().substring(1)))
					.toArray(Airline[]::new)), "[unexpected] -> [missing]");
			assertEquals(" WHERE name like (\"%\"||$1)", bq(predicate));

		}
	}

	@Test
	void testEndsWithIgnoreCase() {
		{
			BooleanExpression predicate = airline.name.endsWithIgnoreCase(united.getName().substring(1));
			Iterable<Airline> result = airlineRepository.findAll(predicate);
			assertNull(comprises(result,
					Arrays.stream(saved)
							.filter(a -> a.getName().toUpperCase().endsWith(united.getName().toUpperCase().substring(1)))
							.toArray(Airline[]::new)),
					"[unexpected] -> [missing]");
			assertEquals(" WHERE UPPER(name) like (\"%\"||$1)", bq(predicate));
		}
	}

	@Test
	void testEqIgnoreCase() {
		{
			BooleanExpression predicate = airline.name.equalsIgnoreCase(flyByNight.getName());
			Iterable<Airline> result = airlineRepository.findAll(predicate);
			assertNull(comprises(result,
					Arrays.stream(saved).filter(a -> a.getName().equalsIgnoreCase(flyByNight.getName())).toArray(Airline[]::new)),
					"[unexpected] -> [missing]");
			assertEquals(" WHERE UPPER(name) = $1", bq(predicate));
		}
		{
			BooleanExpression predicate = airline.name.equalsIgnoreCase(united.getName());
			Iterable<Airline> result = airlineRepository.findAll(predicate);
			assertNull(
					comprises(result,
							Arrays.stream(saved).filter(a -> a.getName().equalsIgnoreCase(united.getName())).toArray(Airline[]::new)),
					"[unexpected] -> [missing]");
			assertEquals(" WHERE UPPER(name) = $1", bq(predicate));
		}

	}

	@Test
	void testContains() {
		{
			BooleanExpression predicate = airline.name.contains("United");
			Iterable<Airline> result = airlineRepository.findAll(predicate);
			assertNull(
					comprises(result, Arrays.stream(saved).filter(a -> a.getName().contains("United")).toArray(Airline[]::new)),
					"[unexpected] -> [missing]");
			assertEquals(" WHERE contains(name, $1)", bq(predicate));
		}
	}

	@Test
	void testContainsIgnoreCase() {
		{
			BooleanExpression predicate = airline.name.containsIgnoreCase("united");
			Iterable<Airline> result = airlineRepository.findAll(predicate);
			assertNull(comprises(result,
					Arrays.stream(saved)
							.filter(a -> a.getName().toUpperCase(Locale.ROOT).contains("united".toUpperCase(Locale.ROOT)))
							.toArray(Airline[]::new)),
					"[unexpected] -> [missing]");
			assertEquals(" WHERE contains(UPPER(name), $1)", bq(predicate));

		}
	}

	@Test
	void testLike() {
		{
			BooleanExpression predicate = airline.name.like("%nited%");
			Iterable<Airline> result = airlineRepository.findAll(predicate);
			assertNull(
					comprises(result, Arrays.stream(saved).filter(a -> a.getName().contains("nited")).toArray(Airline[]::new)),
					"[unexpected] -> [missing]");
			assertEquals(" WHERE name like $1", bq(predicate));
		}
	}

	@Test
	void testLikeIgnoreCase() {
		{
			BooleanExpression predicate = airline.name.likeIgnoreCase("%Airlines");
			Iterable<Airline> result = airlineRepository.findAll(predicate);
			assertNull(comprises(result,
					Arrays.stream(saved)
							.filter(a -> a.getName().toUpperCase(Locale.ROOT).endsWith("Airlines".toUpperCase(Locale.ROOT)))
							.toArray(Airline[]::new)),
					"[unexpected] -> [missing]");
			assertEquals(" WHERE UPPER(name) like $1", bq(predicate));
		}
	}

	// This is 'between' is inclusive
	@Test
	void testBetween() {
		{
			BooleanExpression predicate = airline.name.between(flyByNight.getName(), united.getName());
			Iterable<Airline> result = airlineRepository.findAll(predicate);
			assertNull(comprises(result,
					Arrays.stream(saved)
							.filter(
									a -> a.getName().compareTo(flyByNight.getName()) >= 0 && a.getName().compareTo(united.getName()) <= 0)
							.toArray(Airline[]::new)),
					"[unexpected] -> [missing]");
			assertEquals(" WHERE name between $1 and $2", bq(predicate));
		}
	}

	@Test
	void testIn() {
		{
			BooleanExpression predicate = airline.name.in(Arrays.asList(united.getName()));
			Iterable<Airline> result = airlineRepository.findAll(predicate);
			assertNull(
					comprises(result,
							Arrays.stream(saved).filter(a -> a.getName().equals(united.getName())).toArray(Airline[]::new)),
					"[unexpected] -> [missing]");
			assertEquals(" WHERE name = $1", bq(predicate));
		}

		{
			BooleanExpression predicate = airline.name.in(Arrays.asList(united.getName(), lufthansa.getName()));
			Iterable<Airline> result = airlineRepository.findAll(predicate);
			assertNull(comprises(result,
					Arrays.stream(saved)
							.filter(a -> a.getName().equals(united.getName()) || a.getName().equals(lufthansa.getName()))
							.toArray(Airline[]::new)),
					"[unexpected] -> [missing]");
			assertEquals(" WHERE name in $1", bq(predicate));
		}

		{
			BooleanExpression predicate = airline.name.in("Fly By Night", "Sleep By Day");
			Iterable<Airline> result = airlineRepository.findAll(predicate);
			assertNull(comprises(result,
					Arrays.stream(saved)
							.filter(a -> a.getName().equals(flyByNight.getName()) || a.getName().equals(sleepByDay.getName()))
							.toArray(Airline[]::new)),
					"[unexpected] -> [missing]");
			assertEquals(" WHERE name in $1", bq(predicate));
		}
	}

	@Test
	void testNotIn() {
		{
			BooleanExpression predicate = airline.name.notIn("Fly By Night", "Sleep By Day");
			Iterable<Airline> result = airlineRepository.findAll(predicate);
			assertNull(comprises(result,
					Arrays.stream(saved)
							.filter(a -> !(a.getName().equals(flyByNight.getName()) || a.getName().equals(sleepByDay.getName())))
							.toArray(Airline[]::new)),
					"[unexpected] -> [missing]");
			assertEquals(" WHERE not( (name in $1) )", bq(predicate));
		}
		{
			BooleanExpression predicate = airline.name.notIn(united.getName());
			Iterable<Airline> result = airlineRepository.findAll(predicate);
			assertNull(
					comprises(result,
							Arrays.stream(saved).filter(a -> !a.getName().equals(united.getName())).toArray(Airline[]::new)),
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
			Iterable<Airline> result = airlineRepository.findAll(predicate);
			assertNull(
					comprises(result,
							Arrays.stream(saved).filter(a -> a.getName().compareTo(lufthansa.getName()) < 0).toArray(Airline[]::new)),
					"[unexpected] -> [missing]");
			assertEquals(" WHERE name < $1", bq(predicate));
		}
	}

	@Test
	void testGt() {
		{
			BooleanExpression predicate = airline.name.gt(lufthansa.getName());
			Iterable<Airline> result = airlineRepository.findAll(predicate);
			assertNull(
					comprises(result,
							Arrays.stream(saved).filter(a -> a.getName().compareTo(lufthansa.getName()) > 0).toArray(Airline[]::new)),
					"[unexpected] -> [missing]");
			assertEquals(" WHERE name > $1", bq(predicate));
		}
	}

	@Test
	void testLoe() {
		{
			BooleanExpression predicate = airline.name.loe(lufthansa.getName());
			Iterable<Airline> result = airlineRepository.findAll(predicate);
			assertNull(comprises(result,
					Arrays.stream(saved).filter(a -> a.getName().compareTo(lufthansa.getName()) <= 0).toArray(Airline[]::new)),
					"[unexpected] -> [missing]");
			assertEquals(" WHERE name <= $1", bq(predicate));
		}
	}

	@Test
	void testGoe() {
		{
			BooleanExpression predicate = airline.name.goe(lufthansa.getName());
			Iterable<Airline> result = airlineRepository.findAll(predicate);
			assertNull(comprises(result,
					Arrays.stream(saved).filter(a -> a.getName().compareTo(lufthansa.getName()) >= 0).toArray(Airline[]::new)),
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
			Optional<Airline> result = airlineRepository.findOne(predicate);
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
			Iterable<Airline> result = airlineRepository.findAll(predicate);
			assertNull(comprises(result, Arrays.stream(saved).filter(a -> a.getHqCountry() != null).toArray(Airline[]::new)),
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
			Iterable<Airline> result = airlineRepository.findAll(predicate);
			assertNull(comprises(result,
					Arrays.stream(saved).filter(a -> a.getName().length() == united.getName().length()).toArray(Airline[]::new)),
					"[unexpected] -> [missing]");
			assertEquals(" WHERE LENGTH(name) = $1", bq(predicate));
		}
		{
			BooleanExpression predicate = airline.name.length().eq(flyByNight.getName().length());
			Iterable<Airline> result = airlineRepository.findAll(predicate);
			assertNull(comprises(result, Arrays.stream(saved)
					.filter(a -> a.getName().length() == flyByNight.getName().length()).toArray(Airline[]::new)),
					"[unexpected] -> [missing]");
			assertEquals(" WHERE LENGTH(name) = $1", bq(predicate));
		}
	}

	private void sleep(int millis) {
		try {
			Thread.sleep(millis); // so they are executed out-of-order
		} catch (InterruptedException ie) {
			;
		}
	}

	@Configuration
	@EnableCouchbaseRepositories("org.springframework.data.couchbase")
	@EnableCouchbaseAuditing(dateTimeProviderRef = "dateTimeProviderRef")
	static class Config extends AbstractCouchbaseConfiguration {

		@Override
		public String getConnectionString() {
			return connectionString();
		}

		@Override
		public String getUserName() {
			return config().adminUsername();
		}

		@Override
		public String getPassword() {
			return config().adminPassword();
		}

		@Override
		public String getBucketName() {
			return bucketName();
		}

		@Bean(name = "auditorAwareRef")
		public NaiveAuditorAware testAuditorAware() {
			return new NaiveAuditorAware();
		}

		@Override
		public void configureEnvironment(final ClusterEnvironment.Builder builder) {
			builder.ioConfig().maxHttpConnections(11).idleHttpConnectionTimeout(Duration.ofSeconds(4));
			return;
		}

		@Bean(name = "dateTimeProviderRef")
		public DateTimeProvider testDateTimeProvider() {
			return new AuditingDateTimeProvider();
		}

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

	@Configuration
	@EnableCouchbaseRepositories("org.springframework.data.couchbase")
	@EnableCouchbaseAuditing(auditorAwareRef = "auditorAwareRef", dateTimeProviderRef = "dateTimeProviderRef")
	static class ConfigRequestPlus extends AbstractCouchbaseConfiguration {

		@Override
		public String getConnectionString() {
			return connectionString();
		}

		@Override
		public String getUserName() {
			return config().adminUsername();
		}

		@Override
		public String getPassword() {
			return config().adminPassword();
		}

		@Override
		public String getBucketName() {
			return bucketName();
		}

		@Bean(name = "auditorAwareRef")
		public NaiveAuditorAware testAuditorAware() {
			return new NaiveAuditorAware();
		}

		@Bean(name = "dateTimeProviderRef")
		public DateTimeProvider testDateTimeProvider() {
			return new AuditingDateTimeProvider();
		}

		@Override
		public QueryScanConsistency getDefaultConsistency() {
			return REQUEST_PLUS;
		}
	}
}
