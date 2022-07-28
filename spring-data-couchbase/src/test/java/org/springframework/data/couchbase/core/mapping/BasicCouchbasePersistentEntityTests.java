/*
 * Copyright 2013-2022 the original author or authors.
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

package org.springframework.data.couchbase.core.mapping;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertThrows;

import java.util.Calendar;
import java.util.TimeZone;
import java.util.concurrent.TimeUnit;

import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Configuration;
import org.springframework.core.env.ConfigurableEnvironment;
import org.springframework.data.util.ClassTypeInformation;
import org.springframework.mock.env.MockPropertySource;
import org.springframework.test.context.TestPropertySource;
import org.springframework.test.context.junit.jupiter.SpringJUnitConfig;

@SpringJUnitConfig
@TestPropertySource(properties = { "valid.document.expiry = 10", "invalid.document.expiry = abc" })
public class BasicCouchbasePersistentEntityTests {

	@Autowired ConfigurableEnvironment environment;

	@Test
	void testNoExpiryByDefault() {
		CouchbasePersistentEntity<DefaultExpiry> entity = new BasicCouchbasePersistentEntity<>(
				ClassTypeInformation.from(DefaultExpiry.class));

		assertThat(entity.getExpiryDuration().getSeconds()).isEqualTo(0);
	}

	@Test
	void testDefaultExpiryUnitIsSeconds() {
		CouchbasePersistentEntity<DefaultExpiryUnit> entity = new BasicCouchbasePersistentEntity<>(
				ClassTypeInformation.from(DefaultExpiryUnit.class));

		assertThat(entity.getExpiryDuration().getSeconds()).isEqualTo(78);
	}

	@Test
	void testLargeExpiry30DaysStillInSeconds() {
		CouchbasePersistentEntity<LimitDaysExpiry> entityUnder = new BasicCouchbasePersistentEntity<>(
				ClassTypeInformation.from(LimitDaysExpiry.class));
		assertThat(entityUnder.getExpiryDuration().getSeconds()).isEqualTo(30 * 24 * 60 * 60);
	}

	@Test
	void testLargeExpiry31DaysIsConvertedToUnixUtcTime() {
		CouchbasePersistentEntity<OverLimitDaysExpiry> entityOver = new BasicCouchbasePersistentEntity<>(
				ClassTypeInformation.from(OverLimitDaysExpiry.class));

		int expiryOver = (int) entityOver.getExpiry();
		Calendar expected = Calendar.getInstance(TimeZone.getTimeZone("UTC"));
		expected.add(Calendar.DAY_OF_YEAR, 31);

		Calendar calendar = Calendar.getInstance(TimeZone.getTimeZone("UTC"));
		calendar.clear();
		calendar.add(Calendar.SECOND, expiryOver);
		assertThat(calendar.get(Calendar.YEAR)).isEqualTo(expected.get(Calendar.YEAR));
		assertThat(calendar.get(Calendar.MONTH)).isEqualTo(expected.get(Calendar.MONTH));
		assertThat(calendar.get(Calendar.DAY_OF_MONTH)).isEqualTo(expected.get(Calendar.DAY_OF_MONTH));
		assertThat(calendar.get(Calendar.HOUR_OF_DAY)).isEqualTo(expected.get(Calendar.HOUR_OF_DAY));
		assertThat(calendar.get(Calendar.MINUTE)).isEqualTo(expected.get(Calendar.MINUTE));
		assertThat(calendar.get(Calendar.SECOND)).isEqualTo(expected.get(Calendar.SECOND));
	}

	@Test
	void testLargeExpiryExpression31DaysIsConvertedToUnixUtcTime() {
		BasicCouchbasePersistentEntity<OverLimitDaysExpiryExpression> entityOver = new BasicCouchbasePersistentEntity<>(
				ClassTypeInformation.from(OverLimitDaysExpiryExpression.class));
		entityOver.setEnvironment(environment);

		int expiryOver = (int) entityOver.getExpiry();
		Calendar expected = Calendar.getInstance(TimeZone.getTimeZone("UTC"));
		expected.add(Calendar.DAY_OF_YEAR, 31);

		Calendar calendar = Calendar.getInstance(TimeZone.getTimeZone("UTC"));
		calendar.clear();
		calendar.add(Calendar.SECOND, expiryOver);
		assertThat(calendar.get(Calendar.YEAR)).isEqualTo(expected.get(Calendar.YEAR));
		assertThat(calendar.get(Calendar.MONTH)).isEqualTo(expected.get(Calendar.MONTH));
		assertThat(calendar.get(Calendar.DAY_OF_MONTH)).isEqualTo(expected.get(Calendar.DAY_OF_MONTH));
		assertThat(calendar.get(Calendar.HOUR_OF_DAY)).isEqualTo(expected.get(Calendar.HOUR_OF_DAY));
		assertThat(calendar.get(Calendar.MINUTE)).isEqualTo(expected.get(Calendar.MINUTE));
		assertThat(calendar.get(Calendar.SECOND)).isEqualTo(expected.get(Calendar.SECOND));
	}

	@Test
	void testLargeExpiry31DaysInSecondsIsConvertedToUnixUtcTime() {
		CouchbasePersistentEntity<OverLimitSecondsExpiry> entityOver = new BasicCouchbasePersistentEntity<>(
				ClassTypeInformation.from(OverLimitSecondsExpiry.class));

		int expiryOver = (int) entityOver.getExpiry();
		Calendar expected = Calendar.getInstance(TimeZone.getTimeZone("UTC"));
		expected.add(Calendar.DAY_OF_YEAR, 31);

		Calendar calendar = Calendar.getInstance(TimeZone.getTimeZone("UTC"));
		calendar.clear();
		calendar.add(Calendar.SECOND, expiryOver);
		assertThat(calendar.get(Calendar.YEAR)).isEqualTo(expected.get(Calendar.YEAR));
		assertThat(calendar.get(Calendar.MONTH)).isEqualTo(expected.get(Calendar.MONTH));
		assertThat(calendar.get(Calendar.DAY_OF_MONTH)).isEqualTo(expected.get(Calendar.DAY_OF_MONTH));
		assertThat(calendar.get(Calendar.HOUR_OF_DAY)).isEqualTo(expected.get(Calendar.HOUR_OF_DAY));
		assertThat(calendar.get(Calendar.MINUTE)).isEqualTo(expected.get(Calendar.MINUTE));
		assertThat(calendar.get(Calendar.SECOND)).isEqualTo(expected.get(Calendar.SECOND));
	}

	@Test
	void doesNotUseGetExpiry() {
		assertThat(getBasicCouchbasePersistentEntity(SimpleDocument.class).getExpiry()).isEqualTo(0);
	}

	@Test
	void usesGetExpiry() {
		assertThat(getBasicCouchbasePersistentEntity(SimpleDocumentWithExpiry.class).getExpiry()).isEqualTo(10);
	}

	@Test
	void doesNotUseIsUpdateExpiryForRead() {
		assertThat(getBasicCouchbasePersistentEntity(SimpleDocument.class).isTouchOnRead()).isFalse();
		assertThat(getBasicCouchbasePersistentEntity(SimpleDocumentWithExpiry.class).isTouchOnRead()).isFalse();
	}

	@Test
	void usesTouchOnRead() {
		assertThat(getBasicCouchbasePersistentEntity(SimpleDocumentWithTouchOnRead.class).isTouchOnRead()).isTrue();
	}

	@Test
	void usesGetExpiryExpression() {
		assertThat(getBasicCouchbasePersistentEntity(ConstantExpiryExpression.class).getExpiry()).isEqualTo(10);
	}

	@Test
	void usesGetExpiryFromValidExpression() {
		assertThat(getBasicCouchbasePersistentEntity(ExpiryWithValidExpression.class).getExpiry()).isEqualTo(10);
	}

	@Test
	void doesNotAllowUseExpiryFromInvalidExpression() {
		assertThrows(IllegalArgumentException.class,
				() -> getBasicCouchbasePersistentEntity(ExpiryWithInvalidExpression.class).getExpiry());
	}

	@Test
	void usesGetExpiryExpressionAndRespectsPropertyUpdates() {
		BasicCouchbasePersistentEntity entity = getBasicCouchbasePersistentEntity(ExpiryWithValidExpression.class);
		assertThat(entity.getExpiry()).isEqualTo(10);

		environment.getPropertySources().addFirst(new MockPropertySource().withProperty("valid.document.expiry", "20"));
		assertThat(entity.getExpiry()).isEqualTo(20);
	}

	@Test
	void failsIfExpiryExpressionMissesRequiredProperty() {
		assertThrows(IllegalArgumentException.class,
				() -> getBasicCouchbasePersistentEntity(ExpiryWithMissingProperty.class).getExpiry());
	}

	@Test
	void doesNotAllowUseExpiryAndExpressionSimultaneously() {
		assertThrows(IllegalArgumentException.class,
				() -> getBasicCouchbasePersistentEntity(ExpiryAndExpression.class).getExpiry());
	}

	private BasicCouchbasePersistentEntity getBasicCouchbasePersistentEntity(Class<?> clazz) {
		BasicCouchbasePersistentEntity basicCouchbasePersistentEntity = new BasicCouchbasePersistentEntity(
				ClassTypeInformation.from(clazz));
		basicCouchbasePersistentEntity.setEnvironment(environment);
		return basicCouchbasePersistentEntity;
	}

	@Configuration
	static class Config {}

	class SimpleDocument {}

	@Document(expiry = 10)
	class SimpleDocumentWithExpiry {}

	@Document(expiry = 10, touchOnRead = true)
	class SimpleDocumentWithTouchOnRead {}

	/**
	 * Simple POJO to test default expiry.
	 */
	@Document
	class DefaultExpiry {}

	/**
	 * Simple POJO to test default expiry unit.
	 */
	@Document(expiry = 78)
	class DefaultExpiryUnit {}

	/**
	 * Simple POJO to test limit expiry.
	 */
	@Document(expiry = 30, expiryUnit = TimeUnit.DAYS)
	class LimitDaysExpiry {}

	/**
	 * Simple POJO to test larger than 30 days expiry.
	 */
	@Document(expiry = 31, expiryUnit = TimeUnit.DAYS)
	class OverLimitDaysExpiry {}

	/**
	 * Simple POJO to test larger than 30 days expiry defined as an expression
	 */
	@Document(expiryExpression = "${document.expiry.larger.than.30days:31}", expiryUnit = TimeUnit.DAYS)
	class OverLimitDaysExpiryExpression {}

	/**
	 * Simple POJO to test larger than 30 days expiry, when expressed in default time unit (SECONDS).
	 */
	@Document(expiry = 31 * 24 * 60 * 60)
	class OverLimitSecondsExpiry {}

	/**
	 * Simple POJO to test constant expiry expression
	 */
	@Document(expiryExpression = "10")
	class ConstantExpiryExpression {}

	/**
	 * Simple POJO to test valid expiry expression by resolving simple property from environment
	 */
	@Document(expiryExpression = "${valid.document.expiry}")
	class ExpiryWithValidExpression {}

	/**
	 * Simple POJO to test invalid expiry expression
	 */
	@Document(expiryExpression = "${invalid.document.expiry}")
	class ExpiryWithInvalidExpression {}

	/**
	 * Simple POJO to test expiry expression logic failure to resolve property placeholder
	 */
	@Document(expiryExpression = "${missing.expiry}")
	class ExpiryWithMissingProperty {}

	/**
	 * Simple POJO to test that expiry and expiry expression cannot be used simultaneously
	 */
	@Document(expiry = 10, expiryExpression = "10")
	class ExpiryAndExpression {}

}
