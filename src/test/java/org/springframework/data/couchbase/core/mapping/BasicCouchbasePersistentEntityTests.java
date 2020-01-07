/*
 * Copyright 2013-2020 the original author or authors.
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

import java.util.Calendar;
import java.util.Date;
import java.util.TimeZone;
import java.util.concurrent.TimeUnit;

import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.junit.runner.RunWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.core.env.ConfigurableEnvironment;
import org.springframework.data.util.ClassTypeInformation;
import org.springframework.mock.env.MockPropertySource;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.TestPropertySource;
import org.springframework.test.context.junit4.SpringJUnit4ClassRunner;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Verifies the correct behavior of annotation at the class level on persistable objects.
 *
 * @author Simon Baslé
 */
@RunWith(SpringJUnit4ClassRunner.class)
@TestPropertySource(properties = {
		"valid.document.expiry = 10",
		"invalid.document.expiry = abc"
})
@ContextConfiguration(classes = BasicCouchbasePersistentEntityTests.class)
public class BasicCouchbasePersistentEntityTests {

  @Rule
  public ExpectedException expectedException = ExpectedException.none();

  @Autowired
  ConfigurableEnvironment environment;

  @Test
  public void testNoExpiryByDefault() {
    CouchbasePersistentEntity<DefaultExpiry> entity = new BasicCouchbasePersistentEntity<DefaultExpiry>(
        ClassTypeInformation.from(DefaultExpiry.class));

    assertThat(entity.getExpiry()).isEqualTo(0);
  }

  @Test
  public void testDefaultExpiryUnitIsSeconds() {
    CouchbasePersistentEntity<DefaultExpiryUnit> entity = new BasicCouchbasePersistentEntity<DefaultExpiryUnit>(
        ClassTypeInformation.from(DefaultExpiryUnit.class));

    assertThat(entity.getExpiry()).isEqualTo(78);
  }

  @Test
  public void testLargeExpiry30DaysStillInSeconds() {
    CouchbasePersistentEntity<LimitDaysExpiry> entityUnder = new BasicCouchbasePersistentEntity<LimitDaysExpiry>(
        ClassTypeInformation.from(LimitDaysExpiry.class));
    assertThat(entityUnder.getExpiry()).isEqualTo(30 * 24 * 60 * 60);
  }

  @Test
  public void testLargeExpiry31DaysIsConvertedToUnixUtcTime() {
    CouchbasePersistentEntity<OverLimitDaysExpiry> entityOver = new BasicCouchbasePersistentEntity<OverLimitDaysExpiry>(
        ClassTypeInformation.from(OverLimitDaysExpiry.class));

    int expiryOver = entityOver.getExpiry();
    Calendar expected = Calendar.getInstance(TimeZone.getTimeZone("UTC"));
    expected.add(Calendar.DAY_OF_YEAR, 31);

    Date dateOver = new Date(expiryOver * 1000L);
    System.out.println(entityOver + " => " + dateOver);

    Calendar calendar = Calendar.getInstance(TimeZone.getTimeZone("UTC"));
    calendar.clear();
    calendar.add(Calendar.SECOND, expiryOver);
    assertThat(calendar.get(Calendar.YEAR)).isEqualTo(expected.get(Calendar.YEAR));
    assertThat(calendar.get(Calendar.MONTH)).isEqualTo(expected.get(Calendar.MONTH));
    assertThat(calendar.get(Calendar.DAY_OF_MONTH))
			.isEqualTo(expected.get(Calendar.DAY_OF_MONTH));
    assertThat(calendar.get(Calendar.HOUR_OF_DAY))
			.isEqualTo(expected.get(Calendar.HOUR_OF_DAY));
    assertThat(calendar.get(Calendar.MINUTE)).isEqualTo(expected.get(Calendar.MINUTE));
    assertThat(calendar.get(Calendar.SECOND)).isEqualTo(expected.get(Calendar.SECOND));
  }

  @Test
  public void testLargeExpiryExpression31DaysIsConvertedToUnixUtcTime() {
	BasicCouchbasePersistentEntity<OverLimitDaysExpiryExpression> entityOver = new BasicCouchbasePersistentEntity<OverLimitDaysExpiryExpression>(
        ClassTypeInformation.from(OverLimitDaysExpiryExpression.class));
    entityOver.setEnvironment(environment);

    int expiryOver = entityOver.getExpiry();
    Calendar expected = Calendar.getInstance(TimeZone.getTimeZone("UTC"));
    expected.add(Calendar.DAY_OF_YEAR, 31);

    Date dateOver = new Date(expiryOver * 1000L);
    System.out.println(entityOver + " => " + dateOver);

    Calendar calendar = Calendar.getInstance(TimeZone.getTimeZone("UTC"));
    calendar.clear();
    calendar.add(Calendar.SECOND, expiryOver);
    assertThat(calendar.get(Calendar.YEAR)).isEqualTo(expected.get(Calendar.YEAR));
    assertThat(calendar.get(Calendar.MONTH)).isEqualTo(expected.get(Calendar.MONTH));
    assertThat(calendar.get(Calendar.DAY_OF_MONTH))
			.isEqualTo(expected.get(Calendar.DAY_OF_MONTH));
    assertThat(calendar.get(Calendar.HOUR_OF_DAY))
			.isEqualTo(expected.get(Calendar.HOUR_OF_DAY));
    assertThat(calendar.get(Calendar.MINUTE)).isEqualTo(expected.get(Calendar.MINUTE));
    assertThat(calendar.get(Calendar.SECOND)).isEqualTo(expected.get(Calendar.SECOND));
  }

  @Test
  public void testLargeExpiry31DaysInSecondsIsConvertedToUnixUtcTime() {
    CouchbasePersistentEntity<OverLimitSecondsExpiry> entityOver = new BasicCouchbasePersistentEntity<OverLimitSecondsExpiry>(
        ClassTypeInformation.from(OverLimitSecondsExpiry.class));

    int expiryOver = entityOver.getExpiry();
    Calendar expected = Calendar.getInstance(TimeZone.getTimeZone("UTC"));
    expected.add(Calendar.DAY_OF_YEAR, 31);

    Date dateOver = new Date(expiryOver * 1000L);
    System.out.println(entityOver + " => " + dateOver);

    Calendar calendar = Calendar.getInstance(TimeZone.getTimeZone("UTC"));
    calendar.clear();
    calendar.add(Calendar.SECOND, expiryOver);
    assertThat(calendar.get(Calendar.YEAR)).isEqualTo(expected.get(Calendar.YEAR));
    assertThat(calendar.get(Calendar.MONTH)).isEqualTo(expected.get(Calendar.MONTH));
    assertThat(calendar.get(Calendar.DAY_OF_MONTH))
			.isEqualTo(expected.get(Calendar.DAY_OF_MONTH));
    assertThat(calendar.get(Calendar.HOUR_OF_DAY))
			.isEqualTo(expected.get(Calendar.HOUR_OF_DAY));
    assertThat(calendar.get(Calendar.MINUTE)).isEqualTo(expected.get(Calendar.MINUTE));
    assertThat(calendar.get(Calendar.SECOND)).isEqualTo(expected.get(Calendar.SECOND));
  }

  @Test
  public void doesNotUseGetExpiry() throws Exception {
    assertThat(getBasicCouchbasePersistentEntity(SimpleDocument.class).getExpiry())
			.isEqualTo(0);
  }

  @Test
  public void usesGetExpiry() throws Exception {
    assertThat(getBasicCouchbasePersistentEntity(SimpleDocumentWithExpiry.class)
			.getExpiry()).isEqualTo(10);
  }

  @Test
  public void doesNotUseIsUpdateExpiryForRead() throws Exception {
    assertThat(getBasicCouchbasePersistentEntity(SimpleDocument.class).isTouchOnRead())
			.isFalse();
    assertThat(getBasicCouchbasePersistentEntity(SimpleDocumentWithExpiry.class)
			.isTouchOnRead()).isFalse();
  }

  @Test
  public void usesTouchOnRead() throws Exception {
    assertThat(getBasicCouchbasePersistentEntity(SimpleDocumentWithTouchOnRead.class)
			.isTouchOnRead()).isTrue();
  }

  @Test
  public void usesGetExpiryExpression() throws Exception {
    assertThat(getBasicCouchbasePersistentEntity(ConstantExpiryExpression.class)
			.getExpiry()).isEqualTo(10);
  }

  @Test
  public void usesGetExpiryFromValidExpression() throws Exception {
    assertThat(getBasicCouchbasePersistentEntity(ExpiryWithValidExpression.class)
			.getExpiry()).isEqualTo(10);
  }

  @Test
  public void doesNotAllowUseExpiryFromInvalidExpression() throws Exception {
    expectedException.expect(IllegalArgumentException.class);
    expectedException.expectMessage("Invalid Integer value for expiry expression: abc");
    assertThat(getBasicCouchbasePersistentEntity(ExpiryWithInvalidExpression.class)
			.getExpiry()).isEqualTo(10);
  }

  @Test
  public void usesGetExpiryExpressionAndRespectsPropertyUpdates() throws Exception {
    BasicCouchbasePersistentEntity entity = getBasicCouchbasePersistentEntity(ExpiryWithValidExpression.class);
    assertThat(entity.getExpiry()).isEqualTo(10);

    environment.getPropertySources().addFirst(new MockPropertySource().withProperty("valid.document.expiry", "20"));
    assertThat(entity.getExpiry()).isEqualTo(20);
  }

  @Test
  public void failsIfExpiryExpressionMissesRequiredProperty() {
    expectedException.expect(IllegalArgumentException.class);
    expectedException.expectMessage("Could not resolve placeholder 'missing.expiry'");
    getBasicCouchbasePersistentEntity(ExpiryWithMissingProperty.class).getExpiry();
  }

  @Test
  public void doesNotAllowUseExpiryAndExpressionSimultaneously() throws Exception {
    expectedException.expect(IllegalArgumentException.class);
    expectedException.expectMessage("You cannot use 'expiry' and 'expiryExpression' at the same time");
    expectedException.expectMessage(ExpiryAndExpression.class.getName());
    getBasicCouchbasePersistentEntity(ExpiryAndExpression.class).getExpiry();
  }

  private BasicCouchbasePersistentEntity getBasicCouchbasePersistentEntity(Class<?> clazz) {
    BasicCouchbasePersistentEntity basicCouchbasePersistentEntity = new BasicCouchbasePersistentEntity(ClassTypeInformation.from(clazz));
    basicCouchbasePersistentEntity.setEnvironment(environment);
    return basicCouchbasePersistentEntity;
  }

  public static class SimpleDocument {
  }

  @Document(expiry = 10)
  public static class SimpleDocumentWithExpiry {
  }

  @Document(expiry = 10, touchOnRead = true)
  public static class SimpleDocumentWithTouchOnRead {
  }

  /**
   * Simple POJO to test default expiry.
   */
  @Document
  private class DefaultExpiry {
  }

  /**
   * Simple POJO to test default expiry unit.
   */
  @Document(expiry = 78)
  private class DefaultExpiryUnit {
  }

  /**
   * Simple POJO to test limit expiry.
   */
  @Document(expiry = 30, expiryUnit = TimeUnit.DAYS)
  private class LimitDaysExpiry {
  }

  /**
   * Simple POJO to test larger than 30 days expiry.
   */
  @Document(expiry = 31, expiryUnit = TimeUnit.DAYS)
  public class OverLimitDaysExpiry {
  }

  /**
   * Simple POJO to test larger than 30 days expiry defined as an expression
   */
  @Document(expiryExpression = "${document.expiry.larger.than.30days:31}", expiryUnit = TimeUnit.DAYS)
  public class OverLimitDaysExpiryExpression {
  }

  /**
   * Simple POJO to test larger than 30 days expiry, when expressed in default time unit (SECONDS).
   */
  @Document(expiry = 31 * 24 * 60 * 60)
  public class OverLimitSecondsExpiry {
  }

  /**
   * Simple POJO to test constant expiry expression
   */
  @Document(expiryExpression = "10")
  private class ConstantExpiryExpression {
  }

  /**
   * Simple POJO to test valid expiry expression by resolving simple property from environment
   */
  @Document(expiryExpression = "${valid.document.expiry}")
  private class ExpiryWithValidExpression {
  }

  /**
   * Simple POJO to test invalid expiry expression
   */
  @Document(expiryExpression = "${invalid.document.expiry}")
  private class ExpiryWithInvalidExpression {
  }

  /**
   * Simple POJO to test expiry expression logic failure to resolve property placeholder
   */
  @Document(expiryExpression = "${missing.expiry}")
  private class ExpiryWithMissingProperty {
  }

  /**
   * Simple POJO to test that expiry and expiry expression cannot be used simultaneously
   */
  @Document(expiry = 10, expiryExpression = "10")
  private class ExpiryAndExpression {
  }

}
