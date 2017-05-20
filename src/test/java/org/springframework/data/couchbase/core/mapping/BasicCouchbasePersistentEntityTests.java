/*
 * Copyright 2013 the original author or authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.springframework.data.couchbase.core.mapping;

import static org.junit.Assert.*;

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

    assertEquals(0, entity.getExpiry());
  }

  @Test
  public void testDefaultExpiryUnitIsSeconds() {
    CouchbasePersistentEntity<DefaultExpiryUnit> entity = new BasicCouchbasePersistentEntity<DefaultExpiryUnit>(
        ClassTypeInformation.from(DefaultExpiryUnit.class));

    assertEquals(78, entity.getExpiry());
  }

  @Test
  public void testLargeExpiry30DaysStillInSeconds() {
    CouchbasePersistentEntity<LimitDaysExpiry> entityUnder = new BasicCouchbasePersistentEntity<LimitDaysExpiry>(
        ClassTypeInformation.from(LimitDaysExpiry.class));
    assertEquals(30 * 24 * 60 * 60, entityUnder.getExpiry());
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
    assertEquals(expected.get(Calendar.YEAR), calendar.get(Calendar.YEAR));
    assertEquals(expected.get(Calendar.MONTH), calendar.get(Calendar.MONTH));
    assertEquals(expected.get(Calendar.DAY_OF_MONTH), calendar.get(Calendar.DAY_OF_MONTH));
    assertEquals(expected.get(Calendar.HOUR_OF_DAY), calendar.get(Calendar.HOUR_OF_DAY));
    assertEquals(expected.get(Calendar.MINUTE), calendar.get(Calendar.MINUTE));
    assertEquals(expected.get(Calendar.SECOND), calendar.get(Calendar.SECOND));
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
    assertEquals(expected.get(Calendar.YEAR), calendar.get(Calendar.YEAR));
    assertEquals(expected.get(Calendar.MONTH), calendar.get(Calendar.MONTH));
    assertEquals(expected.get(Calendar.DAY_OF_MONTH), calendar.get(Calendar.DAY_OF_MONTH));
    assertEquals(expected.get(Calendar.HOUR_OF_DAY), calendar.get(Calendar.HOUR_OF_DAY));
    assertEquals(expected.get(Calendar.MINUTE), calendar.get(Calendar.MINUTE));
    assertEquals(expected.get(Calendar.SECOND), calendar.get(Calendar.SECOND));
  }

  @Test
  public void doesNotUseGetExpiry() throws Exception {
    assertEquals(0, getBasicCouchbasePersistentEntity(SimpleDocument.class).getExpiry());
  }

  @Test
  public void usesGetExpiry() throws Exception {
    assertEquals(10, getBasicCouchbasePersistentEntity(SimpleDocumentWithExpiry.class).getExpiry());
  }

  @Test
  public void doesNotUseIsUpdateExpiryForRead() throws Exception {
    assertFalse(getBasicCouchbasePersistentEntity(SimpleDocument.class).isTouchOnRead());
    assertFalse(getBasicCouchbasePersistentEntity(SimpleDocumentWithExpiry.class).isTouchOnRead());
  }

  @Test
  public void usesTouchOnRead() throws Exception {
    assertTrue(getBasicCouchbasePersistentEntity(SimpleDocumentWithTouchOnRead.class).isTouchOnRead());
  }

  @Test
  public void usesGetExpiryExpression() throws Exception {
    assertEquals(10, getBasicCouchbasePersistentEntity(ConstantExpiryExpression.class).getExpiry());
  }

  @Test
  public void usesGetExpiryFromValidExpression() throws Exception {
    assertEquals(10, getBasicCouchbasePersistentEntity(ExpiryWithValidExpression.class).getExpiry());
  }

  @Test
  public void doesNotAllowUseExpiryFromInvalidExpression() throws Exception {
    expectedException.expect(IllegalArgumentException.class);
    expectedException.expectMessage("Invalid Integer value for expiry expression: abc");
    assertEquals(10, getBasicCouchbasePersistentEntity(ExpiryWithInvalidExpression.class).getExpiry());
  }

  @Test
  public void usesGetExpiryExpressionAndRespectsPropertyUpdates() throws Exception {
    BasicCouchbasePersistentEntity entity = getBasicCouchbasePersistentEntity(ExpiryWithValidExpression.class);
    assertEquals(10, entity.getExpiry());

    environment.getPropertySources().addFirst(new MockPropertySource().withProperty("valid.document.expiry", "20"));
    assertEquals(20, entity.getExpiry());
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
