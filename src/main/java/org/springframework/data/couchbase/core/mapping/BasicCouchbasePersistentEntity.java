/*
 * Copyright 2012-2022 the original author or authors
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *        https://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.springframework.data.couchbase.core.mapping;

import java.time.Duration;
import java.util.Calendar;
import java.util.TimeZone;
import java.util.concurrent.TimeUnit;

import org.springframework.context.EnvironmentAware;
import org.springframework.core.annotation.AnnotatedElementUtils;
import org.springframework.core.env.Environment;
import org.springframework.data.mapping.MappingException;
import org.springframework.data.mapping.model.BasicPersistentEntity;
import org.springframework.data.util.TypeInformation;
import org.springframework.util.Assert;
import org.springframework.util.StringUtils;

/**
 * The representation of a persistent entity.
 *
 * @author Michael Nitschinger
 * @author Mark Paluch
 * @author Michael Reiche
 */
public class BasicCouchbasePersistentEntity<T> extends BasicPersistentEntity<T, CouchbasePersistentProperty>
		implements CouchbasePersistentEntity<T>, EnvironmentAware {

	private Environment environment;

	/**
	 * Create a new entity.
	 *
	 * @param typeInformation the type information of the entity.
	 */
	public BasicCouchbasePersistentEntity(final TypeInformation<T> typeInformation) {
		super(typeInformation);
		validateExpirationConfiguration();
	}

	private void validateExpirationConfiguration() {
		Document annotation = getType().getAnnotation(Document.class);
		if (annotation != null && annotation.expiry() > 0 && StringUtils.hasLength(annotation.expiryExpression())) {
			String msg = String.format("Incorrect expiry configuration on class %s using %s. "
					+ "You cannot use 'expiry' and 'expiryExpression' at the same time", getType().getName(), annotation);
			throw new IllegalArgumentException(msg);
		}
	}

	@Override
	public void setEnvironment(Environment environment) {
		this.environment = environment;
	}

	// DATACOUCH-145: allows SDK's @Id annotation to be used
	@Override
	protected CouchbasePersistentProperty returnPropertyIfBetterIdPropertyCandidateOrNull(
			CouchbasePersistentProperty property) {
		if (!property.isIdProperty()) {
			return null;
		}

		if (!this.hasIdProperty()) {
			return property;
		}

		// check existing ID vs new candidate
		boolean currentSpringId = this.getIdProperty().isAnnotationPresent(org.springframework.data.annotation.Id.class);
		boolean candidateSpringId = property.isAnnotationPresent(org.springframework.data.annotation.Id.class);

		if (candidateSpringId && !currentSpringId) {
			// spring IDs will have priority over fields named id
			return property;
		} else if (currentSpringId && !candidateSpringId) {
			// spring IDs will have priority over fields named id
			return null;
		} else {
			// do not allow two @Id fields or two fields named id (possible via @Field)
			throw new MappingException(String.format(
					"Attempt to add id property %s but already have property %s registered as id. Check your mapping configuration!",
					property.getField(), getIdProperty().getField()));
		}

	}

	@Override
	@Deprecated
	public int getExpiry() {
		return getExpiry(AnnotatedElementUtils.findMergedAnnotation(getType(), Expiry.class), environment);
	}

	@Deprecated
	public static int getExpiry(Expiry annotation, Environment environment) {
		if (annotation == null) {
			return 0;
		}

		int expiryValue = getExpiryValue(annotation, environment);

		long secondsShift = annotation.expiryUnit().toSeconds(expiryValue);
		if (secondsShift > TTL_IN_SECONDS_INCLUSIVE_END) {
			// we want it to be represented as a UNIX timestamp style, seconds since Epoch in UTC
			Calendar cal = Calendar.getInstance(TimeZone.getTimeZone("UTC"));
			if (annotation.expiryUnit() == TimeUnit.DAYS) {
				// makes sure we won't lose resolution
				cal.add(Calendar.DAY_OF_MONTH, expiryValue);
			} else {
				// use the shift in seconds since resolution should be smaller
				cal.add(Calendar.SECOND, (int) secondsShift);
			}
			return (int) (cal.getTimeInMillis() / 1000); // note: Unix UTC time representation in int is okay until year 2038
		} else {
			return (int) secondsShift;
		}
	}

	@Override
	public Duration getExpiryDuration() {
		return getExpiryDuration(AnnotatedElementUtils.findMergedAnnotation(getType(), Expiry.class), environment);
	}

	private static Duration getExpiryDuration(Expiry annotation, Environment environment) {
		if (annotation == null) {
			return Duration.ZERO;
		}
		int expiryValue = getExpiryValue(annotation, environment);
		long secondsShift = annotation.expiryUnit().toSeconds(expiryValue);
		return Duration.ofSeconds(secondsShift);
	}

	private static int getExpiryValue(Expiry annotation, Environment environment) {
		int expiryValue = annotation.expiry();
		String expiryExpressionString = annotation.expiryExpression();
		if (StringUtils.hasLength(expiryExpressionString)) {
			Assert.notNull(environment, "Environment must be set to use 'expiryExpression'");
			String expiryWithReplacedPlaceholders = environment.resolveRequiredPlaceholders(expiryExpressionString);
			try {
				expiryValue = Integer.parseInt(expiryWithReplacedPlaceholders);
			} catch (NumberFormatException e) {
				throw new IllegalArgumentException(
						"Invalid Integer value for expiry expression: " + expiryWithReplacedPlaceholders);
			}
		}
		return expiryValue;
	}

	@Override
	public boolean isTouchOnRead() {
		org.springframework.data.couchbase.core.mapping.Document annotation = getType()
				.getAnnotation(org.springframework.data.couchbase.core.mapping.Document.class);
		return annotation == null ? false : annotation.touchOnRead() && getExpiry() > 0;
	}

	@Override
	public boolean hasTextScoreProperty() {
		return false;
	}

	@Override
	public CouchbasePersistentProperty getTextScoreProperty() {
		return null;
	}

}
