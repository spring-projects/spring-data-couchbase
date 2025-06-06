/*
 * Copyright 2012-2025 the original author or authors
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

package org.springframework.data.couchbase.core.mapping.event;

import jakarta.validation.ConstraintViolationException;
import jakarta.validation.Validator;

import java.util.Set;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.data.couchbase.core.mapping.CouchbaseDocument;
import org.springframework.util.Assert;
import org.springframework.validation.beanvalidation.LocalValidatorFactoryBean;

/**
 * javax.validation dependant entities validator. When it is registered as Spring component its automatically invoked
 * before entities are saved in database.
 *
 * @author Maciej Walkowiak
 * @author Michael Nitschinger
 * @author Mark Paluch
 * @author Michael Reiche
 */
public class ValidatingCouchbaseEventListener extends AbstractCouchbaseEventListener<Object> {

	private static final Logger LOG = LoggerFactory.getLogger(ValidatingCouchbaseEventListener.class);

	private final Validator validator;

	/**
	 * Creates a new {@link ValidatingCouchbaseEventListener} using the given {@link Validator}.
	 *
	 * @param validator must not be {@literal null}.
	 */
	public ValidatingCouchbaseEventListener(Validator validator) {
		Assert.notNull(validator, "Validator must not be null!");
		this.validator = validator;
	}

	@Override
	@SuppressWarnings({ "rawtypes", "unchecked" })
	public void onBeforeSave(Object source, CouchbaseDocument dbo) {

		if (LOG.isDebugEnabled()) {
			LOG.debug("Validating object: {}", source);
		}
		Set violations = validator.validate(source);

		if (!violations.isEmpty()) {
			LOG.info("During object: {} validation violations found: {}", source, violations);
			throw new ConstraintViolationException(violations);
		}
	}

}
