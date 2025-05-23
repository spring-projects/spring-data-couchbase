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

package org.springframework.data.couchbase.core.mapping;

import java.util.Optional;

import org.springframework.beans.BeansException;
import org.springframework.context.ApplicationContext;
import org.springframework.context.ApplicationContextAware;
import org.springframework.context.ApplicationEventPublisher;
import org.springframework.data.couchbase.core.index.CouchbasePersistentEntityIndexCreator;
import org.springframework.data.mapping.context.AbstractMappingContext;
import org.springframework.data.mapping.context.MappingContextEvent;
import org.springframework.data.mapping.model.FieldNamingStrategy;
import org.springframework.data.mapping.model.Property;
import org.springframework.data.mapping.model.PropertyNameFieldNamingStrategy;
import org.springframework.data.mapping.model.SimpleTypeHolder;
import org.springframework.data.util.TypeInformation;

/**
 * Default implementation of a {@link org.springframework.data.mapping.context.MappingContext} for Couchbase using
 * {@link BasicCouchbasePersistentEntity} and {@link BasicCouchbasePersistentProperty} as primary abstractions.
 *
 * @author Michael Nitschinger
 * @author Michael Reiche
 */
public class CouchbaseMappingContext
		extends AbstractMappingContext<BasicCouchbasePersistentEntity<?>, CouchbasePersistentProperty>
		implements ApplicationContextAware {

	/**
	 * The default field naming strategy.
	 */
	private static final FieldNamingStrategy DEFAULT_NAMING_STRATEGY = PropertyNameFieldNamingStrategy.INSTANCE;
	/**
	 * Contains the application context to configure the application.
	 */
	private ApplicationContext context;
	/**
	 * The field naming strategy to use.
	 */
	private FieldNamingStrategy fieldNamingStrategy = DEFAULT_NAMING_STRATEGY;

	private boolean autoIndexCreation = true;
	private ApplicationEventPublisher eventPublisher;
	private CouchbasePersistentEntityIndexCreator indexCreator = null;

	/**
	 * Configures the {@link FieldNamingStrategy} to be used to determine the field name if no manual mapping is applied.
	 * Defaults to a strategy using the plain property name.
	 *
	 * @param fieldNamingStrategy the {@link FieldNamingStrategy} to be used to determine the field name if no manual
	 *          mapping is applied.
	 */
	public void setFieldNamingStrategy(final FieldNamingStrategy fieldNamingStrategy) {
		this.fieldNamingStrategy = fieldNamingStrategy == null ? DEFAULT_NAMING_STRATEGY : fieldNamingStrategy;
	}

	/**
	 * Creates a concrete entity based out of the type information passed.
	 *
	 * @param typeInformation type information of the entity to create.
	 * @param <T> the type for the corresponding type information.
	 * @return the constructed entity.
	 */
	@Override
	protected <T> BasicCouchbasePersistentEntity<?> createPersistentEntity(final TypeInformation<T> typeInformation) {
		BasicCouchbasePersistentEntity<T> entity = new BasicCouchbasePersistentEntity<T>(typeInformation);
		if (context != null) {
			entity.setEnvironment(context.getEnvironment());
		}
		return entity;
	}

	/**
	 * Creates a concrete property based on the field information and entity.
	 *
	 * @param property the property descriptor.
	 * @param owner the entity which owns the property.
	 * @param simpleTypeHolder the type holder.
	 * @return the constructed property.
	 */
	@Override
	protected CouchbasePersistentProperty createPersistentProperty(Property property,
			final BasicCouchbasePersistentEntity<?> owner, final SimpleTypeHolder simpleTypeHolder) {
		return new BasicCouchbasePersistentProperty(property, owner, simpleTypeHolder, fieldNamingStrategy);
	}

	/**
	 * Sets (or overrides) the current application context.
	 *
	 * @param applicationContext the application context to be assigned.
	 * @throws BeansException if the context can not be set properly.
	 */
	@Override
	public void setApplicationContext(final ApplicationContext applicationContext) throws BeansException {
		context = applicationContext;
		super.setApplicationContext(applicationContext);
	}

	@Override
	public void setApplicationEventPublisher(ApplicationEventPublisher applicationEventPublisher) {
		eventPublisher = applicationEventPublisher;
		if (this.eventPublisher == null) {
			this.eventPublisher = context;
		}
	}

	public boolean isAutoIndexCreation() {
		return autoIndexCreation;
	}

	public void setAutoIndexCreation(boolean autoCreateIndexes) {
		this.autoIndexCreation = autoCreateIndexes;
	}

	/**
	 * override method from AbstractMappingContext as that method will not publishEvent() if it finds the entity has
	 * already been cached
	 *
	 * @param typeInformation - entity type
	 */
	@Override
	protected Optional<BasicCouchbasePersistentEntity<?>> addPersistentEntity(TypeInformation<?> typeInformation) {
		Optional<BasicCouchbasePersistentEntity<?>> entity = super.addPersistentEntity(typeInformation);

		if (this.eventPublisher != null && entity.isPresent()) {
			if (this.indexCreator != null) {
				if (!indexCreator.hasSeen(entity.get())) {
					this.eventPublisher.publishEvent(new MappingContextEvent(this, entity.get()));
				}
			}
		}
		return entity;
	}

	/**
	 * override method from AbstractMappingContext as that method will not publishEvent() if it finds the entity has
	 * already been cached. Instead, user our own addPersistEntity that will.
	 * 
	 * @param typeInformation - entity type
	 */
	@Override
	public BasicCouchbasePersistentEntity<?> getPersistentEntity(TypeInformation<?> typeInformation) {
		Optional<BasicCouchbasePersistentEntity<?>> entity = addPersistentEntity(typeInformation);
		return entity.isPresent() ? entity.get() : null;
	}

	/**
	 * capture the indexCreator when it has been added as a listener. only publishEvent() if the indexCreator hasn't
	 * already seen the class.
	 * 
	 * @param indexCreator
	 */
	public void setIndexCreator(CouchbasePersistentEntityIndexCreator indexCreator) {
		this.indexCreator = indexCreator;
	}
}
