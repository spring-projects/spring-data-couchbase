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

import org.springframework.beans.BeansException;
import org.springframework.context.ApplicationContext;
import org.springframework.context.ApplicationContextAware;
import org.springframework.data.mapping.context.AbstractMappingContext;
import org.springframework.data.mapping.model.SimpleTypeHolder;
import org.springframework.data.util.TypeInformation;

import java.beans.PropertyDescriptor;
import java.lang.reflect.Field;

/**
 * Default implementation of a {@link org.springframework.data.mapping.context.MappingContext} for Couchbase using
 * {@link BasicCouchbasePersistentEntity} and {@link BasicCouchbasePersistentProperty} as primary abstractions.
 *
 * @author Michael Nitschinger
 */
public class CouchbaseMappingContext
  extends AbstractMappingContext<BasicCouchbasePersistentEntity<?>, CouchbasePersistentProperty>
  implements ApplicationContextAware {

  /**
   * Contains the application context to configure the application.
   */
  private ApplicationContext context;

  /**
   * The default field naming strategy.
   */
  private static final FieldNamingStrategy DEFAULT_NAMING_STRATEGY = FallbackFieldNamingStrategy.INSTANCE;

  /**
   * The field naming strategy to use.
   */
  private FieldNamingStrategy fieldNamingStrategy = DEFAULT_NAMING_STRATEGY;

  /**
   * Configures the {@link FieldNamingStrategy} to be used to determine the field name if no manual mapping is applied.
   * Defaults to a strategy using the plain property name.
   *
   * @param fieldNamingStrategy the {@link FieldNamingStrategy} to be used to determine the field name if no manual
   *  mapping is applied.
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
      entity.setApplicationContext(context);
    }
    return entity;
  }

  /**
   * Creates a concrete property based on the field information and entity.
   *
   * @param field the reflection on the field to be used as a property.
   * @param descriptor the property descriptor.
   * @param owner the entity which owns the property.
   * @param simpleTypeHolder the type holder.
   * @return the constructed property.
   */
  @Override
  protected CouchbasePersistentProperty createPersistentProperty(final Field field, final PropertyDescriptor descriptor,
    final BasicCouchbasePersistentEntity<?> owner, final SimpleTypeHolder simpleTypeHolder) {
    return new BasicCouchbasePersistentProperty(field, descriptor, owner, simpleTypeHolder, fieldNamingStrategy);
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
	}

}
