/*
 * Copyright 2012-2021 the original author or authors
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

package org.springframework.data.couchbase.repository.auditing;

import java.lang.annotation.Annotation;

import org.springframework.beans.factory.config.BeanDefinition;
import org.springframework.beans.factory.support.AbstractBeanDefinition;
import org.springframework.beans.factory.support.BeanDefinitionBuilder;
import org.springframework.beans.factory.support.BeanDefinitionRegistry;
import org.springframework.beans.factory.support.RootBeanDefinition;
import org.springframework.core.type.AnnotationMetadata;
import org.springframework.data.auditing.AuditingHandler;
import org.springframework.data.auditing.IsNewAwareAuditingHandler;
import org.springframework.data.auditing.config.AuditingBeanDefinitionRegistrarSupport;
import org.springframework.data.auditing.config.AuditingConfiguration;
import org.springframework.data.config.ParsingUtils;
import org.springframework.data.couchbase.config.BeanNames;
import org.springframework.data.couchbase.core.mapping.CouchbaseMappingContext;
import org.springframework.data.couchbase.core.mapping.event.AuditingEntityCallback;
import org.springframework.data.couchbase.core.mapping.event.AuditingEventListener;
import org.springframework.util.Assert;

/**
 * A support registrar that allows to set up auditing for Couchbase (including {@link AuditingHandler} and {
 * IsNewStrategyFactory} set up). See {@link EnableCouchbaseAuditing} for the associated annotation.
 *
 * @author Thomas Darimont
 * @author Oliver Gierke
 * @author Simon Baslé
 * @author Michael Reiche
 * @author Jorge Rodríguez Martín
 */
public class CouchbaseAuditingRegistrar extends AuditingBeanDefinitionRegistrarSupport {

	@Override
	protected Class<? extends Annotation> getAnnotation() {
		return EnableCouchbaseAuditing.class;
	}

	@Override
	protected String getAuditingHandlerBeanName() {
		return BeanNames.COUCHBASE_AUDITING_HANDLER;
	}

	@Override
	public void registerBeanDefinitions(AnnotationMetadata annotationMetadata, BeanDefinitionRegistry registry) {
		Assert.notNull(annotationMetadata, "AnnotationMetadata must not be null!");
		Assert.notNull(registry, "BeanDefinitionRegistry must not be null!");

		ensureMappingContext(registry, annotationMetadata);
		super.registerBeanDefinitions(annotationMetadata, registry);
	}

	@Override
	protected BeanDefinitionBuilder getAuditHandlerBeanDefinitionBuilder(AuditingConfiguration configuration) {
		Assert.notNull(configuration, "AuditingConfiguration must not be null!");

		BeanDefinitionBuilder builder = BeanDefinitionBuilder.rootBeanDefinition(IsNewAwareAuditingHandler.class);

		BeanDefinitionBuilder definition = BeanDefinitionBuilder.genericBeanDefinition(PersistentEntitiesFactoryBean.class);
		definition.setAutowireMode(AbstractBeanDefinition.AUTOWIRE_CONSTRUCTOR);

		builder.addConstructorArgValue(definition.getBeanDefinition());
		return configureDefaultAuditHandlerAttributes(configuration, builder);
	}

	@Override
	protected void registerAuditListenerBeanDefinition(BeanDefinition auditingHandlerDefinition,
			BeanDefinitionRegistry registry) {
		Assert.notNull(auditingHandlerDefinition, "BeanDefinition must not be null!");
		Assert.notNull(registry, "BeanDefinitionRegistry must not be null!");

		// Register the AuditEntityCallback

		BeanDefinitionBuilder listenerBeanDefinitionBuilder = BeanDefinitionBuilder
				.rootBeanDefinition(AuditingEntityCallback.class);
		listenerBeanDefinitionBuilder
				.addConstructorArgValue(ParsingUtils.getObjectFactoryBeanDefinition(getAuditingHandlerBeanName(), registry));

		registerInfrastructureBeanWithId(listenerBeanDefinitionBuilder.getBeanDefinition(),
				AuditingEntityCallback.class.getName(), registry);

		// Register the AuditingEventListener

		BeanDefinitionBuilder listenerBeanDefinitionBuilder2 = BeanDefinitionBuilder
				.rootBeanDefinition(AuditingEventListener.class);
		listenerBeanDefinitionBuilder2
				.addConstructorArgValue(ParsingUtils.getObjectFactoryBeanDefinition(getAuditingHandlerBeanName(), registry));

		registerInfrastructureBeanWithId(listenerBeanDefinitionBuilder2.getBeanDefinition(),
				AuditingEventListener.class.getName(), registry);

	}

	private void ensureMappingContext(BeanDefinitionRegistry registry, Object source) {
		if (!registry.containsBeanDefinition(BeanNames.COUCHBASE_MAPPING_CONTEXT)) {
			RootBeanDefinition definition = new RootBeanDefinition(CouchbaseMappingContext.class);
			definition.setRole(BeanDefinition.ROLE_INFRASTRUCTURE);
			definition.setSource(source);

			registry.registerBeanDefinition(BeanNames.COUCHBASE_MAPPING_CONTEXT, definition);
		}
	}
}
