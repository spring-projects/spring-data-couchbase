package org.springframework.data.couchbase.repository.auditing;

import static org.springframework.data.couchbase.config.BeanNames.*;

import java.lang.annotation.Annotation;
import org.springframework.beans.factory.config.BeanDefinition;
import org.springframework.beans.factory.support.AbstractBeanDefinition;
import org.springframework.beans.factory.support.BeanDefinitionBuilder;
import org.springframework.beans.factory.support.BeanDefinitionRegistry;
import org.springframework.data.auditing.ReactiveIsNewAwareAuditingHandler;
import org.springframework.data.auditing.config.AuditingBeanDefinitionRegistrarSupport;
import org.springframework.data.auditing.config.AuditingConfiguration;
import org.springframework.data.config.ParsingUtils;
import org.springframework.data.couchbase.core.mapping.event.ReactiveAuditingEntityCallback;
import org.springframework.util.Assert;


/**
 * A support registrar that allows to set up reactive auditing for Couchbase (including {@link org.springframework.data.auditing.ReactiveAuditingHandler} and {
 * IsNewStrategyFactory} set up). See {@link EnableReactiveCouchbaseAuditing} for the associated annotation.
 *
 * @author Jorge Rodríguez Martín
 * @since 4.2
 */
class ReactiveCouchbaseAuditingRegistrar extends AuditingBeanDefinitionRegistrarSupport {

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.auditing.config.AuditingBeanDefinitionRegistrarSupport#getAnnotation()
	 */
	@Override
	protected Class<? extends Annotation> getAnnotation() {
		return EnableReactiveCouchbaseAuditing.class;
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.auditing.config.AuditingBeanDefinitionRegistrarSupport#getAuditingHandlerBeanName()
	 */
	@Override
	protected String getAuditingHandlerBeanName() {
		return REACTIVE_COUCHBASE_AUDITING_HANDLER;
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.auditing.config.AuditingBeanDefinitionRegistrarSupport#getAuditHandlerBeanDefinitionBuilder(org.springframework.data.auditing.config.AuditingConfiguration)
	 */
	@Override
	protected BeanDefinitionBuilder getAuditHandlerBeanDefinitionBuilder(AuditingConfiguration configuration) {
		Assert.notNull(configuration, "AuditingConfiguration must not be null!");

		BeanDefinitionBuilder builder = BeanDefinitionBuilder.rootBeanDefinition(ReactiveIsNewAwareAuditingHandler.class);

		BeanDefinitionBuilder definition = BeanDefinitionBuilder.genericBeanDefinition(PersistentEntitiesFactoryBean.class);
		definition.setAutowireMode(AbstractBeanDefinition.AUTOWIRE_CONSTRUCTOR);

		builder.addConstructorArgValue(definition.getBeanDefinition());
		return configureDefaultAuditHandlerAttributes(configuration, builder);
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.auditing.config.AuditingBeanDefinitionRegistrarSupport#registerAuditListener(org.springframework.beans.factory.config.BeanDefinition, org.springframework.beans.factory.support.BeanDefinitionRegistry)
	 */
	@Override
	protected void registerAuditListenerBeanDefinition(BeanDefinition auditingHandlerDefinition,
			BeanDefinitionRegistry registry) {
		Assert.notNull(auditingHandlerDefinition, "BeanDefinition must not be null!");
		Assert.notNull(registry, "BeanDefinitionRegistry must not be null!");

		BeanDefinitionBuilder builder = BeanDefinitionBuilder.rootBeanDefinition(ReactiveAuditingEntityCallback.class);

		builder.addConstructorArgValue(ParsingUtils.getObjectFactoryBeanDefinition(getAuditingHandlerBeanName(), registry));
		builder.getRawBeanDefinition().setSource(auditingHandlerDefinition.getSource());

		registerInfrastructureBeanWithId(builder.getBeanDefinition(),
				ReactiveAuditingEntityCallback.class.getName(), registry);
	}

}
