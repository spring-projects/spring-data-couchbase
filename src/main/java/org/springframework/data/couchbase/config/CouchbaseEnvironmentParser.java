/*
 * Copyright 2012-2017 the original author or authors
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *        http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.springframework.data.couchbase.config;

import static org.springframework.data.config.ParsingUtils.setPropertyValue;

import com.couchbase.client.core.retry.RetryStrategy;
import com.couchbase.client.java.env.DefaultCouchbaseEnvironment;
import org.w3c.dom.Element;

import org.springframework.beans.factory.support.AbstractBeanDefinition;
import org.springframework.beans.factory.support.BeanDefinitionBuilder;
import org.springframework.beans.factory.xml.AbstractSingleBeanDefinitionParser;
import org.springframework.beans.factory.xml.ParserContext;
import org.springframework.util.StringUtils;

/**
 * Allows creation of a {@link DefaultCouchbaseEnvironment} via spring XML configuration.
 * <p>
 * The following properties are supported:<br/><ul>
 * <li>{@link DefaultCouchbaseEnvironment.Builder#managementTimeout(long) managementTimeout}</li>
 * <li>{@link DefaultCouchbaseEnvironment.Builder#queryTimeout(long) queryTimeout}</li>
 * <li>{@link DefaultCouchbaseEnvironment.Builder#viewTimeout(long) viewTimeout}</li>
 * <li>{@link DefaultCouchbaseEnvironment.Builder#kvTimeout(long) kvTimeout}</li>
 * <li>{@link DefaultCouchbaseEnvironment.Builder#connectTimeout(long) connectTimeout}</li>
 * <li>{@link DefaultCouchbaseEnvironment.Builder#disconnectTimeout(long) disconnectTimeout}</li>
 * <li>{@link DefaultCouchbaseEnvironment.Builder#dnsSrvEnabled(boolean) dnsSrvEnabled}</li>
 *
 * <li>{@link DefaultCouchbaseEnvironment.Builder#dcpEnabled(boolean) dcpEnabled}</li>
 * <li>{@link DefaultCouchbaseEnvironment.Builder#sslEnabled(boolean) sslEnabled}</li>
 * <li>{@link DefaultCouchbaseEnvironment.Builder#sslKeystoreFile(String) sslKeystoreFile}</li>
 * <li>{@link DefaultCouchbaseEnvironment.Builder#sslKeystorePassword(String) sslKeystorePassword}</li>
 * <li>{@link DefaultCouchbaseEnvironment.Builder#bootstrapHttpEnabled(boolean) bootstrapHttpEnabled}</li>
 * <li>{@link DefaultCouchbaseEnvironment.Builder#bootstrapCarrierEnabled(boolean) bootstrapCarrierEnabled}</li>
 * <li>{@link DefaultCouchbaseEnvironment.Builder#bootstrapHttpDirectPort(int) bootstrapHttpDirectPort}</li>
 * <li>{@link DefaultCouchbaseEnvironment.Builder#bootstrapHttpSslPort(int) bootstrapHttpSslPort}</li>
 * <li>{@link DefaultCouchbaseEnvironment.Builder#bootstrapCarrierDirectPort(int) bootstrapCarrierDirectPort}</li>
 * <li>{@link DefaultCouchbaseEnvironment.Builder#bootstrapCarrierSslPort(int) bootstrapCarrierSslPort}</li>
 * <li>{@link DefaultCouchbaseEnvironment.Builder#ioPoolSize(int) ioPoolSize}</li>
 * <li>{@link DefaultCouchbaseEnvironment.Builder#computationPoolSize(int) computationPoolSize}</li>
 * <li>{@link DefaultCouchbaseEnvironment.Builder#responseBufferSize(int) responseBufferSize}</li>
 * <li>{@link DefaultCouchbaseEnvironment.Builder#requestBufferSize(int) requestBufferSize}</li>
 * <li>{@link DefaultCouchbaseEnvironment.Builder#kvEndpoints(int) kvEndpoints}</li>
 * <li>{@link DefaultCouchbaseEnvironment.Builder#viewEndpoints(int) viewEndpoints}</li>
 * <li>{@link DefaultCouchbaseEnvironment.Builder#queryEndpoints(int) queryEndpoints}</li>
 * <li>{@link DefaultCouchbaseEnvironment.Builder#retryStrategy(RetryStrategy) retryStrategy}</li>
 * <li>{@link DefaultCouchbaseEnvironment.Builder#maxRequestLifetime(long) maxRequestLifetime}</li>
 * <li>{@link DefaultCouchbaseEnvironment.Builder#keepAliveInterval(long) keepAliveInterval}</li>
 * <li>{@link DefaultCouchbaseEnvironment.Builder#autoreleaseAfter(long) autoreleaseAfter}</li>
 * <li>{@link DefaultCouchbaseEnvironment.Builder#bufferPoolingEnabled(boolean) bufferPoolingEnabled}</li>
 * <li>{@link DefaultCouchbaseEnvironment.Builder#tcpNodelayEnabled(boolean) tcpNodelayEnabled}</li>
 * <li>{@link DefaultCouchbaseEnvironment.Builder#mutationTokensEnabled(boolean) mutationTokensEnabled}</li>
 * </ul>
 *
 * @author Simon Baslé
 * @author Subhashni Balakrishnan
 */
public class CouchbaseEnvironmentParser extends AbstractSingleBeanDefinitionParser {

	/**
	 * Resolve the bean ID and assign a default if not set.
	 *
	 * @param element the XML element which contains the attributes.
	 * @param definition the bean definition to work with.
	 * @param parserContext encapsulates the parsing state and configuration.
	 * @return the ID to work with.
	 */
	@Override
	protected String resolveId(final Element element, final AbstractBeanDefinition definition, final ParserContext parserContext) {
		String id = super.resolveId(element, definition, parserContext);
		return StringUtils.hasText(id) ? id : BeanNames.COUCHBASE_ENV;
	}

	/**
	 * Defines the bean class that will be constructed.
	 *
	 * @param element the XML element which contains the attributes.
	 * @return the class type to instantiate.
	 */
	@Override
	protected Class getBeanClass(final Element element) {
		return CouchbaseEnvironmentFactoryBean.class;
	}

	/**
	 * Parse the bean definition and build up the bean.
	 *
	 * @param envElement the XML element which contains the attributes.
	 * @param envDefinitionBuilder the builder which builds the bean.
	 */
	@Override
	protected void doParse(final Element envElement, final BeanDefinitionBuilder envDefinitionBuilder) {
		setPropertyValue(envDefinitionBuilder, envElement, "managementTimeout", "managementTimeout");
		setPropertyValue(envDefinitionBuilder, envElement, "queryTimeout", "queryTimeout");
		setPropertyValue(envDefinitionBuilder, envElement, "viewTimeout", "viewTimeout");
		setPropertyValue(envDefinitionBuilder, envElement, "kvTimeout", "kvTimeout");
		setPropertyValue(envDefinitionBuilder, envElement, "connectTimeout", "connectTimeout");
		setPropertyValue(envDefinitionBuilder, envElement, "disconnectTimeout", "disconnectTimeout");
		setPropertyValue(envDefinitionBuilder, envElement, "dnsSrvEnabled", "dnsSrvEnabled");

		setPropertyValue(envDefinitionBuilder, envElement, "dcpEnabled", "dcpEnabled");
		setPropertyValue(envDefinitionBuilder, envElement, "sslEnabled", "sslEnabled");
		setPropertyValue(envDefinitionBuilder, envElement, "sslKeystoreFile", "sslKeystoreFile");
		setPropertyValue(envDefinitionBuilder, envElement, "sslKeystorePassword", "sslKeystorePassword");
		setPropertyValue(envDefinitionBuilder, envElement, "bootstrapHttpEnabled", "bootstrapHttpEnabled");
		setPropertyValue(envDefinitionBuilder, envElement, "bootstrapCarrierEnabled", "bootstrapCarrierEnabled");
		setPropertyValue(envDefinitionBuilder, envElement, "bootstrapHttpDirectPort", "bootstrapHttpDirectPort");
		setPropertyValue(envDefinitionBuilder, envElement, "bootstrapHttpSslPort", "bootstrapHttpSslPort");
		setPropertyValue(envDefinitionBuilder, envElement, "bootstrapCarrierDirectPort", "bootstrapCarrierDirectPort");
		setPropertyValue(envDefinitionBuilder, envElement, "bootstrapCarrierSslPort", "bootstrapCarrierSslPort");
		setPropertyValue(envDefinitionBuilder, envElement, "ioPoolSize", "ioPoolSize");
		setPropertyValue(envDefinitionBuilder, envElement, "computationPoolSize", "computationPoolSize");
		setPropertyValue(envDefinitionBuilder, envElement, "responseBufferSize", "responseBufferSize");
		setPropertyValue(envDefinitionBuilder, envElement, "requestBufferSize", "requestBufferSize");
		setPropertyValue(envDefinitionBuilder, envElement, "kvEndpoints", "kvEndpoints");
		setPropertyValue(envDefinitionBuilder, envElement, "viewEndpoints", "viewEndpoints");
		setPropertyValue(envDefinitionBuilder, envElement, "queryEndpoints", "queryEndpoints");
		setPropertyValue(envDefinitionBuilder, envElement, "maxRequestLifetime", "maxRequestLifetime");
		setPropertyValue(envDefinitionBuilder, envElement, "keepAliveInterval", "keepAliveInterval");
		setPropertyValue(envDefinitionBuilder, envElement, "autoreleaseAfter", "autoreleaseAfter");
		setPropertyValue(envDefinitionBuilder, envElement, "bufferPoolingEnabled", "bufferPoolingEnabled");
		setPropertyValue(envDefinitionBuilder, envElement, "tcpNodelayEnabled", "tcpNodelayEnabled");
		setPropertyValue(envDefinitionBuilder, envElement, "mutationTokensEnabled", "mutationTokensEnabled");

		//retry strategy is particular, in the xsd this is an enum (FailFast, BestEffort)
		setPropertyValue(envDefinitionBuilder, envElement, "retryStrategy", "retryStrategy");
	}
}
