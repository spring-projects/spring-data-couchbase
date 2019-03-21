/*
 * Copyright 2012-2019 the original author or authors
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

package org.springframework.data.couchbase.config;

import java.util.ArrayList;
import java.util.List;

import com.couchbase.client.java.Cluster;
import com.couchbase.client.java.CouchbaseCluster;
import com.couchbase.client.java.env.CouchbaseEnvironment;
import org.w3c.dom.Element;

import org.springframework.beans.factory.support.AbstractBeanDefinition;
import org.springframework.beans.factory.support.BeanDefinitionBuilder;
import org.springframework.beans.factory.xml.AbstractSingleBeanDefinitionParser;
import org.springframework.beans.factory.xml.ParserContext;
import org.springframework.util.StringUtils;
import org.springframework.util.xml.DomUtils;

/**
 * The XML parser for a {@link Cluster} definition.
 *
 * Such a definition can be tuned by either referencing a {@link CouchbaseEnvironment} via
 * the {@value #CLUSTER_ENVIRONMENT_REF} attribute or define a custom environment inline via
 * the &lt;{@value #CLUSTER_ENVIRONMENT_TAG}&gt; tag (not recommended, environments should be
 * shared as possible). If no environment reference or inline description is provided, the
 * default environment reference {@value BeanNames#COUCHBASE_ENV} is used.
 *
 * To bootstrap the connection, one can provide IPs or hostnames of nodes to connect to
 * via 1 or more &lt;{@value #CLUSTER_NODE_TAG}&gt; tags.
 *
 * @author Simon Baslé
 */
public class CouchbaseClusterParser extends AbstractSingleBeanDefinitionParser {

	/**
	 * The &lt;node&gt; elements in a cluster definition define the bootstrap hosts to use
	 */
	public static final String CLUSTER_NODE_TAG = "node";

	/**
	 * The unique &lt;env&gt; element in a cluster definition define the environment customizations.
	 *
	 * @see CouchbaseEnvironmentParser CouchbaseEnvironmentParser for the possible fields.
	 * @see #CLUSTER_ENVIRONMENT_REF CLUSTER_ENVIRONMENT_REF as an alternative (giving a reference to
	 * an env instead of inline description, lower precedence)
	 */
	public static final String CLUSTER_ENVIRONMENT_TAG = "env";

	/**
	 * The &lt;env-ref&gt; attribute allows to use a reference to an {@link CouchbaseEnvironment} to
	 * tune the connection.
	 *
	 * @see #CLUSTER_ENVIRONMENT_TAG CLUSTER_ENVIRONMENT_TAG for an inline alternative
	 * (which takes priority over this reference)
	 */
	public static final String CLUSTER_ENVIRONMENT_REF = "env-ref";

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
		return StringUtils.hasText(id) ? id : BeanNames.COUCHBASE_CLUSTER;
	}

	/**
	 * Defines the bean class that will be constructed.
	 *
	 * @param element the XML element which contains the attributes.
	 * @return the class type to instantiate.
	 */
	@Override
	protected Class getBeanClass(final Element element) {
		return CouchbaseCluster.class;
	}

	/**
	 * Parse the bean definition and build up the bean.
	 *
	 * @param element the XML element which contains the attributes.
	 * @param bean the builder which builds the bean.
	 */
	@Override
	protected void doParse(final Element element, final BeanDefinitionBuilder bean) {
		bean.setFactoryMethod("create");
		bean.setDestroyMethodName("disconnect");

		parseEnvironment(bean, element);

		List<Element> nodes = DomUtils.getChildElementsByTagName(element, CLUSTER_NODE_TAG);
		if (nodes != null && nodes.size() > 0) {
			List<String> bootstrapUrls = new ArrayList<String>(nodes.size());
			for (int i = 0; i < nodes.size(); i++) {
				bootstrapUrls.add(nodes.get(i).getTextContent());
			}
			bean.addConstructorArgValue(bootstrapUrls);
		}
	}

	/**
	 * @return true if a custom environment was parsed and injected (either reference or inline), false if
	 * the default environment reference was used.
	 */
	protected boolean parseEnvironment(BeanDefinitionBuilder clusterBuilder, Element clusterElement) {
		//any inline environment description would take precedence over a reference
		Element envElement = DomUtils.getChildElementByTagName(clusterElement, CLUSTER_ENVIRONMENT_TAG);
		if (envElement != null && envElement.hasAttributes()) {
			injectEnvElement(clusterBuilder, envElement);
			return true;
		}

		//secondly try to see if an env has been referenced
		String envRef = clusterElement.getAttribute(CLUSTER_ENVIRONMENT_REF);
		if (StringUtils.hasText(envRef)) {
			injectEnvReference(clusterBuilder, envRef);
			return true;
		}

		//if no custom value provided, consider it a reference to the default bean for Couchbase Environment
		injectEnvReference(clusterBuilder, BeanNames.COUCHBASE_ENV);
		return false;
	}

	protected void injectEnvElement(BeanDefinitionBuilder clusterBuilder, Element envElement) {
		BeanDefinitionBuilder envDefinitionBuilder = BeanDefinitionBuilder
				.genericBeanDefinition(CouchbaseEnvironmentFactoryBean.class);
		new CouchbaseEnvironmentParser().doParse(envElement, envDefinitionBuilder);

		clusterBuilder.addConstructorArgValue(envDefinitionBuilder.getBeanDefinition());
	}

	protected void injectEnvReference(BeanDefinitionBuilder clusterBuilder, String envRef) {
		clusterBuilder.addConstructorArgReference(envRef);
	}
}
