/*
 * Copyright 2012-2015 the original author or authors
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

import java.util.ArrayList;
import java.util.List;

import com.couchbase.client.java.Cluster;
import org.w3c.dom.Element;
import org.w3c.dom.NodeList;

import org.springframework.beans.factory.support.AbstractBeanDefinition;
import org.springframework.beans.factory.support.BeanDefinitionBuilder;
import org.springframework.beans.factory.xml.AbstractSingleBeanDefinitionParser;
import org.springframework.beans.factory.xml.ParserContext;
import org.springframework.util.StringUtils;
import org.springframework.util.xml.DomUtils;

public class CouchbaseClusterParser extends AbstractSingleBeanDefinitionParser {

	/**
	 * The &lt;node&gt; elements in a cluster definition define the bootstrap hosts to use
	 */
	public static final String CLUSTER_NODE_TAG = "node";

	/**
	 * The unique &lt;env&gt; element in a cluster definition define the environment customizations.
	 *
	 * @see CouchbaseEnvironmentParser for the possible fields.
	 * @see #CLUSTER_ENVIRONMENT_REF as an alternative (giving a reference to an env instead of inline description)
	 */
	public static final String CLUSTER_ENVIRONMENT_TAG = "env";


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
		return Cluster.class;
	}

	/**
	 * Parse the bean definition and build up the bean.
	 *
	 * @param element the XML element which contains the attributes.
	 * @param bean the builder which builds the bean.
	 */
	@Override
	protected void doParse(final Element element, final BeanDefinitionBuilder bean) {
		parseEnvironment(bean, element);

		NodeList nodes = element.getElementsByTagName(CLUSTER_NODE_TAG);
		if (nodes != null && nodes.getLength() > 0) {
			List<String> bootstrapUrls = new ArrayList<String>(nodes.getLength());
			for (int i = 0; i < bootstrapUrls.size(); i++) {
				bootstrapUrls.add(nodes.item(i).getNodeValue());
			}
			bean.addConstructorArgValue(bootstrapUrls);
		}
	}

	public static boolean parseEnvironment(BeanDefinitionBuilder clusterBuilder, Element clusterElement) {
		//first try a reference
		String envRef = clusterElement.getAttribute(CLUSTER_ENVIRONMENT_REF);
		if (StringUtils.hasText(envRef)) {
			clusterBuilder.addConstructorArgReference(envRef);
			return true;
		}

		//secondly try to see if an env has been described inline
		Element envElement = DomUtils.getChildElementByTagName(clusterElement, CLUSTER_ENVIRONMENT_TAG);
		if (envElement == null || !envElement.hasAttributes()) {
			return false;
		}

		BeanDefinitionBuilder envDefinitionBuilder = BeanDefinitionBuilder
				.genericBeanDefinition(CouchbaseEnvironmentFactoryBean.class);
		new CouchbaseEnvironmentParser().doParse(envElement, envDefinitionBuilder);

		clusterBuilder.addConstructorArgValue(envDefinitionBuilder.getBeanDefinition());
		return true;
	}
}
