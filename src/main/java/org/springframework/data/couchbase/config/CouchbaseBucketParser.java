/*
 * Copyright 2012-2020 the original author or authors
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

import com.couchbase.client.java.Bucket;
import com.couchbase.client.java.Cluster;
import org.w3c.dom.Element;

import org.springframework.beans.factory.support.AbstractBeanDefinition;
import org.springframework.beans.factory.support.BeanDefinitionBuilder;
import org.springframework.beans.factory.xml.AbstractSingleBeanDefinitionParser;
import org.springframework.beans.factory.xml.ParserContext;
import org.springframework.util.StringUtils;

/**
 * The parser for XML definition of a {@link Bucket}, to be constructed from a {@link Cluster} reference.
 * If no reference is given, the default reference <code>{@value BeanNames#COUCHBASE_CLUSTER}</code> is used.
 *
 * See attributes {@link #CLUSTER_REF_ATTR}, {@link #BUCKETNAME_ATTR}, {@link #USERNAME_ATTR} and {@link #BUCKETPASSWORD_ATTR}.
 *
 * @author Simon Baslé
 */
public class CouchbaseBucketParser extends AbstractSingleBeanDefinitionParser {

	/**
	 * The <code>cluster-ref</code> attribute in a bucket definition defines the cluster to build from.
	 */
	public static final String CLUSTER_REF_ATTR = "cluster-ref";

	/**
	 * The <code>bucketName</code> attribute in a bucket definition defines the name of the bucket to open.
	 */
	public static final String BUCKETNAME_ATTR = "bucketName";

	/*
	 * The <code>username</code> attribute in a bucket definition defines the user of the bucket to open.
	 */
	public static final String USERNAME_ATTR = "username";

	/**
	 * The <code>bucketPassword</code> attribute in a bucket definition defines the password of the bucket/user of the bucket to open.
	 */
	public static final String BUCKETPASSWORD_ATTR = "bucketPassword";

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
		return StringUtils.hasText(id) ? id : BeanNames.COUCHBASE_BUCKET;
	}

	/**
	 * Defines the bean class that will be constructed.
	 *
	 * @param element the XML element which contains the attributes.
	 * @return the class type to instantiate.
	 */
	@Override
	protected Class getBeanClass(final Element element) {
		return CouchbaseBucketFactoryBean.class;
	}

	/**
	 * Parse the bean definition and build up the bean.
	 *
	 * @param element the XML element which contains the attributes.
	 * @param builder the builder which builds the bean.
	 */
	@Override
	protected void doParse(final Element element, final BeanDefinitionBuilder builder) {
		String clusterRef = element.getAttribute(CLUSTER_REF_ATTR);
		if (!StringUtils.hasText(clusterRef)) {
			clusterRef = BeanNames.COUCHBASE_CLUSTER;
		}
		builder.addConstructorArgReference(clusterRef);

		String bucketName = element.getAttribute(BUCKETNAME_ATTR);
		if (StringUtils.hasText(bucketName)) {
			builder.addConstructorArgValue(bucketName);
		}

		String username = element.getAttribute(USERNAME_ATTR);
		if (StringUtils.hasText(username)) {
			builder.addConstructorArgValue(username);
		}

		String password = element.getAttribute(BUCKETPASSWORD_ATTR);
		if (StringUtils.hasText(password)) {
			builder.addConstructorArgValue(password);
		}
	}
}
