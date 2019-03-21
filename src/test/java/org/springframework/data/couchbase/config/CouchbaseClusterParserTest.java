/*
 * Copyright 2012-2015 the original author or authors
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

import static org.hamcrest.CoreMatchers.*;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.core.Is.is;

import java.util.List;

import com.couchbase.client.java.env.CouchbaseEnvironment;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import org.springframework.beans.factory.config.BeanDefinition;
import org.springframework.beans.factory.config.ConstructorArgumentValues;
import org.springframework.beans.factory.config.RuntimeBeanReference;
import org.springframework.beans.factory.support.BeanDefinitionReader;
import org.springframework.beans.factory.support.DefaultListableBeanFactory;
import org.springframework.beans.factory.support.GenericBeanDefinition;
import org.springframework.beans.factory.xml.XmlBeanDefinitionReader;
import org.springframework.core.io.ClassPathResource;

public class CouchbaseClusterParserTest {


	private static DefaultListableBeanFactory factory;

	@BeforeClass
	public static void setUp() {
		factory = new DefaultListableBeanFactory();
		BeanDefinitionReader reader = new XmlBeanDefinitionReader(factory);
		int n = reader.loadBeanDefinitions(new ClassPathResource("configurations/couchbaseCluster-bean.xml"));
		System.out.println(n);
	}

	@AfterClass
	public static void tearDown() {
	}

	@Test
	public void testClusterWithoutSpecificEnv() {
		BeanDefinition def = factory.getBeanDefinition("clusterDefault");

		assertThat(def, is(notNullValue()));
		assertThat(def.getConstructorArgumentValues().getArgumentCount(), is(equalTo(1)));
		assertThat(def.getPropertyValues().size(), is(equalTo(0)));
		assertThat(def.getFactoryMethodName(), is(equalTo("create")));

		ConstructorArgumentValues.ValueHolder holder = def.getConstructorArgumentValues()
				.getArgumentValue(0, CouchbaseEnvironment.class);

		assertThat(holder.getValue(), instanceOf(RuntimeBeanReference.class));
		RuntimeBeanReference envRef = (RuntimeBeanReference) holder.getValue();

		assertThat(envRef.getBeanName(), is(equalTo("couchbaseEnv")));
	}

	@Test
	public void testClusterWithNodes() {
		BeanDefinition def = factory.getBeanDefinition("clusterWithNodes");

		assertThat(def, is(notNullValue()));
		assertThat(def.getConstructorArgumentValues().getArgumentCount(), is(equalTo(2)));
		assertThat(def.getPropertyValues().size(), is(equalTo(0)));
		assertThat(def.getFactoryMethodName(), is(equalTo("create")));

		ConstructorArgumentValues.ValueHolder holder = def.getConstructorArgumentValues()
				.getArgumentValue(1, List.class);
		assertThat(holder.getValue(), is(instanceOf(List.class)));
		List nodes = (List<String>) holder.getValue();

		assertThat(nodes.size(), is(equalTo(2)));
		assertThat((String) nodes.get(0), is(equalTo("192.1.2.3")));
		assertThat((String) nodes.get(1), is(equalTo("192.4.5.6")));
	}

	@Test
	public void testClusterWithEnvInline() {
		BeanDefinition def = factory.getBeanDefinition("clusterWithEnvInline");

		assertThat(def, is(notNullValue()));
		assertThat(def.getConstructorArgumentValues().getArgumentCount(), is(equalTo(1)));
		assertThat(def.getPropertyValues().size(), is(equalTo(0)));

		ConstructorArgumentValues.ValueHolder holder = def.getConstructorArgumentValues()
				.getArgumentValue(0, CouchbaseEnvironment.class);
		GenericBeanDefinition envDef = (GenericBeanDefinition) holder.getValue();

		assertThat(envDef.getBeanClassName(), is(equalTo(CouchbaseEnvironmentFactoryBean.class.getName())));
		assertThat("unexpected attribute", envDef.getPropertyValues().contains("managementTimeout"));
	}

	@Test
	public void testClusterWithEnvRef() {
		BeanDefinition def = factory.getBeanDefinition("clusterWithEnvRef");

		assertThat(def, is(notNullValue()));
		assertThat(def.getConstructorArgumentValues().getArgumentCount(), is(equalTo(1)));
		assertThat(def.getPropertyValues().size(), is(equalTo(0)));

		ConstructorArgumentValues.ValueHolder holder = def.getConstructorArgumentValues()
				.getArgumentValue(0, CouchbaseEnvironment.class);

		assertThat(holder.getValue(), instanceOf(RuntimeBeanReference.class));
		RuntimeBeanReference envRef = (RuntimeBeanReference) holder.getValue();

		assertThat(envRef.getBeanName(), is(equalTo("someEnv")));
	}
	@Test
	public void testClusterConfigurationPrecedence() {
		BeanDefinition def = factory.getBeanDefinition("clusterWithAll");

		assertThat(def, is(notNullValue()));
		assertThat(def.getConstructorArgumentValues().getArgumentCount(), is(equalTo(2)));
		assertThat(def.getPropertyValues().size(), is(equalTo(0)));
		assertThat(def.getFactoryMethodName(), is(equalTo("create")));

		assertThat(def.getConstructorArgumentValues().getIndexedArgumentValues().get(0).getValue(),
				instanceOf(GenericBeanDefinition.class));
		assertThat(def.getConstructorArgumentValues().getIndexedArgumentValues().get(1).getValue(),
				instanceOf(List.class));

		ConstructorArgumentValues.ValueHolder holderEnv = def.getConstructorArgumentValues()
				.getArgumentValue(0, CouchbaseEnvironment.class);
		GenericBeanDefinition envDef = (GenericBeanDefinition) holderEnv.getValue();

		assertThat(envDef.getBeanClassName(), is(equalTo(CouchbaseEnvironmentFactoryBean.class.getName())));
		assertThat("unexpected attribute", envDef.getPropertyValues().contains("autoreleaseAfter"));

		ConstructorArgumentValues.ValueHolder holderNodes = def.getConstructorArgumentValues()
				.getArgumentValue(1, List.class);
		List nodes = (List<String>) holderNodes.getValue();

		assertThat(nodes.size(), is(equalTo(2)));
		assertThat((String) nodes.get(0), is(equalTo("2.2.2.2")));
		assertThat((String) nodes.get(1), is(equalTo("4.4.4.4")));
	}
}