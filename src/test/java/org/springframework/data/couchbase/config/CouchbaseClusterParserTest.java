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

import java.util.List;

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

import com.couchbase.client.java.env.CouchbaseEnvironment;

import static org.assertj.core.api.Assertions.assertThat;

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

		assertThat(def).isNotNull();
		assertThat(def.getConstructorArgumentValues().getArgumentCount()).isEqualTo(1);
		assertThat(def.getPropertyValues().size()).isEqualTo(0);
		assertThat(def.getFactoryMethodName()).isEqualTo("create");

		ConstructorArgumentValues.ValueHolder holder = def.getConstructorArgumentValues()
				.getArgumentValue(0, CouchbaseEnvironment.class);

		assertThat(holder.getValue()).isInstanceOf(RuntimeBeanReference.class);
		RuntimeBeanReference envRef = (RuntimeBeanReference) holder.getValue();

		assertThat(envRef.getBeanName()).isEqualTo("couchbaseEnv");
	}

	@Test
	public void testClusterWithNodes() {
		BeanDefinition def = factory.getBeanDefinition("clusterWithNodes");

		assertThat(def).isNotNull();
		assertThat(def.getConstructorArgumentValues().getArgumentCount()).isEqualTo(2);
		assertThat(def.getPropertyValues().size()).isEqualTo(0);
		assertThat(def.getFactoryMethodName()).isEqualTo("create");

		ConstructorArgumentValues.ValueHolder holder = def.getConstructorArgumentValues()
				.getArgumentValue(1, List.class);
		assertThat(holder.getValue()).isInstanceOf(List.class);
		List nodes = (List<String>) holder.getValue();

		assertThat(nodes.size()).isEqualTo(2);
		assertThat((String) nodes.get(0)).isEqualTo("192.1.2.3");
		assertThat((String) nodes.get(1)).isEqualTo("192.4.5.6");
	}

	@Test
	public void testClusterWithEnvInline() {
		BeanDefinition def = factory.getBeanDefinition("clusterWithEnvInline");

		assertThat(def).isNotNull();
		assertThat(def.getConstructorArgumentValues().getArgumentCount()).isEqualTo(1);
		assertThat(def.getPropertyValues().size()).isEqualTo(0);

		ConstructorArgumentValues.ValueHolder holder = def.getConstructorArgumentValues()
				.getArgumentValue(0, CouchbaseEnvironment.class);
		GenericBeanDefinition envDef = (GenericBeanDefinition) holder.getValue();

		assertThat(envDef.getBeanClassName())
				.isEqualTo(CouchbaseEnvironmentFactoryBean.class.getName());
		assertThat(envDef.getPropertyValues().contains("managementTimeout"))
				.as("unexpected attribute").isTrue();
	}

	@Test
	public void testClusterWithEnvRef() {
		BeanDefinition def = factory.getBeanDefinition("clusterWithEnvRef");

		assertThat(def).isNotNull();
		assertThat(def.getConstructorArgumentValues().getArgumentCount()).isEqualTo(1);
		assertThat(def.getPropertyValues().size()).isEqualTo(0);

		ConstructorArgumentValues.ValueHolder holder = def.getConstructorArgumentValues()
				.getArgumentValue(0, CouchbaseEnvironment.class);

		assertThat(holder.getValue()).isInstanceOf(RuntimeBeanReference.class);
		RuntimeBeanReference envRef = (RuntimeBeanReference) holder.getValue();

		assertThat(envRef.getBeanName()).isEqualTo("someEnv");
	}
	@Test
	public void testClusterConfigurationPrecedence() {
		BeanDefinition def = factory.getBeanDefinition("clusterWithAll");

		assertThat(def).isNotNull();
		assertThat(def.getConstructorArgumentValues().getArgumentCount()).isEqualTo(2);
		assertThat(def.getPropertyValues().size()).isEqualTo(0);
		assertThat(def.getFactoryMethodName()).isEqualTo("create");

		assertThat(def.getConstructorArgumentValues().getIndexedArgumentValues().get(0)
				.getValue()).isInstanceOf(GenericBeanDefinition.class);
		assertThat(def.getConstructorArgumentValues().getIndexedArgumentValues().get(1)
				.getValue()).isInstanceOf(List.class);

		ConstructorArgumentValues.ValueHolder holderEnv = def.getConstructorArgumentValues()
				.getArgumentValue(0, CouchbaseEnvironment.class);
		GenericBeanDefinition envDef = (GenericBeanDefinition) holderEnv.getValue();

		assertThat(envDef.getBeanClassName())
				.isEqualTo(CouchbaseEnvironmentFactoryBean.class.getName());
		assertThat(envDef.getPropertyValues().contains("autoreleaseAfter"))
				.as("unexpected attribute").isTrue();

		ConstructorArgumentValues.ValueHolder holderNodes = def.getConstructorArgumentValues()
				.getArgumentValue(1, List.class);
		List nodes = (List<String>) holderNodes.getValue();

		assertThat(nodes.size()).isEqualTo(2);
		assertThat((String) nodes.get(0)).isEqualTo("2.2.2.2");
		assertThat((String) nodes.get(1)).isEqualTo("4.4.4.4");
	}
}
