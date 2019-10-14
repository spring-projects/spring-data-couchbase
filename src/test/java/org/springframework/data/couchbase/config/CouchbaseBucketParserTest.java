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

import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import org.springframework.beans.factory.config.BeanDefinition;
import org.springframework.beans.factory.config.ConstructorArgumentValues;
import org.springframework.beans.factory.config.RuntimeBeanReference;
import org.springframework.beans.factory.support.BeanDefinitionReader;
import org.springframework.beans.factory.support.DefaultListableBeanFactory;
import org.springframework.beans.factory.xml.XmlBeanDefinitionReader;
import org.springframework.core.io.ClassPathResource;

import static org.assertj.core.api.Assertions.assertThat;

public class CouchbaseBucketParserTest {

	private static DefaultListableBeanFactory factory;

	@BeforeClass
	public static void setUp() {
		factory = new DefaultListableBeanFactory();
		BeanDefinitionReader reader = new XmlBeanDefinitionReader(factory);
		int n = reader.loadBeanDefinitions(new ClassPathResource("configurations/couchbaseBucket-bean.xml"));
		System.out.println(n);
	}

	@AfterClass
	public static void tearDown() {
	}

	@Test
	public void testDefaultBucketNoCluster() {
		BeanDefinition def = factory.getBeanDefinition("bucketDefaultNoCluster");

		assertThat(def).isNotNull();
		assertThat(def.getConstructorArgumentValues().getArgumentCount()).isEqualTo(1);
		assertThat(def.getPropertyValues().size()).isEqualTo(0);

		ConstructorArgumentValues.ValueHolder holder = def.getConstructorArgumentValues()
				.getArgumentValue(0, Object.class);
		assertThat(holder.getValue()).isInstanceOf(RuntimeBeanReference.class);

		RuntimeBeanReference clusterRef = (RuntimeBeanReference) holder.getValue();

		assertThat(clusterRef.getBeanName()).isEqualTo(BeanNames.COUCHBASE_CLUSTER);
	}

	@Test
	public void testDefaultBucket() throws Exception {
		BeanDefinition def = factory.getBeanDefinition("bucketDefault");

		assertThat(def).isNotNull();
		assertThat(def.getConstructorArgumentValues().getArgumentCount()).isEqualTo(1);
		assertThat(def.getPropertyValues().size()).isEqualTo(0);

		ConstructorArgumentValues.ValueHolder holder = def.getConstructorArgumentValues()
				.getArgumentValue(0, Object.class);
		assertThat(holder.getValue()).isInstanceOf(RuntimeBeanReference.class);

		RuntimeBeanReference clusterRef = (RuntimeBeanReference) holder.getValue();

		assertThat(clusterRef.getBeanName()).isEqualTo("clusterDefault");
	}

	@Test
	public void testBucketWithName() throws Exception {
		BeanDefinition def = factory.getBeanDefinition("bucketWithName");

		assertThat(def).isNotNull();
		assertThat(def.getConstructorArgumentValues().getArgumentCount()).isEqualTo(2);
		assertThat(def.getPropertyValues().size()).isEqualTo(0);

		ConstructorArgumentValues.ValueHolder holder = def.getConstructorArgumentValues()
				.getArgumentValue(0, Object.class);
		assertThat(holder.getValue()).isInstanceOf(RuntimeBeanReference.class);

		RuntimeBeanReference clusterRef = (RuntimeBeanReference) holder.getValue();
		assertThat(clusterRef.getBeanName()).isEqualTo("clusterDefault");

		ConstructorArgumentValues.ValueHolder nameHolder = def.getConstructorArgumentValues()
				.getArgumentValue(1, Object.class);
		assertThat(nameHolder.getValue()).isInstanceOf(String.class);
		assertThat(nameHolder.getValue()).hasToString("toto");
	}

	@Test
	public void testBucketWithNameAndPassword() throws Exception {
		BeanDefinition def = factory.getBeanDefinition("bucketWithNameAndPassword");

		assertThat(def).isNotNull();
		assertThat(def.getConstructorArgumentValues().getArgumentCount()).isEqualTo(4);
		assertThat(def.getPropertyValues().size()).isEqualTo(0);

		ConstructorArgumentValues.ValueHolder holder = def.getConstructorArgumentValues()
				.getArgumentValue(0, Object.class);
		assertThat(holder.getValue()).isInstanceOf(RuntimeBeanReference.class);

		RuntimeBeanReference clusterRef = (RuntimeBeanReference) holder.getValue();
		assertThat(clusterRef.getBeanName()).isEqualTo("clusterDefault");

		ConstructorArgumentValues.ValueHolder nameHolder = def.getConstructorArgumentValues()
				.getArgumentValue(1, Object.class);
		assertThat(nameHolder.getValue()).isInstanceOf(String.class);
		assertThat(nameHolder.getValue()).hasToString("test");


		ConstructorArgumentValues.ValueHolder usernameHolder = def.getConstructorArgumentValues()
				.getArgumentValue(2, Object.class);
		assertThat(usernameHolder.getValue()).isInstanceOf(String.class);
		assertThat(usernameHolder.getValue()).hasToString("testuser");


		ConstructorArgumentValues.ValueHolder passwordHolder = def.getConstructorArgumentValues()
				.getArgumentValue(3, Object.class);
		assertThat(passwordHolder.getValue()).isInstanceOf(String.class);
		assertThat(passwordHolder.getValue()).hasToString("123");
	}
}
