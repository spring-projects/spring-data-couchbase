/*
 * Copyright 2012-2019 the original author or authors
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

		assertThat(def, is(notNullValue()));
		assertThat(def.getConstructorArgumentValues().getArgumentCount(), is(equalTo(1)));
		assertThat(def.getPropertyValues().size(), is(equalTo(0)));

		ConstructorArgumentValues.ValueHolder holder = def.getConstructorArgumentValues()
				.getArgumentValue(0, Object.class);
		assertThat(holder.getValue(), is(instanceOf(RuntimeBeanReference.class)));

		RuntimeBeanReference clusterRef = (RuntimeBeanReference) holder.getValue();

		assertThat(clusterRef.getBeanName(), is(equalTo(BeanNames.COUCHBASE_CLUSTER)));
	}

	@Test
	public void testDefaultBucket() throws Exception {
		BeanDefinition def = factory.getBeanDefinition("bucketDefault");

		assertThat(def, is(notNullValue()));
		assertThat(def.getConstructorArgumentValues().getArgumentCount(), is(equalTo(1)));
		assertThat(def.getPropertyValues().size(), is(equalTo(0)));

		ConstructorArgumentValues.ValueHolder holder = def.getConstructorArgumentValues()
				.getArgumentValue(0, Object.class);
		assertThat(holder.getValue(), is(instanceOf(RuntimeBeanReference.class)));

		RuntimeBeanReference clusterRef = (RuntimeBeanReference) holder.getValue();

		assertThat(clusterRef.getBeanName(), is(equalTo("clusterDefault")));
	}

	@Test
	public void testBucketWithName() throws Exception {
		BeanDefinition def = factory.getBeanDefinition("bucketWithName");

		assertThat(def, is(notNullValue()));
		assertThat(def.getConstructorArgumentValues().getArgumentCount(), is(equalTo(2)));
		assertThat(def.getPropertyValues().size(), is(equalTo(0)));

		ConstructorArgumentValues.ValueHolder holder = def.getConstructorArgumentValues()
				.getArgumentValue(0, Object.class);
		assertThat(holder.getValue(), is(instanceOf(RuntimeBeanReference.class)));

		RuntimeBeanReference clusterRef = (RuntimeBeanReference) holder.getValue();
		assertThat(clusterRef.getBeanName(), is(equalTo("clusterDefault")));

		ConstructorArgumentValues.ValueHolder nameHolder = def.getConstructorArgumentValues()
				.getArgumentValue(1, Object.class);
		assertThat(nameHolder.getValue(), is(instanceOf(String.class)));
		assertThat(nameHolder.getValue().toString(), is((equalTo("toto"))));
	}

	@Test
	public void testBucketWithNameAndPassword() throws Exception {
		BeanDefinition def = factory.getBeanDefinition("bucketWithNameAndPassword");

		assertThat(def, is(notNullValue()));
		assertThat(def.getConstructorArgumentValues().getArgumentCount(), is(equalTo(3)));
		assertThat(def.getPropertyValues().size(), is(equalTo(0)));

		ConstructorArgumentValues.ValueHolder holder = def.getConstructorArgumentValues()
				.getArgumentValue(0, Object.class);
		assertThat(holder.getValue(), is(instanceOf(RuntimeBeanReference.class)));

		RuntimeBeanReference clusterRef = (RuntimeBeanReference) holder.getValue();
		assertThat(clusterRef.getBeanName(), is(equalTo("clusterDefault")));

		ConstructorArgumentValues.ValueHolder nameHolder = def.getConstructorArgumentValues()
				.getArgumentValue(1, Object.class);
		assertThat(nameHolder.getValue(), is(instanceOf(String.class)));
		assertThat(nameHolder.getValue().toString(), is((equalTo("test"))));

		ConstructorArgumentValues.ValueHolder passwordHolder = def.getConstructorArgumentValues()
				.getArgumentValue(2, Object.class);
		assertThat(passwordHolder.getValue(), is(instanceOf(String.class)));
		assertThat(passwordHolder.getValue().toString(), is((equalTo("123"))));
	}
}
