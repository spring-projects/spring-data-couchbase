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
package org.springframework.data.couchbase.util;

import java.io.IOException;
import java.lang.annotation.Annotation;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;

import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.extension.ExtendWith;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.data.couchbase.CouchbaseClientFactory;
import org.springframework.data.couchbase.SimpleCouchbaseClientFactory;

import com.couchbase.client.core.env.Authenticator;
import com.couchbase.client.core.env.PasswordAuthenticator;
import com.couchbase.client.core.env.SeedNode;
import com.couchbase.client.core.error.IndexFailureException;
import com.couchbase.client.java.manager.query.CreatePrimaryQueryIndexOptions;
import com.couchbase.client.java.manager.query.CreateQueryIndexOptions;

/**
 * Parent class which drives all dynamic integration tests based on the configured cluster setup.
 *
 * @since 2.0.0
 */
@ExtendWith(ClusterInvocationProvider.class)
public abstract class ClusterAwareIntegrationTests {

	private static TestClusterConfig testClusterConfig;
	private static final Logger LOGGER = LoggerFactory.getLogger(ClusterAwareIntegrationTests.class);

	@BeforeAll
	static void setup(TestClusterConfig config) {
		testClusterConfig = config;
		try (CouchbaseClientFactory couchbaseClientFactory = new SimpleCouchbaseClientFactory(connectionString(),
				authenticator(), bucketName())) {
			couchbaseClientFactory.getCluster().queryIndexes().createPrimaryIndex(bucketName(),
					CreatePrimaryQueryIndexOptions.createPrimaryQueryIndexOptions().ignoreIfExists(true));
			// this is for the N1qlJoin test
			List<String> fieldList = new ArrayList<>();
			fieldList.add("parentId");
			couchbaseClientFactory.getCluster().queryIndexes().createIndex(bucketName(), "parent_idx", fieldList,
					CreateQueryIndexOptions.createQueryIndexOptions().ignoreIfExists(true));
			// .with("_class", "org.springframework.data.couchbase.domain.Address"));
		} catch (IndexFailureException ife) {
			LOGGER.warn("IndexFailureException occurred - ignoring: ", ife);
		} catch (IOException ioe) {
			throw new RuntimeException(ioe);
		}
	}

	/**
	 * Returns the current config for the integration test cluster.
	 */
	public static TestClusterConfig config() {
		return testClusterConfig;
	}

	protected static Authenticator authenticator() {
		return PasswordAuthenticator.create(config().adminUsername(), config().adminPassword());
	}

	public static String username() {
		return config().adminUsername();
	}

	public static String password() {
		return config().adminPassword();
	}

	public static String bucketName() {
		return config().bucketname();
	}

	/**
	 * Creates the right connection string out of the seed nodes in the config.
	 *
	 * @return the connection string to connect.
	 */
	public static String connectionString() {
		StringBuffer sb = new StringBuffer();
		for (SeedNode s : seedNodes()) {
			if (s.kvPort().isPresent()) {
				if (sb.length() > 0)
					sb.append(",");
				sb.append(s.address() + ":" + s.kvPort().get() + "=" + Services.KV);
			}
			if (s.clusterManagerPort().isPresent()) {
				if (sb.length() > 0)
					sb.append(",");
				sb.append(s.address() + ":" + s.clusterManagerPort().get() + "=" + Services.MANAGER);
			}
			if (sb.length() == 0) {
				sb.append(s.address());
			}
		}
		return sb.toString();
	}

	protected static Set<SeedNode> seedNodes() {
		return config().nodes().stream().map(cfg -> SeedNode.create(cfg.hostname(),
				Optional.ofNullable(cfg.ports().get(Services.KV)), Optional.ofNullable(cfg.ports().get(Services.MANAGER))))
				.collect(Collectors.toSet());
	}

	@BeforeAll()
	public static void beforeAll() {}

	@AfterAll
	public static void afterAll() {}

	@BeforeEach
	public void beforeEach() {}

	@AfterEach
	public void afterEach() {}

	/**
	 * This should probably be the first call in the @BeforeAll method of a test class. This will call super @BeforeAll
	 * methods when called as callSuperBeforeAll(new Object() {}); this trickery is necessary because super.beforeAll()
	 * cannot be used because it is a static method. it is possible and likely that the beforeAll() method of should still
	 * be called even when a test class defines its own beforeAll() method which would hide the beforeAll() of the super
	 * class. This trickery is not necessary for before/AfterEach, as those are not static methods
	 *
	 * @Author Michael Reiche
	 * @param createdHere - an object from a class defined in the calling class
	 */
	public static void callSuperBeforeAll(Object createdHere) {
		callSuper(createdHere, BeforeAll.class);
	}

	// see comments for callSuperBeforeAll()
	public static void callSuperAfterAll(Object createdHere) {
		callSuper(createdHere, AfterAll.class);
	}

	private static void callSuper(Object createdHere, Class annotationClass) {
		try {
			Class<?> encClass = createdHere.getClass().getEnclosingClass();
			Class<?> theClass = encClass;
			Annotation annotation = null;
			Method invokedSuper = null;
			if (annotationClass != BeforeAll.class && annotationClass != AfterAll.class) {
				throw new RuntimeException("can only call super for BeforeAll and AfterAll " + annotationClass);
			}
			// look recursively for @BeforeAll or @AfterAll methods
			// when one is found and executed, do not continue the recursive search
			// as it is expected that the @BeforeAll or @AfterAll methods call
			// any super methods explicitly - perhaps using callSuperBeforeAll() or callSuperAfterAll()
			// Note that if the @BeforeAll and @AfterAll methods have different names, they will be
			// called twice - once by this callSuper() mechanism and once by junit as the method will not be hidden
			while ((theClass = theClass.getSuperclass()) != null) {
				Method[] methods = theClass.getMethods();
				for (Method m : methods) {
					annotation = m.getAnnotation(annotationClass);
					if (annotation != null) {
						if (annotation != null) {
							m.invoke(null);
							invokedSuper = m;
						}
					}
				}
				if (invokedSuper != null) { // called method is responsible for calling any super methods
					return;
				}

			}

		} catch (IllegalAccessException | InvocationTargetException e) {
			throw new RuntimeException(e);
		}
	}

}
