/*
 * Copyright 2012-present the original author or authors
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

import java.util.Optional;

import org.junit.jupiter.api.extension.BeforeAllCallback;
import org.junit.jupiter.api.extension.ConditionEvaluationResult;
import org.junit.jupiter.api.extension.ExecutionCondition;
import org.junit.jupiter.api.extension.ExtensionContext;
import org.junit.jupiter.api.extension.ParameterContext;
import org.junit.jupiter.api.extension.ParameterResolver;
import org.junit.platform.commons.support.AnnotationSupport;

/**
 * This invocation provider starts and stops the couchbase cluster before and after all tests are executed.
 * <p>
 * Note that the internals on what and how the containers are managed is up to the {@link TestCluster} implementation,
 * and if it is unmanaged than this very well be mostly a "stub".
 *
 * @since 2.0.0
 */
public class ClusterInvocationProvider implements BeforeAllCallback, ParameterResolver, ExecutionCondition {

	/**
	 * Identifier for the container in the root store.
	 */
	private static final String STORE_KEY = "db";

	/**
	 * Helper method to initialize the test cluster when not initialized yet.
	 *
	 * @param ctx the junit extension context.
	 * @return the test cluster initialized.
	 */
	private TestCluster accessAndMaybeInitTestCluster(final ExtensionContext ctx) {
		return (TestCluster) rootStore(ctx).getOrComputeIfAbsent(STORE_KEY, key -> {
			TestCluster testCluster = TestCluster.create();
			testCluster.start();
			return testCluster;
		});
	}

	@Override
	public void beforeAll(final ExtensionContext ctx) {
		accessAndMaybeInitTestCluster(ctx);
	}

	/**
	 * Grabs the root store from the global namespace in junit.
	 *
	 * @param ctx the extension context from the root store.
	 * @return the loaded store.
	 */
	private ExtensionContext.Store rootStore(final ExtensionContext ctx) {
		return ctx.getParent().get().getStore(ExtensionContext.Namespace.GLOBAL);
	}

	@Override
	public boolean supportsParameter(final ParameterContext pCtx, final ExtensionContext eCtx) {
		return pCtx.getParameter().getType().isAssignableFrom(TestClusterConfig.class);
	}

	@Override
	public Object resolveParameter(final ParameterContext pCtx, final ExtensionContext eCtx) {
		return ((TestCluster) rootStore(eCtx).get(STORE_KEY)).config();
	}

	@Override
	public ConditionEvaluationResult evaluateExecutionCondition(ExtensionContext context) {
		TestCluster testCluster = accessAndMaybeInitTestCluster(context);

		Optional<IgnoreWhen> annotation = AnnotationSupport.findAnnotation(context.getElement(), IgnoreWhen.class);
		if (annotation.isPresent()) {
			IgnoreWhen found = annotation.get();
			for (ClusterType type : found.clusterTypes()) {
				if (testCluster.type().equals(type)) {
					return ConditionEvaluationResult
							.disabled("Test disabled on this ClusterType (" + type + ") based on @IgnoreWhen");
				}
			}
			int numNodes = testCluster.config().nodes().size();
			if (numNodes > found.nodesGreaterThan() || numNodes < found.nodesLessThan()) {
				return ConditionEvaluationResult
						.disabled("Test disabled on the number of nodes (" + numNodes + ") based on @IgnoreWhen");
			}
			int numReplicas = testCluster.config().numReplicas();
			if (numReplicas > found.replicasGreaterThan() || numReplicas < found.replicasLessThan()) {
				return ConditionEvaluationResult
						.disabled("Test disabled on the number of replicas (" + numReplicas + ") based on @IgnoreWhen");
			}
			for (Capabilities neededCapability : found.missesCapabilities()) {
				if (!testCluster.config().capabilities().contains(neededCapability)) {
					return ConditionEvaluationResult.disabled("Test disabled because capability of " + neededCapability
							+ " is missing on cluster based on @IgnoreWhen");
				}
			}
			for (Capabilities unwantedCapability : found.hasCapabilities()) {
				if (testCluster.config().capabilities().contains(unwantedCapability)) {
					return ConditionEvaluationResult.disabled("Test disabled because capability of " + unwantedCapability
							+ " is present on cluster based on @IgnoreWhen");
				}
			}
		}
		return ConditionEvaluationResult.enabled("Test is allowed to run based on @IgnoreWhen");
	}

}
