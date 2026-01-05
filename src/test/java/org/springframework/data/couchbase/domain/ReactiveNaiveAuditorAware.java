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
package org.springframework.data.couchbase.domain;

import reactor.core.publisher.Mono;

import org.springframework.data.domain.ReactiveAuditorAware;

/**
 * This class returns a string that represents the current user
 *
 * @author Jorge Rodríguez Martín
 * @since 4.2
 */
public class ReactiveNaiveAuditorAware implements ReactiveAuditorAware<String> {

	public static final String AUDITOR = "reactive_auditor";

	@Override
	public Mono<String> getCurrentAuditor() {
		return Mono.just(AUDITOR);
	}

}
