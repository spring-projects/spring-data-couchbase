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

package org.springframework.data.couchbase.domain;

import org.springframework.data.domain.AuditorAware;

import java.util.Optional;

// These are the classes that would be used for a real getCurrentAuditor() implementation
//import org.springframework.security.core.Authentication;
//import org.springframework.security.core.context.SecurityContextHolder;
//import org.springframework.security.core.userdetails.User;

/**
 * This class returns a string that represents the current user
 *
 * @author Michael Reiche
 * @since 3.0
 */
public class NaiveAuditorAware implements AuditorAware<String> {

	private Optional<String> auditor = Optional.of("auditor");

	@Override
	public Optional<String> getCurrentAuditor() {
		return auditor;
	}

	public void setAuditor(String auditor) {
		this.auditor = Optional.of(auditor);
	}
}
