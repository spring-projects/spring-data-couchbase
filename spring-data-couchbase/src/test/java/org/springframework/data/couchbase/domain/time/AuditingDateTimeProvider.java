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
package org.springframework.data.couchbase.domain.time;

import java.time.Instant;
import java.time.temporal.TemporalAccessor;
import java.util.Optional;

import org.springframework.data.auditing.DateTimeProvider;

public class AuditingDateTimeProvider implements DateTimeProvider {

	private DateTimeService dateTimeService = new FixedDateTimeService();

	public AuditingDateTimeProvider() {}

	public AuditingDateTimeProvider(DateTimeService dateTimeService) {
		this.dateTimeService = dateTimeService;
	}

	@Override
	public Optional<TemporalAccessor> getNow() {
		return Optional.of(Instant.ofEpochSecond(dateTimeService.getCurrentDateAndTime().toEpochSecond()));
	}
}
