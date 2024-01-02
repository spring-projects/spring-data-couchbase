/*
 * Copyright 2012-2024 the original author or authors
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

import java.time.ZoneId;
import java.time.ZonedDateTime;

public class FixedDateTimeService implements DateTimeService {
	public static void main(String[] args) {
		System.out.println((new FixedDateTimeService()).getCurrentDateAndTime());
	}

	@Override
	public ZonedDateTime getCurrentDateAndTime() {
		return ZonedDateTime.of(2020, 1, 1, 0, 0, 0, 0, ZoneId.of("GMT-8"));
	}
}
