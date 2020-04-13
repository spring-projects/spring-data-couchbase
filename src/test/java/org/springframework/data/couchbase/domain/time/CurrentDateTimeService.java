package org.springframework.data.couchbase.domain.time;

import java.time.ZonedDateTime;

public class CurrentDateTimeService implements DateTimeService {
	@Override
	public ZonedDateTime getCurrentDateAndTime() {
		return ZonedDateTime.now();
	}
}
