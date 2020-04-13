package org.springframework.data.couchbase.domain.time;

import java.time.ZoneId;
import java.time.ZonedDateTime;

public class FixedDateTimeService implements DateTimeService {
	@Override
	public ZonedDateTime getCurrentDateAndTime() {
		return ZonedDateTime.of(2020, 1, 1, 0, 0, 0, 0, ZoneId.of("GMT-8"));
	}

	public static void main(String[] args) {
		System.out.println((new FixedDateTimeService()).getCurrentDateAndTime());
	}
}
