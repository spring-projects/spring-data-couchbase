/*
 * Copyright 2017-2020 the original author or authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      https://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.springframework.data.couchbase.core.convert;

import static java.time.Instant.*;
import static java.time.LocalDateTime.*;
import static java.time.ZoneId.*;

import java.time.Duration;
import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.Period;
import java.time.ZoneId;
import java.time.ZonedDateTime;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Date;
import java.util.List;

import org.springframework.core.convert.converter.Converter;
import org.springframework.data.convert.ReadingConverter;
import org.springframework.data.convert.WritingConverter;

/**
 * Helper class to register JSR-310 specific {@link Converter} implementations.
 *
 * @author Oliver Gierke
 * @author Barak Schoster
 * @author Christoph Strobl
 * @author Subhashni Balakrishnan
 */
public final class CouchbaseJsr310Converters {

	private CouchbaseJsr310Converters() {

	}

	/**
	 * Returns the converters to be registered
	 *
	 * @return
	 */
	public static Collection<Converter<?, ?>> getConvertersToRegister() {
		List<Converter<?, ?>> converters = new ArrayList<>();
		converters.add(NumberToLocalDateTimeConverter.INSTANCE);
		converters.add(LocalDateTimeToLongConverter.INSTANCE);
		converters.add(NumberToLocalDateConverter.INSTANCE);
		converters.add(LocalDateToLongConverter.INSTANCE);
		converters.add(NumberToLocalTimeConverter.INSTANCE);
		converters.add(LocalTimeToLongConverter.INSTANCE);
		converters.add(NumberToInstantConverter.INSTANCE);
		converters.add(InstantToLongConverter.INSTANCE);
		converters.add(ZoneIdToStringConverter.INSTANCE);
		converters.add(StringToZoneIdConverter.INSTANCE);
		converters.add(DurationToStringConverter.INSTANCE);
		converters.add(StringToDurationConverter.INSTANCE);
		converters.add(PeriodToStringConverter.INSTANCE);
		converters.add(StringToPeriodConverter.INSTANCE);
		converters.add(ZonedDateTimeToLongConverter.INSTANCE);
		converters.add(NumberToZonedDateTimeConverter.INSTANCE);

		return converters;
	}

	@ReadingConverter
	public enum NumberToLocalDateTimeConverter implements Converter<Number, LocalDateTime> {

		INSTANCE;

		@Override
		public LocalDateTime convert(Number source) {
			return source == null ? null
					: ofInstant(DateConverters.SerializedObjectToDateConverter.INSTANCE.convert(source).toInstant(),
							systemDefault());
		}
	}

	@WritingConverter
	public enum LocalDateTimeToLongConverter implements Converter<LocalDateTime, Long> {

		INSTANCE;

		@Override
		public Long convert(LocalDateTime source) {
			return source == null ? null
					: DateConverters.DateToLongConverter.INSTANCE.convert(Date.from(source.atZone(systemDefault()).toInstant()));
		}
	}

	@ReadingConverter
	public enum NumberToZonedDateTimeConverter implements Converter<Number, ZonedDateTime> {

		INSTANCE;

		@Override
		public ZonedDateTime convert(Number source) {
			return source == null ? null
					: ZonedDateTime.ofInstant(DateConverters.SerializedObjectToDateConverter.INSTANCE.convert(source).toInstant(),
					systemDefault());
		}
	}

	@WritingConverter
	public enum ZonedDateTimeToLongConverter implements Converter<ZonedDateTime, Long> {

		INSTANCE;

		@Override
		public Long convert(ZonedDateTime source) {
			return source == null ? null
					: DateConverters.DateToLongConverter.INSTANCE.convert(Date.from(source.toInstant()));
		}
	}

	@ReadingConverter
	public enum NumberToLocalDateConverter implements Converter<Number, LocalDate> {

		INSTANCE;

		@Override
		public LocalDate convert(Number source) {
			return source == null ? null
					: ofInstant(ofEpochMilli(DateConverters.SerializedObjectToDateConverter.INSTANCE.convert(source).getTime()),
							systemDefault()).toLocalDate();
		}
	}

	@WritingConverter
	public enum LocalDateToLongConverter implements Converter<LocalDate, Long> {

		INSTANCE;

		@Override
		public Long convert(LocalDate source) {
			return source == null ? null
					: DateConverters.DateToLongConverter.INSTANCE
							.convert(Date.from(source.atStartOfDay(systemDefault()).toInstant()));
		}
	}

	@ReadingConverter
	public enum NumberToLocalTimeConverter implements Converter<Number, LocalTime> {

		INSTANCE;

		@Override
		public LocalTime convert(Number source) {
			return source == null ? null
					: ofInstant(ofEpochMilli(DateConverters.SerializedObjectToDateConverter.INSTANCE.convert(source).getTime()),
							systemDefault()).toLocalTime();
		}
	}

	@WritingConverter
	public enum LocalTimeToLongConverter implements Converter<LocalTime, Long> {

		INSTANCE;

		@Override
		public Long convert(LocalTime source) {
			return source == null ? null
					: DateConverters.DateToLongConverter.INSTANCE
							.convert(Date.from(source.atDate(LocalDate.now()).atZone(systemDefault()).toInstant()));
		}
	}

	@ReadingConverter
	public enum NumberToInstantConverter implements Converter<Number, Instant> {

		INSTANCE;

		@Override
		public Instant convert(Number source) {
			return source == null ? null
					: DateConverters.SerializedObjectToDateConverter.INSTANCE.convert(source).toInstant();
		}
	}

	@WritingConverter
	public enum InstantToLongConverter implements Converter<Instant, Long> {

		INSTANCE;

		@Override
		public Long convert(Instant source) {
			return source == null ? null
					: DateConverters.DateToLongConverter.INSTANCE.convert(Date.from(source.atZone(systemDefault()).toInstant()));
		}
	}

	@WritingConverter
	public enum ZoneIdToStringConverter implements Converter<ZoneId, String> {

		INSTANCE;

		@Override
		public String convert(ZoneId source) {
			return source.toString();
		}
	}

	@ReadingConverter
	public enum StringToZoneIdConverter implements Converter<String, ZoneId> {

		INSTANCE;

		@Override
		public ZoneId convert(String source) {
			return ZoneId.of(source);
		}
	}

	@WritingConverter
	public enum DurationToStringConverter implements Converter<Duration, String> {

		INSTANCE;

		@Override
		public String convert(Duration duration) {
			return duration.toString();
		}
	}

	@ReadingConverter
	public enum StringToDurationConverter implements Converter<String, Duration> {

		INSTANCE;

		@Override
		public Duration convert(String s) {
			return Duration.parse(s);
		}
	}

	@WritingConverter
	public enum PeriodToStringConverter implements Converter<Period, String> {

		INSTANCE;

		@Override
		public String convert(Period period) {
			return period.toString();
		}
	}

	@ReadingConverter
	public enum StringToPeriodConverter implements Converter<String, Period> {

		INSTANCE;

		@Override
		public Period convert(String s) {
			return Period.parse(s);
		}
	}
}
