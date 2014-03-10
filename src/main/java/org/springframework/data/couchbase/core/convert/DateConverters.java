/*
 * Copyright 2014 the original author or authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.springframework.data.couchbase.core.convert;

import org.springframework.core.convert.converter.Converter;
import org.springframework.data.convert.ReadingConverter;
import org.springframework.data.convert.WritingConverter;

import java.util.ArrayList;
import java.util.Calendar;
import java.util.Collection;
import java.util.Date;
import java.util.List;

/**
 * Out of the box conversions for java dates and calendars.
 *
 * @author Michael Nitschinger
 */
public final class DateConverters {

  private DateConverters() {}

  public static Collection<Converter<?, ?>> getConvertersToRegister() {
    List<Converter<?, ?>> converters = new ArrayList<Converter<?, ?>>();
    converters.add(DateToLongConverter.INSTANCE);
    converters.add(CalendarToLongConverter.INSTANCE);

    converters.add(LongToDateConverter.INSTANCE);
    converters.add(LongToCalendarConverter.INSTANCE);
    return converters;
  }

  @WritingConverter
  public enum DateToLongConverter implements Converter<Date, Long> {
    INSTANCE;

    @Override
    public Long convert(Date source) {
      return source.getTime();
    }
  }

  @WritingConverter
  public enum CalendarToLongConverter implements Converter<Calendar, Long> {
    INSTANCE;

    @Override
    public Long convert(Calendar source) {
      return source.getTimeInMillis() / 1000;
    }
  }

  @ReadingConverter
  public enum LongToDateConverter implements Converter<Long, Date> {
    INSTANCE;

    @Override
    public Date convert(Long source) {
      Date date = new Date();
      date.setTime(source);
      return date;
    }
  }

  @ReadingConverter
  public enum LongToCalendarConverter implements Converter<Long, Calendar> {
    INSTANCE;

    @Override
    public Calendar convert(Long source) {
      Calendar calendar = Calendar.getInstance();
      calendar.setTimeInMillis(source * 1000);
      return calendar;
    }
  }

}
