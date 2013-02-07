/**
 * Copyright (C) 2009-2012 Couchbase, Inc.
 *
 * Permission is hereby granted, free of charge, to any person obtaining a copy
 * of this software and associated documentation files (the "Software"), to deal
 * in the Software without restriction, including without limitation the rights
 * to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
 * copies of the Software, and to permit persons to whom the Software is
 * furnished to do so, subject to the following conditions:
 *
 * The above copyright notice and this permission notice shall be included in
 * all copies or substantial portions of the Software.
 *
 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
 * IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
 * FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
 * AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
 * LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING
 * FROM, OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALING
 * IN THE SOFTWARE.
 */

package com.couchbase.spring.core.convert;

import com.couchbase.spring.core.mapping.ConvertedCouchbaseDocument;
import com.couchbase.spring.core.mapping.CouchbasePersistentEntity;
import com.couchbase.spring.core.mapping.CouchbasePersistentProperty;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.lang.reflect.Field;

import org.codehaus.jackson.JsonEncoding;
import org.codehaus.jackson.JsonFactory;
import org.codehaus.jackson.JsonGenerator;
import org.codehaus.jackson.JsonParser;
import org.codehaus.jackson.JsonToken;
import org.springframework.beans.BeansException;
import org.springframework.context.ApplicationContext;
import org.springframework.context.ApplicationContextAware;
import org.springframework.core.convert.support.ConversionServiceFactory;
import org.springframework.data.mapping.context.MappingContext;
import org.springframework.data.mapping.model.BeanWrapper;
import org.springframework.data.mapping.model.MappingException;
import org.springframework.data.util.ClassTypeInformation;
import org.springframework.data.util.TypeInformation;
import org.springframework.data.mapping.PropertyHandler;

public class MappingCouchbaseConverter extends AbstractCouchbaseConverter
  implements ApplicationContextAware {

  protected ApplicationContext applicationContext;
  protected final MappingContext<? extends CouchbasePersistentEntity<?>,
      CouchbasePersistentProperty> mappingContext;

  @SuppressWarnings("deprecation")
  public MappingCouchbaseConverter(MappingContext<? extends CouchbasePersistentEntity<?>,
      CouchbasePersistentProperty> mappingContext) {
    super(ConversionServiceFactory.createDefaultConversionService());

    this.mappingContext = mappingContext;
  }

  @Override
  public MappingContext<? extends CouchbasePersistentEntity<?>,
    CouchbasePersistentProperty> getMappingContext() {
    return mappingContext;
  }

  @Override
  public <R> R read(Class<R> type, ConvertedCouchbaseDocument doc) {
  	CouchbasePersistentEntity<?> entity  = mappingContext.getPersistentEntity(type);
  	
  	R decoded = null;
  	try {
  		decoded = type.getConstructor(String.class).newInstance(doc.getId());
  	} catch(Exception e) {
  		throw new MappingException("Could not instantiate object while converting "
          + doc.getId()); 	
  	}
  	
  	JsonFactory jsonFactory = new JsonFactory();
  	try {
  		JsonParser parser = jsonFactory.createJsonParser(doc.getValue());
  		parser.nextToken();
  		while(parser.nextToken() != JsonToken.END_OBJECT) {
  			String fieldname = parser.getCurrentName();
  			parser.nextToken();
  			CouchbasePersistentProperty property = entity.getPersistentProperty(fieldname);
  			if(property == null) {
  				continue;
  			}
  			Field field = type.getDeclaredField(property.getOriginalName());
  			field.setAccessible(true);
  			
  			if(property.getType().equals(boolean.class)) {
  				field.set(decoded, parser.getValueAsBoolean());
  			} else if(property.getType().equals(String.class)) {
  				field.set(decoded, parser.getText());
  			} else {
  				throw new MappingException("Unknown type in JSON found: " + property.getType());
  			}
  		}
  	} catch(Exception e) {
      throw new MappingException("Could not read from JSON while converting "
          + doc.getId(), e); 		
  	}
  	
		return decoded;
  }

  @Override
  public void write(Object source, ConvertedCouchbaseDocument target) {
    if(source == null) {
      return;
    }

    TypeInformation<? extends Object> type = ClassTypeInformation.from(source.getClass());
    try {
      writeInternal(source, target, type);
    } catch (IOException ex) {
      throw new MappingException("Could not translate to JSON while converting "
        + source.getClass().getName());
    }

  }

  protected void writeInternal(final Object source,
    ConvertedCouchbaseDocument target, TypeInformation<?> type)
    throws IOException {
    CouchbasePersistentEntity<?> entity  = mappingContext.getPersistentEntity(
      source.getClass());

    if(entity == null) {
      throw new MappingException("No mapping metadata found for entity of type "
        + source.getClass().getName());
    }

    final CouchbasePersistentProperty idProperty = entity.getIdProperty();
    if(idProperty == null) {
      throw new MappingException("ID property required for entity of type "
        + source.getClass().getName());
    }

    final BeanWrapper<CouchbasePersistentEntity<Object>, Object> wrapper =
      BeanWrapper.create(source, conversionService);

    String id = wrapper.getProperty(idProperty, String.class, false);
    target.setId(id);
    target.setExpiry(entity.getExpiry());

    JsonFactory jsonFactory = new JsonFactory();
    OutputStream jsonStream = new ByteArrayOutputStream();
    final JsonGenerator jsonGenerator = jsonFactory.createJsonGenerator(
      jsonStream, JsonEncoding.UTF8);

    jsonGenerator.writeStartObject();
    entity.doWithProperties(new PropertyHandler<CouchbasePersistentProperty>() {
      @Override
      public void doWithPersistentProperty(CouchbasePersistentProperty prop) {
        if(prop.equals(idProperty)) {
          return;
        }

        Object propertyValue = wrapper.getProperty(prop, prop.getType(), false);
        if(propertyValue != null) {
          try {
            jsonGenerator.writeFieldName(prop.getFieldName());
            jsonGenerator.writeObject(propertyValue);
          } catch (IOException ex) {
            throw new MappingException("Could not translate to JSON while converting "
              + source.getClass().getName());
          }
        }

      }
    });
    jsonGenerator.writeEndObject();
    jsonGenerator.close();

    target.setValue(jsonStream.toString());
  }

  @Override
  public void setApplicationContext(ApplicationContext applicationContext)
    throws BeansException {
    this.applicationContext = applicationContext;
  }

}
