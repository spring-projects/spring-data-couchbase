package org.springframework.data.couchbase.core.convert;

import com.couchbase.client.core.encryption.CryptoManager;
import com.querydsl.codegen.Property;
import org.springframework.data.convert.PropertyValueConverter;
import org.springframework.data.convert.PropertyValueConverterFactory;
import org.springframework.data.convert.ValueConversionContext;

import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;
import java.util.HashMap;
import java.util.Map;

public class CouchbasePropertyValueConverterFactory implements PropertyValueConverterFactory {

  CryptoManager cryptoManager;
  Map<Class<? extends PropertyValueConverter<?,?,?>>,PropertyValueConverter<?,?,?>> converterCache = new HashMap<>();
  public CouchbasePropertyValueConverterFactory(CryptoManager cryptoManager){
    this.cryptoManager = cryptoManager;
  }
  @Override
  public <DV, SV, C extends ValueConversionContext<?>> PropertyValueConverter<DV, SV, C> getConverter(Class<? extends PropertyValueConverter<DV, SV, C>> converterType) {
    try {
      PropertyValueConverter<?, ?, ?> converter = converterCache.get(converterType);
      if(converter != null){
        return ( PropertyValueConverter<DV, SV, C>)converter;
      }
      Constructor cons = converterType.getConstructor(new Class[]{ CryptoManager.class});
      converter =( PropertyValueConverter<DV, SV, C>)cons.newInstance(cryptoManager);
      converterCache.put(converterType, converter);
      return ( PropertyValueConverter<DV, SV, C>)converter;
    } catch (InstantiationException e) {
      throw new RuntimeException(e);
    } catch (IllegalAccessException e) {
      throw new RuntimeException(e);
    } catch (NoSuchMethodException e) {
      throw new RuntimeException(e);
    } catch (InvocationTargetException e) {
      throw new RuntimeException(e);
    }
  }
}
