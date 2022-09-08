package org.springframework.data.couchbase.core.convert;

import org.springframework.core.convert.ConversionFailedException;
import org.springframework.core.convert.ConverterNotFoundException;
import org.springframework.core.convert.TypeDescriptor;
import org.springframework.core.convert.converter.GenericConverter;
import org.springframework.core.convert.support.GenericConversionService;
import org.springframework.lang.Nullable;
import org.springframework.util.Assert;

public class CouchbaseDefaultConversionService extends GenericConversionService {
  public CouchbaseDefaultConversionService() {
    super();
  }
  @Override
  @Nullable
  public Object convert(@Nullable Object source, @Nullable TypeDescriptor sourceType, TypeDescriptor targetType) {
    Assert.notNull(targetType, "Target type to convert to cannot be null");
    if (sourceType == null) {
      Assert.isTrue(source == null, "Source must be [null] if source type == [null]");
      return handleResult(null, targetType, convertNullSource(null, targetType));
    }
    if (source != null && !sourceType.getObjectType().isInstance(source)) {
      throw new IllegalArgumentException("Source to convert from must be an instance of [" +
          sourceType + "]; instead it was a [" + source.getClass().getName() + "]");
    }
    GenericConverter converter = getConverter(sourceType, targetType);
    if (converter != null) {
      Object result = invokeConverter(converter, source, sourceType, targetType);
      return handleResult(sourceType, targetType, result);
    }
    return handleConverterNotFound(source, sourceType, targetType);
  }

  @Nullable
  public static Object invokeConverter(GenericConverter converter, @Nullable Object source,
                                       TypeDescriptor sourceType, TypeDescriptor targetType) {

    try {
      return converter.convert(source, sourceType, targetType);
    }
    catch (ConversionFailedException ex) {
      throw ex;
    }
    catch (Throwable ex) {
      throw new ConversionFailedException(sourceType, targetType, source, ex);
    }
  }

  @Nullable
  private Object handleConverterNotFound(
      @Nullable Object source, @Nullable TypeDescriptor sourceType, TypeDescriptor targetType) {

    if (source == null) {
      assertNotPrimitiveTargetType(sourceType, targetType);
      return null;
    }
    if ((sourceType == null || sourceType.isAssignableTo(targetType)) &&
        targetType.getObjectType().isInstance(source)) {
      return source;
    }
    throw new ConverterNotFoundException(sourceType, targetType);
  }

  @Nullable
  private Object handleResult(@Nullable TypeDescriptor sourceType, TypeDescriptor targetType, @Nullable Object result) {
    if (result == null) {
      assertNotPrimitiveTargetType(sourceType, targetType);
    }
    return result;
  }

  private void assertNotPrimitiveTargetType(@Nullable TypeDescriptor sourceType, TypeDescriptor targetType) {
    if (targetType.isPrimitive()) {
      throw new ConversionFailedException(sourceType, targetType, null,
          new IllegalArgumentException("A null value cannot be assigned to a primitive type"));
    }
  }


}
