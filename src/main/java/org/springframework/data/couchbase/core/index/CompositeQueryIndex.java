package org.springframework.data.couchbase.core.index;

import java.lang.annotation.Documented;
import java.lang.annotation.ElementType;
import java.lang.annotation.Repeatable;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

@Target({ ElementType.TYPE })
@Documented
@Repeatable(CompositeQueryIndexes.class)
@Retention(RetentionPolicy.RUNTIME)
public @interface CompositeQueryIndex {

	String[] fields();

	String name() default "";

}
