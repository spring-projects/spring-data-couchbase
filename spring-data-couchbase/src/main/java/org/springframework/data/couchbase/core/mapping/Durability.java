package org.springframework.data.couchbase.core.mapping;

import com.couchbase.client.core.msg.kv.DurabilityLevel;
import org.springframework.data.annotation.Persistent;

import java.lang.annotation.*;

/**
 * Durability annotation
 *
 * @author Tigran Babloyan
 */
@Persistent
@Inherited
@Retention(RetentionPolicy.RUNTIME)
@Target({ ElementType.TYPE, ElementType.ANNOTATION_TYPE })
public @interface Durability {
    /**
     * The optional durabilityLevel for all mutating operations, allows the application to wait until this replication
     * (or persistence) is successful before proceeding
     */
    DurabilityLevel durabilityLevel() default DurabilityLevel.NONE;

    /**
     * Same as {@link #durabilityLevel()} but allows the actual value to be set using standard Spring property sources mechanism.
     * Only one might be set at the same time: either {@link #durabilityLevel()} or {@link #durabilityExpression()}. <br />
     * Syntax is the same as for {@link org.springframework.core.env.Environment#resolveRequiredPlaceholders(String)}.
     * <br />
     * SpEL is NOT supported.
     */
    String durabilityExpression() default "";
}
