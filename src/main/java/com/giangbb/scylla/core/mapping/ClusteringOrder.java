package com.giangbb.scylla.core.mapping;

 import com.giangbb.scylla.core.cql.Ordering;

 import java.lang.annotation.*;


/**
 * Created by giangbb on 07/04/2024
 */
@Documented
@Retention(value = RetentionPolicy.RUNTIME)
@Target(value = { ElementType.ANNOTATION_TYPE, ElementType.FIELD, ElementType.METHOD })
public @interface ClusteringOrder {
    Ordering value() default Ordering.ASCENDING;
}
