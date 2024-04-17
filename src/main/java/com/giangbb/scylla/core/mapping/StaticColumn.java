package com.giangbb.scylla.core.mapping;

import java.lang.annotation.*;


/**
 * Created by giangbb on 07/04/2024
 */
@Documented
@Retention(RetentionPolicy.RUNTIME)
@Target(value = { ElementType.ANNOTATION_TYPE, ElementType.FIELD, ElementType.METHOD, ElementType.PARAMETER })
public @interface StaticColumn {
}
