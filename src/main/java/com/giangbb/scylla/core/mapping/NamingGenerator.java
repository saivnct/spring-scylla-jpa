package com.giangbb.scylla.core.mapping;

import com.datastax.oss.driver.api.mapper.entity.naming.NamingConvention;
import com.squareup.javapoet.CodeBlock;

/**
 * Created by giangbb on 07/04/2024
 */
public class NamingGenerator {

    public static  <T> String getCQLName(NamingConvention namingConvention, T element) {
        String entityJavaName;
        if (element instanceof ScyllaPersistentEntity){
            entityJavaName = ((ScyllaPersistentEntity<?>) element).getType().getSimpleName();
        }else if (element instanceof ScyllaPersistentProperty) {
            entityJavaName = ((ScyllaPersistentProperty) element).getName();
        }else{
            throw new IllegalArgumentException("Unsupported element type: " + element.getClass().getName());
        }

        String cqlName = buildCqlName(entityJavaName, namingConvention).toString();
//        System.out.println("getCQLName for entityJavaName: " + entityJavaName + " with namingConvention: " + namingConvention.toString() + " is: " + cqlName);
        return cqlName;
    }

    public static CodeBlock buildCqlName(String javaName, NamingConvention namingConvention) {
        if (namingConvention != null) {
            return CodeBlock.of("$S", BuiltInNameConversions.toCassandraName(javaName, namingConvention));
        } else {
            return CodeBlock.of(
                    "context.getNameConverter($T.class).toCassandraName($S)", null, javaName);
        }
    }
}
