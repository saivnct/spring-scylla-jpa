package com.giangbb.scylla.core.mapping;

import com.datastax.oss.driver.api.mapper.entity.naming.NamingConvention;
import com.datastax.oss.driver.internal.core.util.Strings;
import com.datastax.oss.driver.shaded.guava.common.base.CaseFormat;

/**
 * Created by giangbb on 17 Apr, 2024
 */
public class BuiltInNameConversions {
    public static String toCassandraName(String javaName, NamingConvention convention) {
        switch (convention) {
            case CASE_INSENSITIVE:
                return javaName;
            case EXACT_CASE:
                return Strings.doubleQuote(javaName);
            case LOWER_CAMEL_CASE:
                // Piggy-back on Guava's CaseFormat. Note that we indicate that the input is upper-camel
                // when in reality it can be lower-camel for a property name, but CaseFormat is lenient and
                // handles that correctly.
                return Strings.doubleQuote(CaseFormat.UPPER_CAMEL.to(CaseFormat.LOWER_CAMEL, javaName));
            case UPPER_CAMEL_CASE:
                return Strings.doubleQuote(CaseFormat.LOWER_CAMEL.to(CaseFormat.UPPER_CAMEL, javaName));
            case SNAKE_CASE_INSENSITIVE:
                return CaseFormat.LOWER_CAMEL.to(CaseFormat.LOWER_UNDERSCORE, javaName);
            case UPPER_SNAKE_CASE:
                return Strings.doubleQuote(
                        CaseFormat.LOWER_CAMEL.to(CaseFormat.UPPER_UNDERSCORE, javaName));
            case UPPER_CASE:
                return Strings.doubleQuote(javaName.toUpperCase());
            default:
                throw new AssertionError("Unsupported convention: " + convention);
        }
    }
}
