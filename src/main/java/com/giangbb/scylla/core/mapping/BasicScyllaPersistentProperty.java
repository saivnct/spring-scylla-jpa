/*
 * Copyright 2013-2024 the original author or authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *   https://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.giangbb.scylla.core.mapping;

import com.datastax.oss.driver.api.core.CqlIdentifier;
import com.datastax.oss.driver.api.mapper.annotations.ClusteringColumn;
import com.datastax.oss.driver.api.mapper.annotations.Computed;
import com.datastax.oss.driver.api.mapper.annotations.CqlName;
import com.datastax.oss.driver.api.mapper.annotations.PartitionKey;
import com.datastax.oss.driver.api.mapper.entity.naming.NamingConvention;
import org.springframework.context.ApplicationContext;
import org.springframework.context.ApplicationContextAware;
import org.springframework.context.expression.BeanFactoryAccessor;
import org.springframework.context.expression.BeanFactoryResolver;
import org.springframework.core.annotation.AnnotatedElementUtils;
import org.springframework.data.mapping.Association;
import org.springframework.data.mapping.model.AnnotationBasedPersistentProperty;
import org.springframework.data.mapping.model.Property;
import org.springframework.data.mapping.model.SimpleTypeHolder;
import org.springframework.data.util.Optionals;
import org.springframework.data.util.TypeInformation;
import org.springframework.expression.spel.support.StandardEvaluationContext;
import org.springframework.lang.Nullable;
import org.springframework.util.Assert;
import org.springframework.util.ClassUtils;
import org.springframework.util.ObjectUtils;
import com.giangbb.scylla.core.cql.Ordering;

import java.lang.annotation.Annotation;
import java.lang.reflect.AnnotatedParameterizedType;
import java.lang.reflect.AnnotatedType;
import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.util.Map;
import java.util.Optional;

/**
 * Scylla specific {@link AnnotationBasedPersistentProperty} implementation.
 *
 * @author Giangbb
 */
public class BasicScyllaPersistentProperty extends AnnotationBasedPersistentProperty<ScyllaPersistentProperty>
		implements ScyllaPersistentProperty, ApplicationContextAware {

	private final CqlIdentifierGenerator namingAccessor = new CqlIdentifierGenerator();

	private @Nullable CqlIdentifier columnName;

	private @Nullable StandardEvaluationContext spelContext;

	/**
	 * Create a new {@link BasicScyllaPersistentProperty}.
	 *
	 * @param property the actual {@link Property} in the domain entity corresponding to this persistent entity.
	 * @param owner the containing object or {@link ScyllaPersistentEntity} of this persistent property.
	 * @param simpleTypeHolder mapping of Java [simple|wrapper] types to Scylla data types.
	 */
	public BasicScyllaPersistentProperty(Property property, ScyllaPersistentEntity<?> owner,
                                         SimpleTypeHolder simpleTypeHolder) {

		super(property, owner, simpleTypeHolder);
	}

	@Override
	public void setApplicationContext(ApplicationContext context) {

		Assert.notNull(context, "ApplicationContext must not be null");

		this.spelContext = new StandardEvaluationContext();
		this.spelContext.addPropertyAccessor(new BeanFactoryAccessor());
		this.spelContext.setBeanResolver(new BeanFactoryResolver(context));
		this.spelContext.setRootObject(context);
	}

	@Override
	public ScyllaPersistentEntity<?> getOwner() {
		return (ScyllaPersistentEntity<?>) super.getOwner();
	}

	@Override
	public boolean isTransient() {
		//Not include @Computed properties
		return super.isTransient() || isAnnotationPresent(Computed.class);
	}

	@Override
	public CqlIdentifier getColumnName() {

		if (this.columnName == null) {
			this.columnName = determineColumnName();
		}

		if (columnName == null) {
			throw new IllegalStateException(String.format("Cannot determine column name for %s", this));
		}

		return this.columnName;
	}

	@Nullable
	@Override
	public Integer getOrdinal() {
		return null;
	}

	@Nullable
	@Override
	public Ordering getClusteringKeyOrdering() {
		ClusteringColumn annotation = findAnnotation(ClusteringColumn.class);
		if (annotation == null) {
			return null;
		}

		ClusteringOrder orderAnnotation = findAnnotation(ClusteringOrder.class);
		return orderAnnotation != null ? orderAnnotation.value() : Ordering.ASCENDING;
	}

	@Override
	public boolean isClusterKeyColumn() {
		return isAnnotationPresent(ClusteringColumn.class);
	}

	@Override
	public boolean isPartitionKeyColumn() {
		return isAnnotationPresent(PartitionKey.class);
	}

	@Override
	public boolean isPrimaryKeyColumn() {
		return isAnnotationPresent(PartitionKey.class)
				|| isAnnotationPresent(ClusteringColumn.class);
	}

	@Override
	public boolean isStaticColumn() {
		return isAnnotationPresent(StaticColumn.class);
	}

	@Nullable
	private CqlIdentifier determineColumnName() {
		String overriddenName = null;
		CqlName cqlName = findAnnotation(CqlName.class);

		if (cqlName != null) {
			overriddenName = cqlName.value();
		}

		return namingAccessor.generate(overriddenName, false, NamingGenerator::getCQLName, this, this.spelContext);
	}

	@Override
	public boolean hasExplicitColumnName() {
		CqlName cqlName = findAnnotation(CqlName.class);
		return cqlName != null && !ObjectUtils.isEmpty(cqlName.value());
	}

	@Override
	public void setColumnName(CqlIdentifier columnName) {

		Assert.notNull(columnName, "ColumnName must not be null");

		this.columnName = columnName;
	}

	/**
	 * Set the {@link NamingConvention} to use.
	 *
	 * @param namingStrategy must not be {@literal null}.
	 */
	public void setNamingStrategy(NamingConvention namingStrategy) {
		this.namingAccessor.setNamingStrategy(namingStrategy);
	}

	@Override
	public Association<ScyllaPersistentProperty> getAssociation() {
		return null;
	}

	@Override
	protected Association<ScyllaPersistentProperty> createAssociation() {
		return new Association<>(this, this);
	}

	@Override
	public boolean isMapLike() {
		return ClassUtils.isAssignable(Map.class, getType());
	}

	@Override
	public AnnotatedType findAnnotatedType(Class<? extends Annotation> annotationType) {

		return Optionals
				.toStream(Optional.ofNullable(getField()).map(Field::getAnnotatedType),
						Optional.ofNullable(getGetter()).map(Method::getAnnotatedReturnType),
						Optional.ofNullable(getSetter()).map(it -> it.getParameters()[0].getAnnotatedType()))
				.filter(it -> hasAnnotation(it, annotationType, getTypeInformation())).findFirst().orElse(null);
	}

	private static boolean hasAnnotation(AnnotatedType type, Class<? extends Annotation> annotationType,
			TypeInformation<?> typeInformation) {

		if (AnnotatedElementUtils.hasAnnotation(type, annotationType)) {
			return true;
		}

		if (type instanceof AnnotatedParameterizedType) {

			AnnotatedParameterizedType parameterizedType = (AnnotatedParameterizedType) type;
			AnnotatedType[] arguments = parameterizedType.getAnnotatedActualTypeArguments();

			if (typeInformation.isCollectionLike() && arguments.length == 1) {
				return AnnotatedElementUtils.hasAnnotation(arguments[0], annotationType);
			}

			if (typeInformation.isMap() && arguments.length == 2) {
				return AnnotatedElementUtils.hasAnnotation(arguments[0], annotationType)
						|| AnnotatedElementUtils.hasAnnotation(arguments[1], annotationType);
			}
		}

		return false;
	}
}
