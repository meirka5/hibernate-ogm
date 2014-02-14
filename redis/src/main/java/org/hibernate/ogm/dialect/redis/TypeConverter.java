/*
 * Hibernate, Relational Persistence for Idiomatic Java
 *
 * JBoss, Home of Professional Open Source
 * Copyright 2013 Red Hat Inc. and/or its affiliates and other contributors
 * as indicated by the @authors tag. All rights reserved.
 * See the copyright.txt in the distribution for a
 * full listing of individual contributors.
 *
 * This copyrighted material is made available to anyone wishing to use,
 * modify, copy, or redistribute it subject to the terms and conditions
 * of the GNU Lesser General Public License, v. 2.1.
 * This program is distributed in the hope that it will be useful, but WITHOUT A
 * WARRANTY; without even the implied warranty of MERCHANTABILITY or FITNESS FOR A
 * PARTICULAR PURPOSE.  See the GNU Lesser General Public License for more details.
 * You should have received a copy of the GNU Lesser General Public License,
 * v.2.1 along with this distribution; if not, write to the Free Software
 * Foundation, Inc., 51 Franklin Street, Fifth Floor, Boston,
 * MA  02110-1301, USA.
 */
package org.hibernate.ogm.dialect.redis;

import java.util.HashMap;
import java.util.Map;

import org.hibernate.ogm.dialect.redis.type.BooleanAsStringType;
import org.hibernate.ogm.dialect.redis.type.ByteAsStringType;
import org.hibernate.ogm.dialect.redis.type.DoubleAsStringType;
import org.hibernate.ogm.dialect.redis.type.IntegerAsStringType;
import org.hibernate.ogm.dialect.redis.type.LongAsStringType;
import org.hibernate.ogm.dialect.redis.type.PrimitiveByteAsStringType;
import org.hibernate.ogm.type.BigDecimalType;
import org.hibernate.ogm.type.BigIntegerType;
import org.hibernate.ogm.type.GridType;
import org.hibernate.ogm.type.Iso8601StringCalendarType;
import org.hibernate.ogm.type.Iso8601StringDateType;
import org.hibernate.type.StandardBasicTypes;
import org.hibernate.type.Type;

/**
 * Container for methods used to obtain the {@link GridType} representation of a {@link Type} for Redis
 *
 * @author Davide D'Alto <davide@hibernate.org>
 */
public class TypeConverter {

	public static final TypeConverter INSTANCE = new TypeConverter();

	private static final Map<Type, GridType> conversionMap = createGridTypeConversionMap();

	private static Map<Type, GridType> createGridTypeConversionMap() {
		Map<Type, GridType> conversion = new HashMap<Type, GridType>();
		conversion.put( StandardBasicTypes.BIG_DECIMAL, BigDecimalType.INSTANCE );
		conversion.put( StandardBasicTypes.BIG_INTEGER, BigIntegerType.INSTANCE );
		conversion.put( StandardBasicTypes.CALENDAR, Iso8601StringCalendarType.DATE_TIME );
		conversion.put( StandardBasicTypes.CALENDAR_DATE, Iso8601StringCalendarType.DATE );
		conversion.put( StandardBasicTypes.DATE, Iso8601StringDateType.DATE );
		conversion.put( StandardBasicTypes.TIME, Iso8601StringDateType.TIME );
		conversion.put( StandardBasicTypes.TIMESTAMP, Iso8601StringDateType.DATE_TIME );
		conversion.put( StandardBasicTypes.BYTE, ByteAsStringType.INSTANCE );
		conversion.put( StandardBasicTypes.LONG, LongAsStringType.INSTANCE );
		conversion.put( StandardBasicTypes.INTEGER, IntegerAsStringType.INSTANCE );
		conversion.put( StandardBasicTypes.DOUBLE, DoubleAsStringType.INSTANCE );
		conversion.put( StandardBasicTypes.BOOLEAN, BooleanAsStringType.INSTANCE );
		conversion.put( StandardBasicTypes.MATERIALIZED_BLOB, PrimitiveByteAsStringType.INSTANCE );
		return conversion;
	}

	/**
	 * Returns the {@link GridType} representing the {@link Type}.
	 *
	 * @param type the Type that needs conversion
	 * @return the corresponding GridType
	 */
	public GridType convert(Type type) {
		return conversionMap.get( type );
	}

}