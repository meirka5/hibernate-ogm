/* 
 * JBoss, Home of Professional Open Source
 * Copyright 2012 Red Hat Inc. and/or its affiliates and other contributors
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
import java.util.Iterator;
import java.util.Map;

import org.hibernate.LockMode;
import org.hibernate.dialect.lock.LockingStrategy;
import org.hibernate.id.IntegralDataTypeHolder;
import org.hibernate.loader.custom.CustomQuery;
import org.hibernate.ogm.datastore.impl.EmptyTupleSnapshot;
import org.hibernate.ogm.datastore.impl.MapHelpers;
import org.hibernate.ogm.datastore.map.impl.MapAssociationSnapshot;
import org.hibernate.ogm.datastore.redis.impl.RedisAssociationSnapshot;
import org.hibernate.ogm.datastore.redis.impl.RedisDatastoreProvider;
import org.hibernate.ogm.datastore.redis.impl.RedisTupleSnapshot;
import org.hibernate.ogm.datastore.spi.Association;
import org.hibernate.ogm.datastore.spi.AssociationContext;
import org.hibernate.ogm.datastore.spi.AssociationOperation;
import org.hibernate.ogm.datastore.spi.Tuple;
import org.hibernate.ogm.datastore.spi.TupleContext;
import org.hibernate.ogm.datastore.spi.TupleOperation;
import org.hibernate.ogm.dialect.GridDialect;
import org.hibernate.ogm.dialect.redis.type.RedisBooleanType;
import org.hibernate.ogm.dialect.redis.type.RedisByteType;
import org.hibernate.ogm.dialect.redis.type.RedisDoubleType;
import org.hibernate.ogm.dialect.redis.type.RedisIntegerType;
import org.hibernate.ogm.dialect.redis.type.RedisLongType;
import org.hibernate.ogm.dialect.redis.type.RedisPrimitiveByteType;
import org.hibernate.ogm.grid.AssociationKey;
import org.hibernate.ogm.grid.EntityKey;
import org.hibernate.ogm.grid.EntityKeyMetadata;
import org.hibernate.ogm.grid.RowKey;
import org.hibernate.ogm.massindex.batchindexing.Consumer;
import org.hibernate.ogm.type.BigDecimalType;
import org.hibernate.ogm.type.BigIntegerType;
import org.hibernate.ogm.type.BooleanType;
import org.hibernate.ogm.type.ByteType;
import org.hibernate.ogm.type.GridType;
import org.hibernate.ogm.type.IntegerType;
import org.hibernate.ogm.type.Iso8601StringCalendarType;
import org.hibernate.ogm.type.Iso8601StringDateType;
import org.hibernate.ogm.type.LongType;
import org.hibernate.ogm.type.PrimitiveByteArrayType;
import org.hibernate.ogm.util.impl.Log;
import org.hibernate.ogm.util.impl.LoggerFactory;
import org.hibernate.persister.entity.Lockable;
import org.hibernate.type.StandardBasicTypes;
import org.hibernate.type.Type;
import org.hibernate.type.descriptor.java.BlobTypeDescriptor;

/**
 * @author Seiya Kawashima <skawashima@uchicago.edu>
 */
public class RedisDialect implements GridDialect {

	private static final Log log = LoggerFactory.make();

	private final RedisDatastoreProvider provider;

	public RedisDialect(RedisDatastoreProvider provider) {
		this.provider = provider;
	}

	@Override
	public LockingStrategy getLockingStrategy(Lockable lockable, LockMode lockMode) {
		// TODO Implementing this method needs help from Redis community. Once figuring out how to map lock strategies
		// with Redis, this method will be implemented. Until that time, this method simply throws an exception.
		throw new RuntimeException( "the lock is not supported yet." );
	}

	@Override
	public Tuple getTuple(EntityKey key, TupleContext context) {
		Map<String, Object> entityMap = provider.getEntityTuple( key, context );

		if ( entityMap == null ) {
			return null;
		}

		return new Tuple( new RedisTupleSnapshot( entityMap ) );
	}

	@Override
	public Tuple createTuple(EntityKey key) {
		return new Tuple( new RedisTupleSnapshot() );
	}

	@Override
	public void updateTuple(Tuple tuple, EntityKey key) {
		provider.putEntity( key, tuple );
	}

	@Override
	public void removeTuple(EntityKey key) {
		provider.removeEntity( key );
	}

	@Override
	public Association getAssociation(AssociationKey key, AssociationContext context) {
		Map<RowKey, Map<String, Object>> associationMap = provider.getAssociation( key, context );
		return associationMap == null ? null : new Association( new MapAssociationSnapshot( associationMap ) );
	}

	@Override
	public Association createAssociation(AssociationKey key, AssociationContext context) {
		Map<RowKey, Map<String, Object>> associationMap = new HashMap<RowKey, Map<String, Object>>();
		return new Association( new RedisAssociationSnapshot( associationMap ) );
	}

	@Override
	public void updateAssociation(Association association, AssociationKey key, AssociationContext context) {
		provider.putAssociation( association, key);
	}

	@Override
	public void removeAssociation(AssociationKey key, AssociationContext context) {
		provider.removeAssociation( key );
	}

	@Override
	public Tuple createTupleAssociation(AssociationKey associationKey, RowKey rowKey) {
		return new Tuple( EmptyTupleSnapshot.INSTANCE );
	}

	@Override
	public void nextValue(RowKey key, IntegralDataTypeHolder value, int increment, int initialValue) {
		provider.setNextValue( key, value, increment, initialValue );
	}

	@Override
	public GridType overrideType(Type type) {
		if ( type == StandardBasicTypes.BIG_DECIMAL ) {
			return BigDecimalType.INSTANCE;
		}
		if ( type == StandardBasicTypes.BIG_INTEGER ) {
			return BigIntegerType.INSTANCE;
		}
		// persist calendars as ISO8601 strings, including TZ info
		else if ( type == StandardBasicTypes.CALENDAR ) {
			return Iso8601StringCalendarType.DATE_TIME;
		}
		else if ( type == StandardBasicTypes.CALENDAR_DATE ) {
			return Iso8601StringCalendarType.DATE;
		}
		// persist date as ISO8601 strings, in UTC, without TZ info
		else if ( type == StandardBasicTypes.DATE ) {
			return Iso8601StringDateType.DATE;
		}
		else if ( type == StandardBasicTypes.TIME ) {
			return Iso8601StringDateType.TIME;
		}
		else if ( type == StandardBasicTypes.TIMESTAMP ) {
			return Iso8601StringDateType.DATE_TIME;
		}
		else if ( type == StandardBasicTypes.BYTE ) {
			return RedisByteType.INSTANCE;
		}
		else if ( type == StandardBasicTypes.LONG ) {
			return RedisLongType.INSTANCE;
		}
		else if ( type == StandardBasicTypes.INTEGER ) {
			return RedisIntegerType.INSTANCE;
		}
		else if ( type == StandardBasicTypes.DOUBLE ) {
			return RedisDoubleType.INSTANCE;
		}
		else if ( type == StandardBasicTypes.BOOLEAN ) {
			return RedisBooleanType.INSTANCE;
		}
		else if ( type == StandardBasicTypes.MATERIALIZED_BLOB ) {
			return RedisPrimitiveByteType.INSTANCE;
		}
		return null;
	}

	@Override
	public void forEachTuple(Consumer consumer, EntityKeyMetadata... entityKeyMetadatas) {
	}

	@Override
	public Iterator<Tuple> executeBackendQuery(CustomQuery customQuery, EntityKeyMetadata[] metadatas) {
		return null;
	}

}
