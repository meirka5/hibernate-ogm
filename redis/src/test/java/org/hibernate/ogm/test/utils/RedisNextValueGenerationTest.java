/*
 * Hibernate, Relational Persistence for Idiomatic Java
 *
 * JBoss, Home of Professional Open Source
 * Copyright 2013-2014 Red Hat Inc. and/or its affiliates and other contributors
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
package org.hibernate.ogm.test.utils;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.junit.Assert.assertThat;

import java.util.Properties;

import org.hibernate.id.IdentifierGeneratorHelper;
import org.hibernate.ogm.datastore.redis.impl.RedisDatastoreProvider;
import org.hibernate.ogm.dialect.GridDialect;
import org.hibernate.ogm.dialect.redis.RedisDialect;
import org.hibernate.ogm.grid.RowKey;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;

/**
 * @author Davide D'Alto <davide@hibernate.org>
 */
public class RedisNextValueGenerationTest {

	private static final int LOOPS = 1;
	private static final int THREADS = 5;

	private GridDialect dialect;
	private RedisDatastoreProvider provider;

	@Before
	public void setUp() {
		provider = new RedisDatastoreProvider();
		provider.configure( new Properties() );
		provider.start();
		dialect = new RedisDialect( provider );
	}

	@After
	public void tearDown() {
		JedisPool pool = provider.getPool();
		Jedis jedis = pool.getResource();
		try {
			jedis.flushDB();
		}
		finally {
			pool.returnResource( jedis );
			provider.stop();
		}
	}

	@Test
	public void testFirstValueIsInitialValue() {
		final int initialValue = 5;
		final RowKey sequenceNode = new RowKey( "initialSequence", new String[0], new Object[0] );
		final IdentifierGeneratorHelper.BigIntegerHolder sequenceValue = new IdentifierGeneratorHelper.BigIntegerHolder();
		dialect.nextValue( sequenceNode, sequenceValue, 1, initialValue );
		assertThat( sequenceValue.makeValue().intValue(), equalTo( initialValue ) );
	}

	@Test
	public void testTwoIncrements() {
		final int initialValue = 5;
		final RowKey sequenceNode = new RowKey( "initialSequence", new String[0], new Object[0] );
		final IdentifierGeneratorHelper.BigIntegerHolder sequenceValue = new IdentifierGeneratorHelper.BigIntegerHolder();
		dialect.nextValue( sequenceNode, sequenceValue, 1, initialValue );
		dialect.nextValue( sequenceNode, sequenceValue, 1, initialValue );
		assertThat( sequenceValue.makeValue().intValue(), equalTo( initialValue + 1) );
	}

	@Test
	public void testThreadSafty() throws InterruptedException {
		final RowKey test = new RowKey( "test", new String[0], new Object[0] );
		Thread[] threads = new Thread[THREADS];
		for ( int i = 0; i < threads.length; i++ ) {
			threads[i] = new Thread( new Runnable() {
				@Override
				public void run() {
					final IdentifierGeneratorHelper.BigIntegerHolder value = new IdentifierGeneratorHelper.BigIntegerHolder();
					for ( int i = 0; i < LOOPS; i++ ) {
						dialect.nextValue( test, value, 1, 0 );
					}
				}
			} );
			threads[i].start();
		}
		for ( Thread thread : threads ) {
			thread.join();
		}
		final IdentifierGeneratorHelper.BigIntegerHolder value = new IdentifierGeneratorHelper.BigIntegerHolder();
		dialect.nextValue( test, value, 0, 1 );
		assertThat( value.makeValue().intValue(), equalTo( LOOPS * THREADS ) );
	}
}
