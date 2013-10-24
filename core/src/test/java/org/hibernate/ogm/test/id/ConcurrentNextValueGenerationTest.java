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
package org.hibernate.ogm.test.id;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.junit.Assert.assertThat;

import org.hibernate.id.IdentifierGeneratorHelper;
import org.hibernate.ogm.datastore.impl.DatastoreServices;
import org.hibernate.ogm.dialect.GridDialect;
import org.hibernate.ogm.grid.RowKey;
import org.hibernate.ogm.test.utils.GridDialectType;
import org.hibernate.ogm.test.utils.OgmTestCase;
import org.hibernate.ogm.test.utils.SkipByGridDialect;
import org.hibernate.ogm.test.utils.TestHelper;
import org.hibernate.service.spi.ServiceRegistryImplementor;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

/**
 * @author Davide D'Alto <davide@hibernate.org>
 */
@SkipByGridDialect({
	GridDialectType.HOTROD
})
public class ConcurrentNextValueGenerationTest extends OgmTestCase {

	private static final int LOOPS = 2;
	private static final int THREADS = 10;

	private GridDialect dialect;

	@Before
	public void setUp() {
		ServiceRegistryImplementor registry = sfi().getServiceRegistry();
		dialect = registry.getService( DatastoreServices.class ).getGridDialect();
	}

	@After
	public void clearCaches() {
		TestHelper.dropSchemaAndDatabase( sessions );
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
						System.out.println(value.makeValue());
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
		assertThat( value.makeValue().intValue(), equalTo( ( LOOPS * THREADS ) - 1 ) );
	}

	@Override
	protected Class<?>[] getAnnotatedClasses() {
		return new Class<?>[] { Actor.class };
	}

}
