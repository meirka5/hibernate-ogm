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
package org.hibernate.ogm.util.impl;

import java.util.regex.Pattern;

import org.hibernate.ogm.grid.AssociationKey;
import org.hibernate.ogm.grid.EntityKey;
import org.hibernate.ogm.grid.RowKey;

/**
 * Generates the ids used to create the {@link org.hibernate.ogm.dialect.couchdb.backend.json.impl.Document}
 *
 * @author Andrea Boriero <dreborier@gmail.com/>
 */
public class Identifier {

	private static final String COLUMN_VALUES_SEPARATOR = "_";
	private static final Pattern escapingPattern = Pattern.compile( COLUMN_VALUES_SEPARATOR );

	/**
	 * Create the id used to store an {@link org.hibernate.ogm.dialect.couchdb.backend.json.impl.EntityDocument}
	 *
	 * @param key the {@link EntityKey} used to generate the id
	 * @return the value of the generate id
	 */
	public static String createEntityId(EntityKey key) {
		return createId( key.getTable(), key.getColumnNames(), key.getColumnValues() );
	}

	/**
	 * Create the id used to store an {@link org.hibernate.ogm.dialect.couchdb.backend.json.impl.AssociationDocument}
	 *
	 * @param key the{@link AssociationKey} used to generate the id
	 * @return the value of the generate id
	 */
	public static String createAssociationId(AssociationKey key) {
		return createId( key.getTable(), key.getColumnNames(), key.getColumnValues() );
	}

	public static String createRowId(RowKey key) {
		return createId( key.getTable(), key.getColumnNames(), key.getColumnValues() );
	}

	private static String createId(String table, String[] columnNames, Object[] columnValues) {
		return createPrefix( table ) + fromValues( columnNames ) + ":" + fromValues( columnValues );
	}

	public static String createPrefix(String table) {
		return table + ":";
	}

	private static String fromValues(Object[] columnValues) {
		String id = "";
		for ( int i = 0; i < columnValues.length; i++ ) {
			id += escapeCharsValuesUsedAsColumnValuesSeparator( columnValues[i] ) + COLUMN_VALUES_SEPARATOR;
		}
		return id;
	}

	private static String escapeCharsValuesUsedAsColumnValuesSeparator(Object columnValue) {
		final String value = String.valueOf( columnValue );
		return escapingPattern.matcher( value ).replaceAll( "/_" );
	}

}
