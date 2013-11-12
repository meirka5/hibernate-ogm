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
package org.hibernate.ogm.datastore.blueprints.neo4j.impl;

import java.util.HashSet;
import java.util.Set;

import com.tinkerpop.blueprints.Element;
import com.tinkerpop.blueprints.util.StringFactory;
import com.tinkerpop.blueprints.util.wrappers.wrapped.WrappedElement;

/**
 * @author Davide D'Alto <davide@hibernate.org>
 */
public class ReservedWordWrapper extends WrappedElement {

	public static final String PREFIX = "ogm_";

	public static final String ESCAPED_ID = PREFIX + StringFactory.ID;

	public ReservedWordWrapper(Element baseElement) {
		super( baseElement );
	}

	@Override
	public void setProperty(String key, Object value) {
		super.setProperty( escape( key ), value );
	}

	@Override
	public <T> T getProperty(String key) {
		return super.getProperty( escape( key ) );
	}

	@Override
	public <T> T removeProperty(String key) {
		return super.removeProperty( escape( key ) );
	}

	@Override
	public Set<String> getPropertyKeys() {
		Set<String> propertyKeys = super.getPropertyKeys();
		Set<String> unescaped = new HashSet<String>();
		for ( String name : propertyKeys ) {
			unescaped.add( unescape( name ) );
		}
		return unescaped;
	}

	public static String escape(String column) {
		if ( StringFactory.ID.equals( column ) ) {
			return ESCAPED_ID;
		}
		return column;
	}

	private static String unescape(String column) {
		if ( column.equals( ESCAPED_ID ) ) {
			return StringFactory.ID;
		}
		return column;
	}

}