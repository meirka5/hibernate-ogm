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
package org.hibernate.ogm.dialect.blueprints.neo4j;

import java.util.Set;

import org.hibernate.ogm.datastore.blueprints.neo4j.impl.ReservedWordWrapper;
import org.hibernate.ogm.datastore.spi.TupleSnapshot;

import com.tinkerpop.blueprints.Element;

/**
 * Represents the Tuple snapshot as loaded by the Neo4j datastore.
 * <p>
 * A {@link org.neo4j.graphdb.Node} represents a {@link org.hibernate.ogm.datastore.spi.Tuple}. Columns are mapped as
 * properties of a the Node.
 *
 * @author Davide D'Alto <davide@hibernate.org>
 */
public final class BlueprintsTupleSnapshot implements TupleSnapshot {

	private final Element node;

	public BlueprintsTupleSnapshot(Element node) {
		this.node = new ReservedWordWrapper( node );
	}

	@Override
	public Object get(String column) {
		Object value = node.getProperty( column );
		if ( value == null ) {
			return null;
		}
		return value;
	}

	@Override
	public boolean isEmpty() {
		return node.getPropertyKeys().isEmpty();
	}

	@Override
	public Set<String> getColumnNames() {
		return node.getPropertyKeys();
	}

}
