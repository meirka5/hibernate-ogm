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
package org.hibernate.ogm.dialect.neo4j;

import java.util.Set;

import org.hibernate.LockMode;
import org.hibernate.dialect.lock.LockingStrategy;
import org.hibernate.id.IntegralDataTypeHolder;
import org.hibernate.ogm.datastore.impl.EmptyAssociationSnapshot;
import org.hibernate.ogm.datastore.impl.EmptyTupleSnapshot;
import org.hibernate.ogm.datastore.neo4j.impl.Neo4jDatastoreProvider;
import org.hibernate.ogm.datastore.neo4j.impl.Neo4jTypeConverter;
import org.hibernate.ogm.datastore.neo4j.impl.PropertyNameWrapper;
import org.hibernate.ogm.datastore.spi.Association;
import org.hibernate.ogm.datastore.spi.AssociationContext;
import org.hibernate.ogm.datastore.spi.AssociationOperation;
import org.hibernate.ogm.datastore.spi.Tuple;
import org.hibernate.ogm.datastore.spi.TupleContext;
import org.hibernate.ogm.datastore.spi.TupleOperation;
import org.hibernate.ogm.dialect.GridDialect;
import org.hibernate.ogm.grid.AssociationKey;
import org.hibernate.ogm.grid.EntityKey;
import org.hibernate.ogm.grid.EntityKeyMetadata;
import org.hibernate.ogm.grid.RowKey;
import org.hibernate.ogm.massindex.batchindexing.Consumer;
import org.hibernate.ogm.type.GridType;
import org.hibernate.persister.entity.Lockable;
import org.hibernate.type.Type;

import com.tinkerpop.blueprints.CloseableIterable;
import com.tinkerpop.blueprints.Edge;
import com.tinkerpop.blueprints.Element;
import com.tinkerpop.blueprints.Vertex;
import com.tinkerpop.blueprints.util.wrappers.wrapped.WrappedElement;

/**
 * Abstracts Hibernate OGM from Neo4j.
 * <p>
 * A {@link Tuple} is saved as a {@link Vertex} where the columns are converted into properties of the node.<br>
 * An {@link Association} is converted into an {@link Edge} identified by the {@link AssociationKey} and the
 * {@link RowKey}.
 *
 * @author Davide D'Alto <davide@hibernate.org>
 */
public class Neo4jDialect implements GridDialect {

	/**
	 * Contains the name of the property with the table name.
	 */
	public static final String TABLE_PROPERTY = "_table";

	private final Neo4jDatastoreProvider provider;

	private final Neo4jIndexManager indexer;

	public Neo4jDialect(Neo4jDatastoreProvider provider) {
		this.provider = provider;
		this.indexer = new Neo4jIndexManager( provider );
	}

	@Override
	public LockingStrategy getLockingStrategy(Lockable lockable, LockMode lockMode) {
		throw new UnsupportedOperationException( "LockMode " + lockMode + " is not supported by the Neo4j GridDialect" );
	}

	@Override
	public Tuple getTuple(EntityKey key, TupleContext context) {
		Vertex entityNode = findNode( key );
		if ( entityNode == null ) {
			return null;
		}
		return createTuple( entityNode );
	}

	private Tuple createTuple(Vertex entityNode) {
		return new Tuple( new Neo4jTupleSnapshot( entityNode ) );
	}

	@Override
	public Tuple createTuple(EntityKey key) {
		return new Tuple( EmptyTupleSnapshot.SINGLETON );
	}

	@Override
	public void updateTuple(Tuple tuple, EntityKey key) {
		Vertex node = createNodeUnlessExists( key );
		applyTupleOperations( new PropertyNameWrapper( node ), tuple.getOperations() );
	}

	@Override
	public void removeTuple(EntityKey key) {
		Vertex entityNode = findNode( key );
		if ( entityNode != null ) {
			removeRelationships( entityNode );
			removeNode( entityNode );
		}
	}

	@Override
	public Tuple createTupleAssociation(AssociationKey associationKey, RowKey rowKey) {
		return new Tuple( EmptyTupleSnapshot.SINGLETON );
	}

	@Override
	public Association getAssociation(AssociationKey associationKey, AssociationContext associationContext) {
		Vertex entityNode = findNode( associationKey.getEntityKey() );
		if ( entityNode == null ) {
			return null;
		}
		return new Association( new Neo4jAssociationSnapshot( entityNode, label( associationKey ), associationKey ) );
	}

	@Override
	public Association createAssociation(AssociationKey associationKey) {
		return new Association( EmptyAssociationSnapshot.SINGLETON );
	}

	@Override
	public void updateAssociation(Association association, AssociationKey key) {
		for ( AssociationOperation action : association.getOperations() ) {
			applyAssociationOperation( key, action );
		}
	}

	@Override
	public void nextValue(RowKey key, IntegralDataTypeHolder value, int increment, int initialValue) {
		int nextValue = provider.nextValue( key, increment, initialValue );
		value.initialize( nextValue );
	}

	@Override
	public GridType overrideType(Type type) {
		return Neo4jTypeConverter.INSTANCE.convert( type );
	}

	@Override
	public void removeAssociation(AssociationKey key) {
		if ( key != null ) {
			Vertex node = findNode( key.getEntityKey() );
			Iterable<Edge> relationships = node.getEdges( com.tinkerpop.blueprints.Direction.OUT, label( key ) );
			for ( Edge rel : relationships ) {
				removeRelationship( rel );
			}
		}
	}

	private void applyAssociationOperation(AssociationKey key, AssociationOperation operation) {
		switch ( operation.getType() ) {
		case CLEAR:
			removeAssociation( key );
			break;
		case PUT:
			putAssociationOperation( key, operation );
			break;
		case PUT_NULL:
			removeAssociationOperation( key, operation );
			break;
		case REMOVE:
			removeAssociationOperation( key, operation );
			break;
		}
	}

	private void putAssociationOperation(AssociationKey associationKey, AssociationOperation action) {
		RowKey rowKey = action.getKey();
		Edge relationship = createRelationshipUnlessExists( findNode( associationKey.getEntityKey() ), associationKey, rowKey );
		Vertex vertex = relationship.getVertex( com.tinkerpop.blueprints.Direction.IN );
		applyTupleOperations( new PropertyNameWrapper( vertex ), action.getValue().getOperations() );
	}

	private Edge createRelationshipUnlessExists(Vertex startNode, AssociationKey associationKey, RowKey rowKey) {
		Edge relationship = indexer.findRelationship( label( associationKey ), rowKey );
		if ( relationship == null ) {
			return createRelationship( startNode, associationKey, rowKey );
		}
		return relationship;
	}

	private Vertex findNode(EntityKey entityKey) {
		return indexer.findNode( entityKey );
	}

	private void removeAssociationOperation(AssociationKey associationKey, AssociationOperation action) {
		RowKey rowKey = action.getKey();
		Edge relationship = indexer.findRelationship( label( associationKey ), rowKey );
		removeRelationship( relationship );
	}

	private void removeRelationship(Edge relationship) {
		if ( relationship != null ) {
			indexer.remove( relationship );
			relationship.remove();
		}
	}

	private void applyTupleOperations(WrappedElement node, Set<TupleOperation> operations) {
		for ( TupleOperation operation : operations ) {
			applyOperation( node, operation );
		}
	}

	private void applyOperation(Element node, TupleOperation operation) {
		switch ( operation.getType() ) {
		case PUT:
			putTupleOperation( node, operation );
			break;
		case PUT_NULL:
			removeTupleOperation( node, operation );
			break;
		case REMOVE:
			removeTupleOperation( node, operation );
			break;
		}
	}

	private void removeTupleOperation(Element node, TupleOperation operation) {
		if ( hasProperty( node, operation ) ) {
			node.removeProperty( operation.getColumn() );
		}
	}

	private boolean hasProperty(Element node, TupleOperation operation) {
		return node.getProperty( operation.getColumn() ) != null;
	}

	private void putTupleOperation(Element node, TupleOperation operation) {
		node.setProperty( operation.getColumn(), operation.getValue() );
	}

	private Vertex createNodeUnlessExists(EntityKey key) {
		Vertex node = findNode( key );
		if ( node == null ) {
			node = createNode( key );
		}
		return node;
	}

	private Vertex createNode(EntityKey key) {
		Vertex node = provider.createNode();
		node.setProperty( TABLE_PROPERTY, key.getTable() );
		applyProperties( new PropertyNameWrapper( node ), key.getColumnNames(), key.getColumnValues() );
		indexer.index( node, key );
		return node;
	}

	private Edge createRelationship(Vertex startNode, AssociationKey associationKey, RowKey rowKey) {
		Edge relationship = startNode.addEdge( label( associationKey ), provider.createNode() );
		applyProperties( new PropertyNameWrapper( relationship ), rowKey.getColumnNames(), rowKey.getColumnValues() );
		indexer.index( relationship, rowKey );
		return relationship;
	}

	private void applyProperties( WrappedElement node, String[] names, Object[] values) {
		for ( int i = 0; i < names.length; i++ ) {
			node.setProperty( names[i], values[i] );
		}
	}

	private void removeNode(Vertex entityNode) {
		removeRelationships( entityNode );
		indexer.remove( entityNode );
		entityNode.remove();
	}

	private String label(AssociationKey associationKey) {
		StringBuilder builder = new StringBuilder(associationKey.getEntityKey().getTable());
		builder.append( ":" );
		builder.append( associationKey.getCollectionRole() );
		return builder.toString();
	}

	private void removeRelationships(Vertex node) {
		if ( node != null ) {
			for ( Edge rel : node.getEdges( com.tinkerpop.blueprints.Direction.BOTH) ) {
				removeRelationship( rel );
			}
		}
	}

	@Override
	public void forEachTuple(Consumer consumer, EntityKeyMetadata... entityKeyMetadatas) {
		for ( EntityKeyMetadata entityKeyMetadata : entityKeyMetadatas ) {
			CloseableIterable<Vertex> queryNodes = indexer.findNodes( entityKeyMetadata.getTable() );
			try {
				for ( Vertex node : queryNodes ) {
					Tuple tuple = createTuple( node );
					consumer.consume( tuple );
				}
			}
			finally {
				queryNodes.close();
			}
		}
	}

}
