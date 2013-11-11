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

import java.util.Iterator;
import java.util.Set;

import org.hibernate.LockMode;
import org.hibernate.dialect.lock.LockingStrategy;
import org.hibernate.id.IntegralDataTypeHolder;
import org.hibernate.loader.custom.CustomQuery;
import org.hibernate.ogm.datastore.blueprints.neo4j.impl.BlueprintsTypeConverter;
import org.hibernate.ogm.datastore.blueprints.neo4j.impl.Neo4jBlueprintsDatastoreProvider;
import org.hibernate.ogm.datastore.blueprints.neo4j.impl.ReservedWordWrapper;
import org.hibernate.ogm.datastore.impl.EmptyAssociationSnapshot;
import org.hibernate.ogm.datastore.impl.EmptyTupleSnapshot;
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
import org.hibernate.ogm.util.impl.Log;
import org.hibernate.ogm.util.impl.LoggerFactory;
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
 * A {@link Tuple} is saved as a {@link Vertex} where the columns are converted into properties of the vertex.<br>
 * An {@link Association} is converted into an {@link Edge} identified by the {@link AssociationKey} and the
 * {@link RowKey}.
 *
 * @author Davide D'Alto <davide@hibernate.org>
 */
public class BlueprintsDialect implements GridDialect {
	
	private static final Log log =  LoggerFactory.make();
	/**
	 * Contains the name of the property with the table name.
	 */
	public static final String TABLE_PROPERTY = "_table";

	private final Neo4jBlueprintsDatastoreProvider provider;

	private final BlueprintsIndexManager indexer;

	public BlueprintsDialect(Neo4jBlueprintsDatastoreProvider provider) {
		this.provider = provider;
		this.indexer = new BlueprintsIndexManager( provider );
	}

	@Override
	public LockingStrategy getLockingStrategy(Lockable lockable, LockMode lockMode) {
		throw new UnsupportedOperationException( "LockMode " + lockMode + " is not supported by the Neo4j GridDialect" );
	}

	@Override
	public Tuple getTuple(EntityKey key, TupleContext context) {
		Vertex entityVertex = findVertex( key );
		if ( entityVertex == null ) {
			return null;
		}
		return createTuple( entityVertex );
	}

	private Tuple createTuple(Vertex entityVertex) {
		return new Tuple( new BlueprintsTupleSnapshot( entityVertex ) );
	}

	@Override
	public Tuple createTuple(EntityKey key) {
		return new Tuple( EmptyTupleSnapshot.SINGLETON );
	}

	@Override
	public void updateTuple(Tuple tuple, EntityKey key) {
		Vertex vertex = createVertexUnlessExists( key );
		applyTupleOperations( new ReservedWordWrapper( vertex ), tuple.getOperations() );
	}

	@Override
	public void removeTuple(EntityKey key) {
		Vertex entityVertex = findVertex( key );
		if ( entityVertex != null ) {
			removeRelationships( entityVertex );
			removeVertex( key, entityVertex );
		}
	}

	@Override
	public Tuple createTupleAssociation(AssociationKey associationKey, RowKey rowKey) {
		return new Tuple( EmptyTupleSnapshot.SINGLETON );
	}

	@Override
	public Association getAssociation(AssociationKey associationKey, AssociationContext associationContext) {
		Vertex entityVertex = findVertex( associationKey.getEntityKey() );
		if ( entityVertex == null ) {
			return null;
		}
		return new Association( new BlueprintsAssociationSnapshot( entityVertex, label( associationKey ), associationKey ) );
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
		return BlueprintsTypeConverter.INSTANCE.convert( type );
	}

	@Override
	public void removeAssociation(AssociationKey key) {
		if ( key != null ) {
			Vertex vertex = findVertex( key.getEntityKey() );
			Iterable<Edge> edges = vertex.getEdges( com.tinkerpop.blueprints.Direction.OUT, label( key ) );
			for ( Edge rel : edges ) {
				removeRelationship( rel );
			}
		}
	}

	@Override
	public Iterator<Tuple> executeBackendQuery(CustomQuery customQuery, EntityKeyMetadata[] metadatas) {
		throw new UnsupportedOperationException( "Native queries not suported for Neo4j" );
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
		Edge edge = createRelationshipUnlessExists( findVertex( associationKey.getEntityKey() ), associationKey, rowKey );
		Vertex vertex = edge.getVertex( com.tinkerpop.blueprints.Direction.IN );
		applyTupleOperations( vertex, action.getValue().getOperations() );
	}

	private Edge createRelationshipUnlessExists(Vertex startVertex, AssociationKey associationKey, RowKey rowKey) {
		Edge edge = indexer.findRelationship( rowKey, label( associationKey ) );
		if ( edge == null ) {
			return createRelationship( startVertex, associationKey, rowKey );
		}
		return edge;
	}

	private Vertex findVertex(EntityKey entityKey) {
		return indexer.findVertex( entityKey );
	}

	private void removeAssociationOperation(AssociationKey associationKey, AssociationOperation action) {
		RowKey rowKey = action.getKey();
		Edge edge = indexer.findRelationship( rowKey, label( associationKey ) );
		removeRelationship( edge );
	}

	private void removeRelationship(Edge edge) {
		if ( edge != null ) {
			indexer.remove( edge );
			edge.remove();
		}
	}

	private void applyTupleOperations(Element vertex, Set<TupleOperation> operations) {
		for ( TupleOperation operation : operations ) {
			applyOperation( vertex, operation );
		}
	}

	private void applyOperation(Element element, TupleOperation operation) {
		WrappedElement wrapped = new ReservedWordWrapper( element );
		switch ( operation.getType() ) {
			case PUT:
				putTupleOperation( wrapped, operation );
				break;
			case PUT_NULL:
			case REMOVE:
				removeTupleOperation( wrapped, operation );
				break;
		}
	}

	private void removeTupleOperation(Element vertex, TupleOperation operation) {
		if ( hasProperty( vertex, operation ) ) {
			vertex.removeProperty( operation.getColumn() );
		}
	}

	private boolean hasProperty(Element vertex, TupleOperation operation) {
		return vertex.getProperty( operation.getColumn() ) != null;
	}

	private void putTupleOperation(Element vertex, TupleOperation operation) {
		vertex.setProperty( operation.getColumn(), operation.getValue() );
	}

	private Vertex createVertexUnlessExists(EntityKey key) {
		Vertex vertex = findVertex( key );
		if ( vertex == null ) {
			vertex = createVertex( key );
		}
		return vertex;
	}

	private Vertex createVertex(EntityKey key) {
 		Vertex vertex = provider.createVertex();
		vertex.setProperty( TABLE_PROPERTY, key.getTable() );
		applyProperties( vertex, key.getColumnNames(), key.getColumnValues() );
		indexer.index( key, vertex );
		return vertex;
	}

	private Edge createRelationship(Vertex startVertex, AssociationKey associationKey, RowKey rowKey) {
		Edge edge = startVertex.addEdge( label( associationKey ), provider.createVertex() );
		applyProperties( edge, rowKey.getColumnNames(), rowKey.getColumnValues() );
		indexer.index( edge );
		return edge;
	}

	private void applyProperties(Element element, String[] names, Object[] values) {
		WrappedElement wrapped = new ReservedWordWrapper( element );
		for ( int i = 0; i < names.length; i++ ) {
			wrapped.setProperty( names[i], values[i] );
		}
	}

	private void removeVertex(EntityKey key, Vertex entityVertex) {
		removeRelationships( entityVertex );
		indexer.remove( key, entityVertex );
		entityVertex.remove();
	}

	private String label(AssociationKey associationKey) {
		StringBuilder builder = new StringBuilder(associationKey.getEntityKey().getTable());
		builder.append( ":" );
		builder.append( associationKey.getCollectionRole() );
		return builder.toString();
	}

	private void removeRelationships(Vertex vertex) {
		if ( vertex != null ) {
			Iterable<Edge> edges = vertex.getEdges( com.tinkerpop.blueprints.Direction.BOTH);
			for ( Edge rel : edges ) {
				removeRelationship( rel );
			}
		}
	}

	@Override
	public void forEachTuple(Consumer consumer, EntityKeyMetadata... entityKeyMetadatas) {
		for ( EntityKeyMetadata entityKeyMetadata : entityKeyMetadatas ) {
			CloseableIterable<Vertex> queryVertexes = indexer.findVertexes( entityKeyMetadata.getTable() );
			try {
				for ( Vertex vertex : queryVertexes ) {
					Tuple tuple = createTuple( vertex );
					consumer.consume( tuple );
				}
			}
			finally {
				queryVertexes.close();
			}
		}
	}

}
