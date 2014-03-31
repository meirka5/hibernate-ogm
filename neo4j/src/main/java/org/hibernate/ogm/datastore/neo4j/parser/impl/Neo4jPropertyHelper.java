/*
 * Hibernate OGM, Domain model persistence for NoSQL datastores
 *
 * License: GNU Lesser General Public License (LGPL), version 2.1 or later
 * See the lgpl.txt file in the root directory or <http://www.gnu.org/licenses/lgpl-2.1.html>.
 */
package org.hibernate.ogm.datastore.neo4j.parser.impl;

import java.util.List;

import org.hibernate.engine.spi.SessionFactoryImplementor;
import org.hibernate.hql.ast.spi.EntityNamesResolver;
import org.hibernate.hql.ast.spi.PropertyHelper;
import org.hibernate.ogm.dialect.GridDialect;
import org.hibernate.ogm.persister.OgmEntityPersister;
import org.hibernate.ogm.type.AbstractGenericBasicType;
import org.hibernate.ogm.type.GridType;
import org.hibernate.type.AbstractStandardBasicType;
import org.hibernate.type.Type;

/**
 * Property helper dealing with MongoDB.
 *
 * @author Gunnar Morling
 */
public class Neo4jPropertyHelper implements PropertyHelper {

	private final SessionFactoryImplementor sessionFactory;
	private final EntityNamesResolver entityNames;
	private final GridDialect dialect;

	public Neo4jPropertyHelper(SessionFactoryImplementor sessionFactory, EntityNamesResolver entityNames) {
		this.sessionFactory = sessionFactory;
		this.entityNames = entityNames;
		this.dialect = sessionFactory.getServiceRegistry().getService( GridDialect.class );
	}

	@Override
	public Object convertToPropertyType(String entityType, List<String> propertyPath, String value) {
		if ( propertyPath.size() > 1 ) {
			throw new UnsupportedOperationException( "Queries on embedded/associated entities are not supported yet." );
		}
		OgmEntityPersister persister = getPersister( entityType );
		Type propertyType = persister.getPropertyType( propertyPath.get( propertyPath.size() - 1 ) );
		if ( propertyType instanceof AbstractStandardBasicType ) {
			return ( (AbstractStandardBasicType<?>) propertyType ).fromString( value );
		}
		else {
			return value;
		}
	}

	public Object convertToPropertyGridType(String entityType, List<String> propertyPath, Object value) {
		if ( propertyPath.size() > 1 ) {
			throw new UnsupportedOperationException( "Queries on embedded/associated entities are not supported yet." );
		}
		OgmEntityPersister persister = getPersister( entityType );
		Type propertyType = persister.getPropertyType( propertyPath.get( propertyPath.size() - 1 ) );
		GridType neo4jType = dialect.overrideType( propertyType );
		if ( neo4jType instanceof AbstractGenericBasicType ) {
			return ( (AbstractGenericBasicType) neo4jType ).toString( value );
		}
		else {
			return value;
		}
	}

	public String getColumnName(String entityType, String propertyName) {
		OgmEntityPersister persister = getPersister( entityType );
		String[] columnNames = persister.getPropertyColumnNames( propertyName );
		return columnNames[0];
	}

	private OgmEntityPersister getPersister(String entityType) {
		Class<?> targetedType = entityNames.getClassFromName( entityType );
		if ( targetedType == null ) {
			throw new IllegalStateException( "Unknown entity name " + entityType );
		}

		return (OgmEntityPersister) sessionFactory.getEntityPersister( targetedType.getName() );
	}

}
