/*
 * Hibernate OGM, Domain model persistence for NoSQL datastores
 *
 * License: GNU Lesser General Public License (LGPL), version 2.1 or later
 * See the lgpl.txt file in the root directory or <http://www.gnu.org/licenses/lgpl-2.1.html>.
 */
package org.hibernate.ogm.datastore.neo4j.parser.impl;

import java.util.Map;

import org.hibernate.Query;
import org.hibernate.Session;
import org.hibernate.SessionFactory;
import org.hibernate.engine.spi.SessionFactoryImplementor;
import org.hibernate.hql.QueryParser;
import org.hibernate.hql.ast.spi.EntityNamesResolver;
import org.hibernate.ogm.hibernatecore.impl.OgmSession;
import org.hibernate.ogm.service.impl.BaseQueryParserService;
import org.hibernate.ogm.service.impl.SessionFactoryEntityNamesResolver;
import org.hibernate.ogm.util.impl.Log;
import org.hibernate.ogm.util.impl.LoggerFactory;

/**
 * @author Davide D'Alto <davide@hibernate.org>
 */
public class Neo4jBasedQueryParserService extends BaseQueryParserService {

	private static final Log log = LoggerFactory.make();

	private volatile SessionFactoryEntityNamesResolver entityNamesResolver;

	@Override
	public Query getParsedQueryExecutor(OgmSession session, String queryString, Map<String, Object> namedParameters) {
		QueryParser queryParser = new QueryParser();
		Neo4jProcessingChain processingChain = createProcessingChain( session, unwrap( namedParameters ) );
		Neo4jQueryParsingResult result = queryParser.parseQuery( queryString, processingChain );

		log.createdQuery( queryString, result );

		return new Neo4jQuery( result.getEntityType(), result.getQuery().toString(), result.getProjections(), session );
	}

	private Neo4jProcessingChain createProcessingChain(Session session, Map<String, Object> namedParameters) {
		EntityNamesResolver entityNamesResolver = getDefinedEntityNames( session.getSessionFactory() );
		return new Neo4jProcessingChain( (SessionFactoryImplementor) session.getSessionFactory(), entityNamesResolver, namedParameters );
	}

	private EntityNamesResolver getDefinedEntityNames(SessionFactory sessionFactory) {
		if ( entityNamesResolver == null ) {
			entityNamesResolver = new SessionFactoryEntityNamesResolver( sessionFactory );
		}
		return entityNamesResolver;
	}
}
