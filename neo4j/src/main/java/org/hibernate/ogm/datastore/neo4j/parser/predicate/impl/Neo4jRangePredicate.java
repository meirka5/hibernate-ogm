/*
 * Hibernate OGM, Domain model persistence for NoSQL datastores
 *
 * License: GNU Lesser General Public License (LGPL), version 2.1 or later
 * See the lgpl.txt file in the root directory or <http://www.gnu.org/licenses/lgpl-2.1.html>.
 */
package org.hibernate.ogm.datastore.neo4j.parser.predicate.impl;

import static org.neo4j.cypherdsl.CypherQuery.identifier;
import static org.neo4j.cypherdsl.CypherQuery.literal;

import org.hibernate.hql.ast.spi.predicate.NegatablePredicate;
import org.hibernate.hql.ast.spi.predicate.RangePredicate;
import org.neo4j.cypherdsl.expression.BooleanExpression;
import org.neo4j.cypherdsl.query.Operator;
import org.neo4j.cypherdsl.query.Value;

/**
 * @author Davide D'Alto <davide@hibernate.org>
 */
public class Neo4jRangePredicate extends RangePredicate<BooleanExpression> implements NegatablePredicate<BooleanExpression> {

	private final String alias;

	public Neo4jRangePredicate(String alias, String propertyName, Object lower, Object upper) {
		super( propertyName, lower, upper );
		this.alias = alias;
	}

	@Override
	public BooleanExpression getQuery() {
		BooleanExpression gteThanLower = comparator( ">=", lower );
		BooleanExpression lteThanUpper = comparator( "<=", upper );
		return gteThanLower.and( lteThanUpper );
	}

	@Override
	public BooleanExpression getNegatedQuery() {
		BooleanExpression ltThanLower = comparator( "<", lower );
		BooleanExpression gtThanUpper = comparator( ">", upper );
		return ltThanLower.or( gtThanUpper );
	}

	private BooleanExpression comparator(String operator, Object value) {
		org.neo4j.cypherdsl.query.Query.checkNull( value, "Value" );
		return new Value( new Operator( identifier( alias ).property( propertyName ), operator ), literal( value ) );
	}

}
