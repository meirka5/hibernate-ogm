/*
 * Hibernate OGM, Domain model persistence for NoSQL datastores
 *
 * License: GNU Lesser General Public License (LGPL), version 2.1 or later
 * See the lgpl.txt file in the root directory or <http://www.gnu.org/licenses/lgpl-2.1.html>.
 */
package org.hibernate.ogm.datastore.mongodb.query.parsing.nativequery.impl;

import org.hibernate.ogm.datastore.mongodb.query.impl.MongoDBQueryDescriptor.Operation;
import org.parboiled.BaseParser;
import org.parboiled.Rule;
import org.parboiled.annotations.BuildParseTree;
import org.parboiled.annotations.SuppressNode;
import org.parboiled.annotations.SuppressSubnodes;

import com.mongodb.util.JSON;

/**
 * A parser for MongoDB queries which can be given in one of the following representations:
 * <ul>
 * <li>Criteria-only find query, e.g. <code>{ $and: [ { name : 'Portia' }, { author : 'Oscar Wilde' } ] }</code>. It is
 * left to MongoDB's own {@link JSON} parser to interpret such queries.
 * <li>As "invocation" of the MongoDB shell API (CLI), e.g.
 * <code>db.WILDE_POEM.find({ '$query' : { 'name' : 'Athanasia' }, '$orderby' : { 'name' : 1 } })</code>. Currently the
 * following API methods are supported:
 * <ul>
 * <li>find(criteria)</li>
 * <li>find(criteria, projection)</li>
 * <li>findOne(criteria)</li>
 * <li>findOne(criteria, projection)</li>
 * <li>findAndModify(document)</li>
 * <li>insert(document or array)</li>
 * <li>insert(document or array, options)</li>
 * <li>remove(criteria)</li>
 * <li>remove(criteria, options)</li>
 * <li>update(criteria, update)</li>
 * <li>update(criteria, update, options)</li>
 * <li>count()</li>
 * <li>count(criteria)</li>
 * </ul>
 * The parameter values must be given as JSON objects adhering to the <a
 * href="http://docs.mongodb.org/manual/reference/mongodb-extended-json/">strict mode</a> of MongoDB's JSON handling,
 * with the relaxation that Strings may not only be given in double quotes but also single quotes to facilitate their
 * specification in Java Strings.</li>
 * </ul>
 *
 * @author Gunnar Morling
 * @author Thorsten Möller
 * @author Guillaume Smet
 */
@BuildParseTree
public class NativeQueryParser extends BaseParser<MongoDBQueryDescriptorBuilder> {

	final MongoDBQueryDescriptorBuilder builder;

	public NativeQueryParser() {
		this.builder = new MongoDBQueryDescriptorBuilder();
	}


        
        public Rule Query() {
		return Sequence( FirstOf(AggregateQuery(), ParsedQuery(), CriteriaOnlyFindQuery() ), EOI, push( builder ) );
	}

     //   public Rule Query() {
     //       return Sequence(Db(),Collection(), Operation(), EOI, push( builder ) );
     //   }
	public Rule ParsedQuery() {
            
	     return Sequence( Db(),  Collection(),  Operation());
	}

	/**
	 * A find query only given as criterion. Leave it to MongoDB's own parser to handle it.
	 *
	 * @return the {@link Rule} to identify a find query only
	 */
	public Rule CriteriaOnlyFindQuery() {
		return Sequence( ZeroOrMore( ANY ), builder.setOperation( Operation.FIND ), builder.setCriteria( match() ) );
	}
        public Rule AggregateQuery(){
            return Sequence(Db(),CollectionAggregate(), OperationAggregate(), EOI, push( builder ) );
        }

        public Rule Demo() {
		Rule rule =  Sequence( ZeroOrMore( WhiteSpace() ), "gggOrder ",builder.setCollection( match() ));
                return rule;
	}
        
        public Rule ACollectionDemo() {
		Rule rule =  Sequence( ZeroOrMore( WhiteSpace() ), "Order ", builder.setCollection( match() ),Separator() );
                return rule;
	}
        public Rule Demo2() {
		Rule rule =  Sequence( ZeroOrMore( WhiteSpace() ), "aggregate ",
				"(",ZeroOrMore( WhiteSpace() ),"[", ZeroOrMore( WhiteSpace() ),
				JsonObject(), builder.setCriteria( match() ),
				Optional( Sequence( ", ", JsonObject(), builder.setProjection( match() ) ) ),
                               ZeroOrMore( WhiteSpace() ), "]", ZeroOrMore( WhiteSpace() ),
				") "
                
                
                );
                return rule;
	}
        
        public Rule AggregateReversed() {
		Rule rule =  Sequence( ZeroOrMore( WhiteSpace() ), "aggregate ");
                return rule;
	}
	@SuppressNode
	public Rule Db() {
		Rule rule =  Sequence( ZeroOrMore( WhiteSpace() ), "db", Separator() );
                return rule;
	}

	public Rule CollectionAggregate() {
		Rule rule =  Sequence( OneOrMore( TestNot( ReservedAggregate() ), ANY ), builder.setCollection( match() ) );
                if (rule!=null){
                  //  throw new RuntimeException(builder.build().getCollectionName());
                }
                return rule;
		//TODO OGM-949 it should not be just ANY matcher as they are some restrictions in the Collection naming in Mongo
		// cf. https://docs.mongodb.org/manual/faq/developers/#are-there-any-restrictions-on-the-names-of-collections
	}
	public Rule Collection() {
		Rule rule =  Sequence( OneOrMore( TestNot( Reserved() ), ANY ), builder.setCollection( match() ) );
                if (rule!=null){
                  //  throw new RuntimeException(builder.build().getCollectionName());
                }
                return rule;
		//TODO OGM-949 it should not be just ANY matcher as they are some restrictions in the Collection naming in Mongo
		// cf. https://docs.mongodb.org/manual/faq/developers/#are-there-any-restrictions-on-the-names-of-collections
	}

	@SuppressNode
	public Rule Separator() {
		return Sequence( ZeroOrMore( WhiteSpace() ), ". " );
	}

	public Rule Reserved() {
		return FirstOf( Find(), FindOne(), FindAndModify(), Insert(), Remove(), Update(), Count()
                        ,Db());
		//TODO There are many more query types than what we support.
	}
public Rule ReservedAggregate() {
		return FirstOf( Demo(),AggregateReversed()
                        //, FindOne(), FindAndModify(), Insert(), Remove(), Update(), Count()
                        ,Db());
		//TODO There are many more query types than what we support.
	}
        public Rule OperationAggregate() {
            return FirstOf(Sequence( Demo2(),builder.setOperation( Operation.AGGREGATE ) ),
             //       Sequence( Find(), builder.setOperation( Operation.FIND ) ),
                    Sequence( Demo(), builder.setOperation( Operation.AGGREGATE ) )
                    );
              
        }
    
	public Rule Operation() {
                System.out.println("From operation*****************************************************************************************************************************************************");
		return FirstOf(
                               
				Sequence( Find(), builder.setOperation( Operation.FIND ) ),
				Sequence( FindOne(), builder.setOperation( Operation.FINDONE ) ),
				Sequence( FindAndModify(), builder.setOperation( Operation.FINDANDMODIFY ) ),
				Sequence( Insert(), builder.setOperation( Operation.INSERT ) ),
				Sequence( Remove(), builder.setOperation( Operation.REMOVE ) ),
				Sequence( Update(), builder.setOperation( Operation.UPDATE ) ),
				Sequence( Count(), builder.setOperation( Operation.COUNT ) )
                                
		);
	}
	public Rule Find() {
		return Sequence(
				Separator(),
				"find ",
				"( ",
				JsonObject(), builder.setCriteria( match() ),
				Optional( Sequence( ", ", JsonObject(), builder.setProjection( match() ) ) ),
				") "
		);
	}

	public Rule FindOne() {
		return Sequence(
				Separator(),
				"findOne ",
				"( ",
				Optional( JsonObject(), builder.setCriteria( match() ) ),
				Optional( Sequence( ", ", JsonObject(), builder.setProjection( match() ) ) ),
				") "
		);
	}

	public Rule FindAndModify() {
		return Sequence(
				Separator(),
				"findAndModify ",
				"( ",
				JsonObject(), builder.setCriteria( match() ),
				") "
		);
	}

	public Rule Insert() {
		return Sequence(
				Separator(),
				"insert ",
				"( ",
				JsonComposite(), builder.setUpdateOrInsert( match() ),
				Optional( Sequence( ", ", JsonObject(), builder.setOptions( match() ) ) ),
				") "
		);
	}

	public Rule Remove() {
		return Sequence(
				Separator(),
				"remove ",
				"( ",
				JsonObject(), builder.setCriteria( match() ),
				Optional( Sequence( ", ",
					FirstOf(
						Sequence( BooleanValue(), builder.setOptions( "{ 'justOne': " + match() + " }" ) ),
						Sequence( JsonObject(), builder.setOptions( match() ) )
					)
				) ),
				") "
		);
	}

	public Rule Update() {
		return Sequence(
				Separator(),
				"update ",
				"( ",
				JsonComposite(), builder.setUpdateOrInsert( match() ),
				Optional( Sequence( ", ", JsonObject(), builder.setOptions( match() ) ) ),
				") "
		);
	}
        
        public Rule Aggregate() {
		return Sequence(
				Separator(),
				"aggregate ",
				"( ",
				Sequence( JsonComposite(), ZeroOrMore( Sequence( ", ", Pair() ) ) ) ,
				") "
		);
	}

	public Rule Count() {
		return Sequence(
				Separator(),
				"count ",
				"( ",
				Optional( Sequence( JsonComposite(), builder.setCriteria( match() ) ) ),
				") "
		);
	}

        
        
	public Rule JsonComposite() {
		return FirstOf( JsonObject(), JsonArray() );
	}
        public Rule JsonComposite2() {
		return Sequence( SupportedTestFunction(),ZeroOrMore( WhiteSpace() ));
                
	}
	public Rule JsonObject() {
		return Sequence(
				ZeroOrMore( WhiteSpace() ).skipNode(),
				"{ ",
				FirstOf(
						Sequence( Pair(), ZeroOrMore( Sequence( ", ", Pair() ) ) ),
						Optional( Pair() )
				).suppressNode(),
				"} "
		);
	}

	public Rule Pair() {
		return Sequence( JsonString(), ": ", Value() );
	}

	public Rule Value() {
		return FirstOf( PrimitiveValue(), JsonComposite(), BsonFunctionCall() );
	}

	public Rule PrimitiveValue() {
		return FirstOf( JsonString(), JsonNumber(), "true ", "false ", "null ",
				"Infinity ", "NaN ", "undefined " );
	}

	public Rule BooleanValue() {
		return FirstOf( "true", "false" );
	}

	@SuppressNode
	public Rule JsonNumber() {
		return Sequence( Integer(), Optional( Sequence( Frac(), Optional( Exp() ) ) ), ZeroOrMore( WhiteSpace() ) );
	}

	public Rule JsonArray() {
		return Sequence(
				"[ ",
				FirstOf(
						Sequence( Value(), ZeroOrMore( Sequence( ", ", Value() ) ) ),
						Optional( Value() )
				),
				"] "
		);
	}

	@SuppressSubnodes
	public Rule JsonString() {
		return FirstOf( JsonDoubleQuotedString(), JsonSingleQuotedString() );
	}

	@SuppressSubnodes
	public Rule JsonDoubleQuotedString() {
		return Sequence( "\"", ZeroOrMore( Character() ), "\" " );
	}

	@SuppressSubnodes
	public Rule JsonSingleQuotedString() {
		return Sequence( "'", ZeroOrMore( SingleQuotedStringCharacter() ), "' " );
	}

	@SuppressSubnodes
	public Rule BsonFunctionCall() {
		return Sequence( Optional( "new " ), SupportedBsonFunction(), ZeroOrMore( WhiteSpace() ), "( ",
				FirstOf(
						Sequence( PrimitiveValue(), ZeroOrMore( Sequence( ", ", PrimitiveValue() ) ) ),
						Optional( PrimitiveValue() )
				)
				, ") " );
	}

	public Rule SupportedBsonFunction() {
		return FirstOf( "BinData", "Date", "HexData", "ISODate", "NumberInt", "NumberLong", "ObjectId", "Timestamp", "RegExp", "DBPointer",
				"UUID", "GUID", "CSUUID", "CSGUID", "JUUID", "JGUID", "PYUUID", "PYGUID" );
	}
        public Rule SupportedTestFunction() {
		return FirstOf( "match", "Date" );
	}
	public Rule Character() {
		return FirstOf( EscapedChar(), NormalChar() );
	}

	public Rule SingleQuotedStringCharacter() {
		return FirstOf( SingleQuotedStringEscapedChar(), SingleQuotedStringNormalChar() );
	}

	public Rule EscapedChar() {
		return Sequence( "\\", FirstOf( AnyOf( "\"\\/bfnrt" ), Unicode() ) );
	}

	public Rule SingleQuotedStringEscapedChar() {
		return Sequence( "\\", FirstOf( AnyOf( "'\\/bfnrt" ), Unicode() ) );
	}

	public Rule NormalChar() {
		return Sequence( TestNot( AnyOf( "\"\\" ) ), ANY );
	}

	public Rule SingleQuotedStringNormalChar() {
		return Sequence( TestNot( AnyOf( "'\\" ) ), ANY );
	}

	public Rule Unicode() {
		return Sequence( "u", HexDigit(), HexDigit(), HexDigit(), HexDigit() );
	}

	public Rule Integer() {
		return Sequence( Optional( "-" ), NonZeroDigit(), ZeroOrMore( Digit() ) );
	}

	public Rule Digits() {
		return OneOrMore( Digit() );
	}

	public Rule Digit() {
		return CharRange( '0', '9' );
	}

	public Rule NonZeroDigit() {
		return CharRange( '1', '9' );
	}

	public Rule HexDigit() {
		return FirstOf( CharRange( '0', '9' ), CharRange( 'a', 'f' ), CharRange( 'A', 'F' ) );
	}

	public Rule Frac() {
		return Sequence( ".", Digits() );
	}

	public Rule Exp() {
		return Sequence( IgnoreCase( "e" ), Optional( AnyOf( "+-" ) ), Digits() );
	}

	@SuppressNode
	public Rule WhiteSpace() {
		return OneOrMore( AnyOf( " \n\r\t\f" ) );
	}

	@Override
	@SuppressNode
	protected Rule fromStringLiteral(final String string) {
		if ( string.endsWith( " " ) ) {
			return Sequence( string.trim(), Optional( WhiteSpace() ) );
		}
		else {
			return String( string );
		}
	}
}
