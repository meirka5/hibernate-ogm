package org.hibernate.ogm.dialect.redis.type;

import org.hibernate.MappingException;
import org.hibernate.engine.spi.Mapping;
import org.hibernate.ogm.type.AbstractGenericBasicType;
import org.hibernate.ogm.type.descriptor.StringMappedGridTypeDescriptor;
import org.hibernate.type.descriptor.java.IntegerTypeDescriptor;

/**
 * @author Andrea Boriero <dreborier@gmail.com/>
 */
public class RedisIntegerType extends AbstractGenericBasicType<Integer> {

	public static final RedisIntegerType INSTANCE = new RedisIntegerType();

	public RedisIntegerType() {
		super( StringMappedGridTypeDescriptor.INSTANCE, IntegerTypeDescriptor.INSTANCE );
	}

	@Override
	public String getName() {
		return "redis_integer";
	}

	@Override
	public int getColumnSpan(Mapping mapping) throws MappingException {
		return 1;
	}
}