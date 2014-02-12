package org.hibernate.ogm.dialect.redis;

import org.hibernate.MappingException;
import org.hibernate.engine.spi.Mapping;
import org.hibernate.ogm.type.AbstractGenericBasicType;
import org.hibernate.ogm.type.descriptor.StringMappedGridTypeDescriptor;
import org.hibernate.type.descriptor.java.LongTypeDescriptor;

/**
 * @author Andrea Boriero <dreborier@gmail.com/>
 */
public class RedisLongType extends AbstractGenericBasicType<Long> {

	public static final RedisLongType INSTANCE = new RedisLongType();

	public RedisLongType() {
		super( StringMappedGridTypeDescriptor.INSTANCE, LongTypeDescriptor.INSTANCE );
	}

	@Override
	public String getName() {
		return "redis_long";
	}

	@Override
	public int getColumnSpan(Mapping mapping) throws MappingException {
		return 1;
	}
}