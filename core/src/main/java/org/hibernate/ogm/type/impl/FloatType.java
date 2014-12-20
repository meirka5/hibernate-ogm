package org.hibernate.ogm.type.impl;

import org.hibernate.MappingException;
import org.hibernate.engine.spi.Mapping;
import org.hibernate.ogm.type.descriptor.impl.PassThroughGridTypeDescriptor;
import org.hibernate.type.descriptor.java.FloatTypeDescriptor;

public class FloatType extends AbstractGenericBasicType<Float> {

	public static final FloatType INSTANCE = new FloatType();

	public FloatType() {
		super( PassThroughGridTypeDescriptor.INSTANCE, FloatTypeDescriptor.INSTANCE );
	}

	@Override
	public String[] getRegistrationKeys() {
		return new String[] {getName(), short.class.getName(), Short.class.getName()};
	}


	@Override
	public int getColumnSpan(Mapping mapping) throws MappingException {
		return 1;
	}

	@Override
	public String getName() {
		return "float";
	}
}
