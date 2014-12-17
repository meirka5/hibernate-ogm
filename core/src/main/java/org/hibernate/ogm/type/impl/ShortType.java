package org.hibernate.ogm.type.impl;

import org.hibernate.MappingException;
import org.hibernate.engine.spi.Mapping;
import org.hibernate.ogm.type.descriptor.impl.PassThroughGridTypeDescriptor;
import org.hibernate.type.descriptor.java.ShortTypeDescriptor;

public class ShortType extends AbstractGenericBasicType<Short> {
    public static final ShortType INSTANCE = new ShortType();

    public ShortType() {
        super(PassThroughGridTypeDescriptor.INSTANCE, ShortTypeDescriptor.INSTANCE);
    }

    @Override
    public int getColumnSpan(Mapping mapping) throws MappingException {
        return 1;
    }

    @Override
    public String[] getRegistrationKeys() {
        return new String[]{getName(), short.class.getName(), Short.class.getName()};
    }

    @Override
    public String getName() {
        return "short";
    }
}
