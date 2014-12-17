package org.hibernate.ogm.datastore.googledatastore.options.navigation.impl;

import org.hibernate.ogm.datastore.googledatastore.options.navigation.GoogleEntityContext;
import org.hibernate.ogm.datastore.googledatastore.options.navigation.GooglePropertyContext;

import java.lang.annotation.ElementType;

/**
 * Created by ABhat on 14 Dec 2014.
 */
public class GoogleEntityContextImpl implements GoogleEntityContext{
    @Override
    public GoogleEntityContext entity(Class<?> type) {
        return null;
    }

    @Override
    public GooglePropertyContext property(String propertyName, ElementType target) {
        return null;
    }
}
