package org.hibernate.ogm.datastore.googledatastore.dialect.impl;

import org.hibernate.ogm.model.spi.TupleSnapshot;

import java.util.Set;

public class GoogleDBTupleSnapshot implements TupleSnapshot {
    @Override
    public Object get(String column) {
        return null;
    }

    @Override
    public boolean isEmpty() {
        return false;
    }

    @Override
    public Set<String> getColumnNames() {
        return null;
    }
}
