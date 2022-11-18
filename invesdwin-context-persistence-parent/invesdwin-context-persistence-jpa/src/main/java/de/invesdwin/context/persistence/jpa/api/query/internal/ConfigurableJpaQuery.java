package de.invesdwin.context.persistence.jpa.api.query.internal;

import javax.annotation.concurrent.Immutable;

import de.invesdwin.context.persistence.jpa.api.query.IConfigurableQuery;
import jakarta.persistence.Query;

@Immutable
public class ConfigurableJpaQuery implements IConfigurableQuery {

    private final Query query;

    public ConfigurableJpaQuery(final Query query) {
        this.query = query;
    }

    @Override
    public void setHint(final String name, final Object value) {
        query.setHint(name, value);
    }

    @Override
    public void setFirstResult(final Integer firstResult) {
        query.setFirstResult(firstResult);
    }

    @Override
    public void setMaxResults(final Integer maxResult) {
        query.setMaxResults(maxResult);
    }

}
