package de.invesdwin.context.persistence.jpa.api.query.internal;

import javax.annotation.concurrent.Immutable;

import com.querydsl.jpa.impl.JPAQuery;

import de.invesdwin.context.persistence.jpa.api.query.IConfigurableQuery;

@Immutable
public class ConfigurableQueryDslQuery implements IConfigurableQuery {

    private final JPAQuery query;

    public ConfigurableQueryDslQuery(final JPAQuery query) {
        this.query = query;
    }

    @Override
    public void setHint(final String name, final Object value) {
        query.setHint(name, value);
    }

    @Override
    public void setFirstResult(final Integer firstResult) {
        query.offset(firstResult);
    }

    @Override
    public void setMaxResults(final Integer maxResults) {
        query.limit(maxResults);
    }

}
