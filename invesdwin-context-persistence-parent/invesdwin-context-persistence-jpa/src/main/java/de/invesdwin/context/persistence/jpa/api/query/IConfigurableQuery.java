package de.invesdwin.context.persistence.jpa.api.query;

public interface IConfigurableQuery {

    void setHint(String name, Object value);

    void setFirstResult(Integer firstResult);

    void setMaxResults(Integer maxResults);

}
