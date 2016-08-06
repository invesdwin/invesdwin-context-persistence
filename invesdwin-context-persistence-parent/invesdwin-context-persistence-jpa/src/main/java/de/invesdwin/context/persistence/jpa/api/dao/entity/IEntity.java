package de.invesdwin.context.persistence.jpa.api.dao.entity;

public interface IEntity {

    String ID_COLUMN_NAME = "id";

    Long getId();

    void setId(Long id);

}
