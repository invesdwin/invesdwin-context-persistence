package de.invesdwin.context.persistence.jpa.simple;

import javax.annotation.concurrent.NotThreadSafe;
import javax.persistence.Entity;
import javax.validation.constraints.NotNull;

import de.invesdwin.context.persistence.jpa.api.dao.entity.AUnversionedEntity;
import de.invesdwin.context.persistence.jpa.api.index.Index;
import de.invesdwin.context.persistence.jpa.api.index.Indexes;

@NotThreadSafe
@Entity
@Indexes({ @Index(columnNames = { "name" }) })
public class SimpleTestEntity extends AUnversionedEntity {

    private static final long serialVersionUID = 1L;

    @NotNull
    private String name;

    public String getName() {
        return name;
    }

    public void setName(final String name) {
        this.name = name;
    }

}
