package de.invesdwin.context.persistence.jpa.api.dao.entity.identity;

import java.util.Set;

import javax.annotation.concurrent.NotThreadSafe;

import de.invesdwin.context.persistence.jpa.api.dao.entity.IEntity;
import de.invesdwin.norva.beanpath.annotation.Hidden;
import de.invesdwin.util.assertions.Assertions;
import de.invesdwin.util.bean.AValueObject;
import de.invesdwin.util.lang.Objects;
import jakarta.persistence.GeneratedValue;
import jakarta.persistence.GenerationType;
import jakarta.persistence.Id;
import jakarta.persistence.MappedSuperclass;
import jakarta.persistence.PrePersist;
import jakarta.persistence.PreUpdate;

/**
 * An Entity is the same as a table row and is persistent as one. These are JPA-Annotated classes that do not contain
 * any logic, but just data and verification.
 * 
 * The lifecycle status fields may be only manipulated by infrastructure classes, that's why the setters for deleted and
 * persistent are package private.
 * 
 * @author subes
 * 
 */
@MappedSuperclass
@NotThreadSafe
public abstract class AUnversionedEntityWithIdentity extends AValueObject implements IEntity {

    private static final long serialVersionUID = 1L;

    @Id
    @GeneratedValue(strategy = GenerationType.IDENTITY)
    private Long id;

    @Override
    @Hidden(skip = true)
    public Long getId() {
        return id;
    }

    @Override
    public void setId(final Long id) {
        Assertions.assertThat(id).as("Id cannot be reset.").isNotNull();
        this.id = id;
    }

    @Override
    public AUnversionedEntityWithIdentity clone() {
        return (AUnversionedEntityWithIdentity) super.clone();
    }

    /**
     * Enity fields are being ignored here, thus no id, created, version, etc is getting copied.
     * 
     * If this is desired, use clone() instead.
     */
    @Override
    protected void innerMergeFrom(final Object o, final boolean overwrite, final boolean clone,
            final Set<String> recursionFilter) {
        final Long prevId = id;
        super.innerMergeFrom(o, overwrite, clone, recursionFilter);
        id = prevId;
    }

    /**
     * If id is set, the identity is specified by it and by the class.
     */
    @Override
    public int hashCode() {
        if (id != null) {
            return this.getClass().hashCode() + id.hashCode();
        } else {
            return super.hashCode();
        }
    }

    /**
     * Matches id match if available, otherwise all attributes.
     */
    @Override
    public boolean equals(final Object obj) {
        if (obj instanceof AUnversionedEntityWithIdentity && id != null) {
            final AUnversionedEntityWithIdentity cObj = (AUnversionedEntityWithIdentity) obj;
            return Objects.equals(id, cObj.id);
        } else {
            return super.equals(obj);
        }
    }

    @PrePersist
    protected void prePersist() {}

    @PreUpdate
    protected void preUpdate() {}
}
