package de.invesdwin.context.persistence.jpa.api.dao.entity.identity;

import java.util.Date;
import java.util.Set;

import javax.annotation.concurrent.NotThreadSafe;
import javax.persistence.Column;
import javax.persistence.MappedSuperclass;
import javax.persistence.Version;

import de.invesdwin.norva.beanpath.annotation.Hidden;
import de.invesdwin.util.time.fdate.FDate;

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
public abstract class AEntityWithIdentity extends AUnversionedEntityWithIdentity {

    private static final long serialVersionUID = 1L;

    @Column(nullable = false)
    private Date created;
    @Column(nullable = false)
    private Date updated;
    @Version
    @Column(nullable = false)
    private Long version;

    /**
     * This is the version number for Optimistic Locking. It may be null when the Entity has not been written to the DB.
     */
    @Hidden(skip = true)
    public Long getVersion() {
        return version;
    }

    /**
     * Returns the time when the Entity was created in the DB.
     */
    @Hidden(skip = true)
    public FDate getCreated() {
        return FDate.valueOf(created);
    }

    /**
     * Returns the time when the Entity was last updated in the DB.
     */
    @Hidden(skip = true)
    public FDate getUpdated() {
        return FDate.valueOf(updated);
    }

    @Override
    public AEntityWithIdentity clone() {
        return (AEntityWithIdentity) super.clone();
    }

    /**
     * Enity fields are being ignored here, thus no id, created, version, etc is getting copied.
     * 
     * If this is desired, use clone() instead.
     */
    @Override
    protected void innerMergeFrom(final Object o, final boolean overwrite, final boolean clone,
            final Set<String> recursionFilter) {
        final Date prevCreated = created;
        final Date prevUpdated = updated;
        final Long prevVersion = version;
        super.innerMergeFrom(o, overwrite, clone, recursionFilter);
        created = prevCreated;
        updated = prevUpdated;
        version = prevVersion;
    }

    /**************************** Entity Lifecycle Hooks **********************/

    @Override
    protected void prePersist() {
        super.prePersist();
        //CHECKSTYLE:OFF for performance not using FDate here
        final Date date = new Date();
        //CHECKSTYLE:ON
        this.created = date;
        this.updated = date;
    }

    @Override
    protected void preUpdate() {
        super.prePersist();
        //CHECKSTYLE:OFF for performance not using FDate here
        this.updated = new Date();
        //CHECKSTYLE:ON
    }

}
