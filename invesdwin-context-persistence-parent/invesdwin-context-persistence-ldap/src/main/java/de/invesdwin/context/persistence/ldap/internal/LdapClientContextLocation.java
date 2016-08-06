package de.invesdwin.context.persistence.ldap.internal;

import java.util.Arrays;
import java.util.List;

import javax.annotation.concurrent.Immutable;
import javax.inject.Named;

import org.springframework.core.io.ClassPathResource;

import de.invesdwin.context.beans.init.locations.IContextLocation;
import de.invesdwin.context.beans.init.locations.PositionedResource;

@Immutable
@Named
public class LdapClientContextLocation implements IContextLocation {

    @Override
    public List<PositionedResource> getContextResources() {
        return Arrays.asList(PositionedResource.of(new ClassPathResource("/META-INF/ctx.ldap.client.xml")));
    }

}
