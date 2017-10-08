package de.invesdwin.context.persistence.jpa.hibernate.internal;

import java.util.Properties;

import javax.annotation.concurrent.NotThreadSafe;

import org.hibernate.id.enhanced.SequenceStyleGenerator;
import org.hibernate.service.ServiceRegistry;
import org.hibernate.type.Type;
import org.junit.jupiter.api.Test;

import de.invesdwin.context.test.ATest;
import de.invesdwin.util.assertions.Assertions;

@NotThreadSafe
public class OneSequencePerEntityAspectTest extends ATest {

    @Test
    public void testAspectWorking() {
        final Type type = null;
        final Properties params = new Properties();
        final ServiceRegistry serviceRegistry = null;
        try {
            new SequenceStyleGenerator().configure(type, params, serviceRegistry);
            Assertions.fail("exception expected");
        } catch (final NullPointerException e) { //SUPPRESS CHECKSTYLE single line
            //ignore
        }

        Assertions.assertThat(params.get(SequenceStyleGenerator.CONFIG_PREFER_SEQUENCE_PER_ENTITY)).isEqualTo(true);
    }

}
