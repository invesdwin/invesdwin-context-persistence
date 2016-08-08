package de.invesdwin.context.persistence.ldap.directory.server.internal;

import java.lang.annotation.Annotation;

import javax.annotation.concurrent.Immutable;

import org.apache.directory.api.ldap.model.constants.SupportedSaslMechanisms;
import org.apache.directory.server.annotations.CreateChngPwdServer;
import org.apache.directory.server.annotations.CreateKdcServer;
import org.apache.directory.server.annotations.CreateLdapServer;
import org.apache.directory.server.annotations.CreateTransport;
import org.apache.directory.server.annotations.SaslMechanism;
import org.apache.directory.server.annotations.TransportType;
import org.apache.directory.server.core.annotations.ContextEntry;
import org.apache.directory.server.core.annotations.CreateAuthenticator;
import org.apache.directory.server.core.annotations.CreateDS;
import org.apache.directory.server.core.annotations.CreateIndex;
import org.apache.directory.server.core.annotations.CreatePartition;
import org.apache.directory.server.core.annotations.LoadSchema;
import org.apache.directory.server.core.api.partition.Partition;
import org.apache.directory.server.core.kerberos.KeyDerivationInterceptor;
import org.apache.directory.server.ldap.handlers.sasl.cramMD5.CramMd5MechanismHandler;
import org.apache.directory.server.ldap.handlers.sasl.digestMD5.DigestMd5MechanismHandler;
import org.apache.directory.server.ldap.handlers.sasl.gssapi.GssapiMechanismHandler;
import org.apache.directory.server.ldap.handlers.sasl.ntlm.NtlmMechanismHandler;
import org.apache.directory.server.ldap.handlers.sasl.plain.PlainMechanismHandler;

import de.invesdwin.context.persistence.ldap.LdapProperties;
import de.invesdwin.context.security.kerberos.KerberosProperties;
import de.invesdwin.util.assertions.Assertions;
import de.invesdwin.util.lang.Reflections;
import de.invesdwin.util.lang.Strings;

/**
 * https://github.com/kwart/kerberos-using-apacheds/blob/master/src/main/java/org/jboss/test/kerberos/KerberosSetup.java
 */
@Immutable
public class DirectoryServerConfigAnnotations {

    //@CreateKdcServer(primaryRealm = "JBOSS.ORG", kdcPrincipal = "krbtgt/JBOSS.ORG@JBOSS.ORG", searchBaseDn = "dc=jboss,dc=org", transports = { @CreateTransport(protocol = "UDP", port = 6088) })
    @CreateKdcServer
    public CreateKdcServer newCreateKdcServerAnnotation() {
        final CreateKdcServer defaults = Reflections.getAnnotation(CreateKdcServer.class);
        return new CreateKdcServer() {

            @Override
            public Class<? extends Annotation> annotationType() {
                return defaults.annotationType();
            }

            @Override
            public CreateTransport[] transports() {
                return new CreateTransport[] { newKdcCreateTransportAnnotation() };
            }

            @Override
            public String searchBaseDn() {
                return LdapProperties.LDAP_CONTEXT_BASE;
            }

            @Override
            public String primaryRealm() {
                return KerberosProperties.KERBEROS_PRIMARY_REALM;
            }

            @Override
            public String name() {
                return defaults.name();
            }

            @Override
            public long maxTicketLifetime() {
                return defaults.maxTicketLifetime();
            }

            @Override
            public long maxRenewableLifetime() {
                return defaults.maxRenewableLifetime();
            }

            @Override
            public String kdcPrincipal() {
                return KerberosProperties.KERBEROS_SERVICE_PRINCIPAL;
            }

            @Override
            public CreateChngPwdServer[] chngPwdServer() {
                return defaults.chngPwdServer();
            }
        };
    }

    @CreateLdapServer(saslMechanisms = {
            @SaslMechanism(name = SupportedSaslMechanisms.PLAIN, implClass = PlainMechanismHandler.class),
            @SaslMechanism(name = SupportedSaslMechanisms.CRAM_MD5, implClass = CramMd5MechanismHandler.class),
            @SaslMechanism(name = SupportedSaslMechanisms.DIGEST_MD5, implClass = DigestMd5MechanismHandler.class),
            @SaslMechanism(name = SupportedSaslMechanisms.GSSAPI, implClass = GssapiMechanismHandler.class),
            @SaslMechanism(name = SupportedSaslMechanisms.NTLM, implClass = NtlmMechanismHandler.class),
            @SaslMechanism(name = SupportedSaslMechanisms.GSS_SPNEGO, implClass = NtlmMechanismHandler.class) })
    public CreateLdapServer newCreateLdapServerAnnotation() {
        final CreateLdapServer defaults = Reflections.getAnnotation(CreateLdapServer.class);
        return new CreateLdapServer() {

            @Override
            public Class<? extends Annotation> annotationType() {
                return defaults.annotationType();
            }

            @Override
            public CreateTransport[] transports() {
                return new CreateTransport[] { newLdapCreateTransportAnnotation() };
            }

            @Override
            public String[] saslRealms() {
                return defaults.saslRealms();
            }

            @Override
            public String saslPrincipal() {
                return KerberosProperties.KERBEROS_SERVICE_PRINCIPAL;
            }

            @Override
            public SaslMechanism[] saslMechanisms() {
                return defaults.saslMechanisms();
            }

            @Override
            public String saslHost() {
                return KerberosProperties.KERBEROS_SERVER_URI.getHost();
            }

            @Override
            public Class<?> ntlmProvider() {
                return defaults.ntlmProvider();
            }

            @Override
            public String name() {
                return defaults.name();
            }

            @Override
            public int maxTimeLimit() {
                return defaults.maxTimeLimit();
            }

            @Override
            public long maxSizeLimit() {
                return defaults.maxSizeLimit();
            }

            @Override
            public String keyStore() {
                return defaults.keyStore();
            }

            @Override
            public Class<?> factory() {
                return defaults.factory();
            }

            @Override
            public Class<?>[] extendedOpHandlers() {
                return defaults.extendedOpHandlers();
            }

            @Override
            public String certificatePassword() {
                return defaults.certificatePassword();
            }

            @Override
            public boolean allowAnonymousAccess() {
                return defaults.allowAnonymousAccess();
            }
        };
    }

    //@CreateDS(name = "JBossDS", partitions = { @CreatePartition(name = "jboss", suffix = "dc=jboss,dc=org", contextEntry = @ContextEntry(entryLdif = "dn: dc=jboss,dc=org\n"
    //  + "dc: jboss\n" + "objectClass: top\n" + "objectClass: domain\n\n"), indexes = {
    //@CreateIndex(attribute = "objectClass"), @CreateIndex(attribute = "dc"), @CreateIndex(attribute = "ou") }) }, additionalInterceptors = { KeyDerivationInterceptor.class })
    @CreateDS(factory = ConfiguredDirectoryServiceFactory.class, additionalInterceptors = { KeyDerivationInterceptor.class })
    public CreateDS newCreateDSAnnotation() {
        final CreateDS defaults = Reflections.getAnnotation(CreateDS.class);
        return new CreateDS() {

            @Override
            public Class<? extends Annotation> annotationType() {
                return defaults.annotationType();
            }

            @Override
            public CreatePartition[] partitions() {
                return new CreatePartition[] { newCreatePartitionAnnotation() };
            }

            @Override
            public String name() {
                return defaults.name();
            }

            @Override
            public LoadSchema[] loadedSchemas() {
                return defaults.loadedSchemas();
            }

            @Override
            public Class<?> factory() {
                return defaults.factory();
            }

            @Override
            public boolean enableChangeLog() {
                return defaults.enableChangeLog();
            }

            @Override
            public boolean enableAccessControl() {
                return defaults.enableAccessControl();
            }

            @Override
            public CreateAuthenticator[] authenticators() {
                return defaults.authenticators();
            }

            @Override
            public boolean allowAnonAccess() {
                return defaults.allowAnonAccess();
            }

            @Override
            public Class<?>[] additionalInterceptors() {
                return defaults.additionalInterceptors();
            }
        };
    }

    @CreateTransport(protocol = "KRB")
    private CreateTransport newKdcCreateTransportAnnotation() {
        final CreateTransport defaults = Reflections.getAnnotation(CreateTransport.class);
        return new CreateTransport() {

            @Override
            public Class<? extends Annotation> annotationType() {
                return defaults.annotationType();
            }

            @Override
            public TransportType type() {
                return defaults.type();
            }

            @Override
            public boolean ssl() {
                return defaults.ssl();
            }

            @Override
            public String protocol() {
                return defaults.protocol();
            }

            @Override
            public int port() {
                return KerberosProperties.KERBEROS_SERVER_URI.getPort();
            }

            @Override
            public int nbThreads() {
                return defaults.nbThreads();
            }

            @Override
            public int backlog() {
                return defaults.backlog();
            }

            @Override
            public String address() {
                return KerberosProperties.KERBEROS_SERVER_URI.getHost();
            }
        };
    }

    @CreateTransport(protocol = "LDAP")
    private CreateTransport newLdapCreateTransportAnnotation() {
        final CreateTransport defaults = Reflections.getAnnotation(CreateTransport.class);
        return new CreateTransport() {

            @Override
            public Class<? extends Annotation> annotationType() {
                return defaults.annotationType();
            }

            @Override
            public TransportType type() {
                return defaults.type();
            }

            @Override
            public boolean ssl() {
                return defaults.ssl();
            }

            @Override
            public String protocol() {
                return defaults.protocol();
            }

            @Override
            public int port() {
                return LdapProperties.LDAP_CONTEXT_URI.getPort();
            }

            @Override
            public int nbThreads() {
                return defaults.nbThreads();
            }

            @Override
            public int backlog() {
                return defaults.backlog();
            }

            @Override
            public String address() {
                return LdapProperties.LDAP_CONTEXT_URI.getHost();
            }
        };
    }

    @CreatePartition(name = "invesdwin", suffix = "_DUMMY_", indexes = { @CreateIndex(attribute = "objectClass"),
            @CreateIndex(attribute = "dc"), @CreateIndex(attribute = "ou"), @CreateIndex(attribute = "cn"),
            @CreateIndex(attribute = "sn") })
    private CreatePartition newCreatePartitionAnnotation() {
        final CreatePartition defaults = Reflections.getAnnotation(CreatePartition.class);
        return new CreatePartition() {

            @Override
            public Class<? extends Annotation> annotationType() {
                return defaults.annotationType();
            }

            @Override
            public Class<? extends Partition> type() {
                return defaults.type();
            }

            @Override
            public String suffix() {
                return LdapProperties.LDAP_CONTEXT_BASE;
            }

            @Override
            public String name() {
                return defaults.name();
            }

            //*     @Indexes( {
            //*  @CreateIndex( attribute = "cn" ),
            //*  @CreateIndex( attribute = "sn' )

            @Override
            public CreateIndex[] indexes() {
                return defaults.indexes();
            }

            //      @ContextEntry(
            //*  {
            //*      "dn: dc=example, dc=com",
            //*      "objectclass: top",
            //*      "objectclass: domain",
            //*      "dc: example",
            //*  }),
            @Override
            public ContextEntry contextEntry() {
                return newContextEntryAnnotation();
            }

            @Override
            public int cacheSize() {
                return defaults.cacheSize();
            }
        };
    }

    @ContextEntry(entryLdif = "_DUMMY_")
    private ContextEntry newContextEntryAnnotation() {
        final ContextEntry defaults = Reflections.getAnnotation(ContextEntry.class);
        return new ContextEntry() {

            @Override
            public Class<? extends Annotation> annotationType() {
                return defaults.annotationType();
            }

            @Override
            public String entryLdif() {
                final String kerberosServicePrincipal = KerberosProperties.KERBEROS_SERVICE_PRINCIPAL;
                Assertions.assertThat(kerberosServicePrincipal).endsWith(
                        "@" + KerberosProperties.KERBEROS_PRIMARY_REALM);
                final String uid = Strings.substringBefore(kerberosServicePrincipal, "/");
                Assertions.assertThat(uid).isNotEmpty();
                //
                return "dn: " + LdapProperties.LDAP_CONTEXT_BASE + "\n" //
                        + "objectclass: top\n" //
                        + "objectclass: domain\n" //
                        + "dc: invesdwin\n" //
                        + "\n" //
                        //dn: ou=users,dc=example,dc=com
                        //objectClass: organizationalUnit
                        //objectClass: top
                        //ou: users
                        + "dn: ou=users," + LdapProperties.LDAP_CONTEXT_BASE + "\n"//
                        + "objectClass: organizationalUnit\n"//
                        + "objectClass: top\n"//
                        + "ou: users\n"//
                        + "\n"//
                        ;

            }
        };
    }
}
