package com.alejandro.mtofield.configuration.security;

/**
 * Prefijos con los que se publican las autoridades en el {@code SecurityContext}.
 */
public final class SecurityAuthorityPrefixes {

    private SecurityAuthorityPrefixes() {
    }

    public static final String ROLE_PREFIX = "ROLE_";
    public static final String REALM_ROLE_PREFIX = "ROLE_REALM_";
    public static final String CLIENT_ROLE_PREFIX = "ROLE_CLIENT_";
    public static final String SCOPE_PREFIX = "SCOPE_";
}
