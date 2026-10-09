package com.alejandro.mtofield.configuration.security;

/**
 * Nombres de los claims de Keycloak que la API lee.
 */
public final class JwtClaimNames {

    private JwtClaimNames() {
    }

    public static final String REALM_ACCESS = "realm_access";
    public static final String RESOURCE_ACCESS = "resource_access";
    public static final String ROLES = "roles";
    public static final String SCOPE = "scope";
    public static final String AUDIENCE = "aud";
    public static final String PREFERRED_USERNAME = "preferred_username";
    public static final String EMAIL = "email";
}
