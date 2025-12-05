package com.us.kc.ldap;

import javax.naming.Context;
import javax.naming.NamingException;
import javax.naming.directory.DirContext;
import javax.naming.directory.InitialDirContext;
import java.util.Hashtable;

public class LdapAuthenticator {

    public static boolean authenticate(String username, String password) {
        String ldapUrl = "ldap://localhost:389";
        String baseDN = "dc=test,dc=com";
        String userDN = "uid=" + username + ",ou=people," + baseDN;

        Hashtable<String, String> env = new Hashtable<>();
        env.put(Context.INITIAL_CONTEXT_FACTORY, "com.sun.jndi.ldap.LdapCtxFactory");
        env.put(Context.PROVIDER_URL, ldapUrl);
        env.put(Context.SECURITY_AUTHENTICATION, "simple");
        env.put(Context.SECURITY_PRINCIPAL, userDN);
        env.put(Context.SECURITY_CREDENTIALS, password);

        try {
            DirContext ctx = new InitialDirContext(env);
            ctx.close();
            return true;  // Authentication successful
        } catch (NamingException e) {
            System.out.println("LDAP auth failed: " + e.getMessage());
            return false; // Authentication failed
        }
    }

    public static void main(String[] args) {
        String testUser = "admin";         //
        String testPass = "admin";    //

        boolean isAuthenticated = authenticate(testUser, testPass);
        System.out.println("Authentication successful? " + isAuthenticated);
    }
}
