package com.us.kc.ldap;

import com.unboundid.ldap.sdk.*;
import org.keycloak.OAuth2Constants;
import org.keycloak.admin.client.Keycloak;
import org.keycloak.admin.client.KeycloakBuilder;
import org.keycloak.admin.client.resource.GroupsResource;
import org.keycloak.admin.client.resource.UsersResource;
import org.keycloak.representations.idm.GroupRepresentation;
import org.keycloak.representations.idm.UserRepresentation;

import java.util.*;

public class LdapToKeycloakGroupSync {

    private static final String LDAP_URL = "ldap://your-ldap-server:389";
    private static final String LDAP_BIND_DN = "CN=LDAPUser,OU=ServiceAccounts,DC=example,DC=com";
    private static final String LDAP_PASSWORD = "YourLDAPPassword";
    private static final String LDAP_BASE_DN = "DC=example,DC=com";
    private static final String GROUP_SEARCH_FILTER = "(objectClass=group)";
    private static final String GROUP_NAME_ATTR = "cn";
    private static final String MEMBER_ATTR = "member";

    // Keycloak config
    private static final String KC_SERVER_URL = "https://keycloak.example.com/auth";
    private static final String KC_REALM = "myrealm";
    private static final String KC_CLIENT_ID = "admin-cli";
    private static final String KC_USERNAME = "admin";
    private static final String KC_PASSWORD = "YourKCPassword";

    public static void main(String[] args) throws Exception {
        // 1. Connect to LDAP
        LDAPConnection ldap = new LDAPConnection(LDAP_URL, 389, LDAP_BIND_DN, LDAP_PASSWORD);

        // 2. Connect to Keycloak
        Keycloak kc = KeycloakBuilder.builder()
                .serverUrl(KC_SERVER_URL)
                .realm("master") // Admin login realm
                .username(KC_USERNAME)
                .password(KC_PASSWORD)
                .clientId(KC_CLIENT_ID)
                .grantType(OAuth2Constants.PASSWORD)
                .build();

        GroupsResource groupsResource = kc.realm(KC_REALM).groups();
        UsersResource usersResource = kc.realm(KC_REALM).users();

        // 3. Search LDAP groups
        SearchResult searchResult = ldap.search(LDAP_BASE_DN, SearchScope.SUB, GROUP_SEARCH_FILTER, GROUP_NAME_ATTR, MEMBER_ATTR);

        for (SearchResultEntry entry : searchResult.getSearchEntries()) {
            String groupName = entry.getAttributeValue(GROUP_NAME_ATTR);
            System.out.println("Processing group: " + groupName);

            // 4. Create group in Keycloak if missing
            GroupRepresentation group = findOrCreateGroup(groupsResource, groupName);

            // 5. Add users to group
            String[] members = entry.getAttributeValues(MEMBER_ATTR);
            if (members != null) {
                for (String memberDN : members) {
                    String username = extractCNFromDN(memberDN);
                    Optional<UserRepresentation> user = findUserByUsername(usersResource, username);
                    user.ifPresent(u -> usersResource.get(u.getId()).joinGroup(group.getId()));
                }
            }
        }

        ldap.close();
        kc.close();
    }

    private static GroupRepresentation findOrCreateGroup(GroupsResource groupsResource, String groupName) {
        List<GroupRepresentation> existingGroups = groupsResource.groups();
        for (GroupRepresentation g : existingGroups) {
            if (g.getName().equalsIgnoreCase(groupName)) {
                return g;
            }
        }
        GroupRepresentation newGroup = new GroupRepresentation();
        newGroup.setName(groupName);
        groupsResource.add(newGroup);
        // Fetch created group
        return groupsResource.groups().stream()
                .filter(g -> g.getName().equalsIgnoreCase(groupName))
                .findFirst()
                .orElseThrow();
    }

    private static Optional<UserRepresentation> findUserByUsername(UsersResource usersResource, String username) {
        List<UserRepresentation> users = usersResource.search(username, true);
        return users.isEmpty() ? Optional.empty() : Optional.of(users.get(0));
    }

    private static String extractCNFromDN(String dn) {
        String[] parts = dn.split(",");
        for (String part : parts) {
            if (part.trim().toLowerCase().startsWith("cn=")) {
                return part.substring(3);
            }
        }
        return dn;
    }
}
