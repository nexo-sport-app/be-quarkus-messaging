package it.nexoapp.messaging.contract;

import java.util.Set;
import java.util.UUID;

/**
 * Version 1 contract of the identity and access projection feed: published when the access of a person to an
 * association changes (consumer arrives in Phase 2, ROL-13). Carries identifiers and role names only, never personal
 * data.
 *
 * @param associationId identifier of the association (tenant) whose access changed
 * @param memberId      identifier of the member the access belongs to
 * @param issuer        issuer ({@code iss}) of the identity provider the subject belongs to
 * @param subject       subject ({@code sub}) of the person at the identity provider
 * @param roles         names of the roles the member currently holds in the association
 * @param accessVersion monotonically increasing version of the member's access, used to ignore stale events
 */
public record IdentityAccessChanged(
        UUID associationId,
        UUID memberId,
        String issuer,
        String subject,
        Set<String> roles,
        long accessVersion) {

    /** Event type carried in the envelope for this contract. */
    public static final String TYPE = "identity.access.changed";

    /** Version of this contract carried in the envelope. */
    public static final int VERSION = 1;

    /**
     * Makes the role set immutable and never {@code null}.
     */
    public IdentityAccessChanged {
        roles = roles == null ? Set.of() : Set.copyOf(roles);
    }
}
