package it.nexoapp.messaging;

import io.quarkus.hibernate.orm.panache.PanacheRepositoryBase;
import jakarta.enterprise.context.ApplicationScoped;

import java.util.UUID;

/**
 * Test-only data access of {@code probe_record}. Reads are tenant-filtered by Hibernate.
 */
@ApplicationScoped
public class ProbeRecordRepository implements PanacheRepositoryBase<ProbeRecordEntity, UUID> {
}
