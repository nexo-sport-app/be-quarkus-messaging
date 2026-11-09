package it.nexoapp.messaging;

import it.nexoapp.common.tenant.TenantEntity;
import jakarta.persistence.Column;
import jakarta.persistence.Entity;
import jakarta.persistence.Id;
import jakarta.persistence.Table;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;
import org.hibernate.annotations.UuidGenerator;

import java.io.Serial;
import java.util.UUID;

/**
 * Test-only tenant-owned row, mapped to the table {@code probe_record}. Jobs and consumers write and read it to prove
 * that their work is confined to one association.
 */
@Getter
@Setter
@NoArgsConstructor
@Entity
@Table(name = "probe_record")
public class ProbeRecordEntity extends TenantEntity {

    @Serial
    private static final long serialVersionUID = 5240318870426117305L;

    /* identifiers */
    /**
     * Identifier of the row.
     */
    @Id
    @UuidGenerator(style = UuidGenerator.Style.VERSION_7)
    @Column(
            name = "id",
            columnDefinition = "UUID",
            nullable = false,
            unique = true
    )
    private UUID id;

    /* data */
    /**
     * Free text that tells the rows apart in assertions.
     */
    @Column(
            name = "label",
            columnDefinition = "VARCHAR(40)",
            nullable = false
    )
    private String label;

    // -----------------------------------------------------------------------------------------------------------------

    /**
     * Creates a row with a label.
     *
     * @param label free text of the row
     */
    public ProbeRecordEntity(final String label) {
        this.label = label;
    }

}
