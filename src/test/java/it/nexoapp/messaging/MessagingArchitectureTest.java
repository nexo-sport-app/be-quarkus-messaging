package it.nexoapp.messaging;

import com.tngtech.archunit.core.importer.ImportOption;
import com.tngtech.archunit.junit.AnalyzeClasses;
import com.tngtech.archunit.junit.ArchTest;
import com.tngtech.archunit.junit.ArchTests;
import it.nexoapp.common.testing.ArchitectureRules;

/**
 * Applies the shared architecture rules of the platform to the messaging library: encapsulation, layering and native
 * SQL only inside the outbox and inbox packages.
 */
@AnalyzeClasses(packages = "it.nexoapp.messaging", importOptions = ImportOption.DoNotIncludeTests.class)
class MessagingArchitectureTest {

    /** The shared rule set. */
    @ArchTest
    static final ArchTests RULES = ArchTests.in(ArchitectureRules.class);

}
