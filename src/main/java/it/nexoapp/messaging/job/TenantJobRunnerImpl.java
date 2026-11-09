package it.nexoapp.messaging.job;

import io.quarkus.narayana.jta.QuarkusTransaction;
import it.nexoapp.common.configuration.AppInfo;
import it.nexoapp.common.constant.MDCConstant;
import it.nexoapp.common.context.ExecutionScope;
import it.nexoapp.common.tenant.TenantContext;
import it.nexoapp.common.utils.MDCUtils;
import jakarta.enterprise.context.ApplicationScoped;
import lombok.RequiredArgsConstructor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.UUID;
import java.util.function.Supplier;

/**
 * Default {@link TenantJobRunner}: mirrors the scope order of the incoming message processor, so jobs and consumers
 * give the work the same guarantees.
 */
@ApplicationScoped
@RequiredArgsConstructor
class TenantJobRunnerImpl implements TenantJobRunner {

    /* - logger - */
    private static final Logger logger = LoggerFactory.getLogger(TenantJobRunnerImpl.class);

    /* - collaborators - */
    private final AppInfo appInfo;

    // -----------------------------------------------------------------------------------------------------------------

    /**
     * {@inheritDoc}
     */
    @Override
    public void runForTenant(final String jobName, final UUID tenantId, final Runnable work) {
        callForTenant(jobName, tenantId, () -> {
            work.run();
            return null;
        });
    }

    // -----------------------------------------------------------------------------------------------------------------

    /**
     * {@inheritDoc}
     */
    @Override
    public <T> T callForTenant(final String jobName, final UUID tenantId, final Supplier<T> work) {
        if (tenantId == null) {
            throw new IllegalArgumentException("Cannot run a job without tenant");
        }
        if (TenantContext.isUnscoped()) {
            throw new IllegalStateException("Cannot run a tenant job inside an unscoped block");
        }
        final String actor = ExecutionScope.systemActor(appInfo.code());
        final String callId = UUID.randomUUID().toString();
        final String previousCallId = MDCUtils.get(MDCConstant.CALL_ID);
        final String previousAssociationId = MDCUtils.get(MDCConstant.ASSOCIATION_ID);
        MDCUtils.put(MDCConstant.ASSOCIATION_ID, tenantId.toString());
        MDCUtils.put(MDCConstant.CALL_ID, callId);
        try {
            logger.debug("Job {} starts with call id {}", jobName, callId);
            // the scopes are opened BEFORE the transaction: Hibernate resolves the tenant when the session opens
            return TenantContext.callAs(tenantId, () -> ExecutionScope.callAs(actor,
                    () -> QuarkusTransaction.requiringNew().call(work::get)));
        } finally {
            restore(MDCConstant.CALL_ID, previousCallId);
            restore(MDCConstant.ASSOCIATION_ID, previousAssociationId);
        }
    }

    // -----------------------------------------------------------------------------------------------------------------

    /**
     * Puts a logging context value back to what it was before the run.
     *
     * @param key      the logging context key
     * @param previous the previous value, null when the key was not set
     */
    private static void restore(final MDCConstant key, final String previous) {
        if (previous == null) {
            MDCUtils.remove(key);
        } else {
            MDCUtils.put(key, previous);
        }
    }

}
