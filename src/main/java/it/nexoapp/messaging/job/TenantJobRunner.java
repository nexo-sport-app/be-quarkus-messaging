package it.nexoapp.messaging.job;

import java.util.UUID;
import java.util.function.Supplier;

/**
 * Entry point for scheduled jobs that work on the data of one association.
 * <p>
 * A scheduled job runs on a scheduler thread without any HTTP request, so nobody can derive the tenant, the audit actor
 * or a correlation id from a token. A job that has to touch tenant data iterates the tenants explicitly and passes the
 * work for each one here. The runner runs the work inside that tenant, with the application itself as audit actor
 * ({@code system:<app.code>}), with a new correlation id and the tenant in the logging context, and in one new database
 * transaction. Everything is restored when the work ends, also when it fails, so nothing leaks into the next run.
 * <p>
 * A job must not open a transaction before calling the runner: Hibernate resolves the tenant when the session opens.
 */
public interface TenantJobRunner {

    /**
     * Runs the work of a job for one tenant in its own transaction.
     *
     * @param jobName  name of the job, used for logging only
     * @param tenantId identifier of the association the work is for, never null
     * @param work     the work to run
     * @throws IllegalArgumentException if the tenant is null, in which case nothing runs
     * @throws IllegalStateException    if called inside an unscoped block, in which case nothing runs
     */
    void runForTenant(String jobName, UUID tenantId, Runnable work);

    // -----------------------------------------------------------------------------------------------------------------

    /**
     * Calls a supplier for one tenant in its own transaction and returns its value.
     *
     * @param <T>      type of the supplied value
     * @param jobName  name of the job, used for logging only
     * @param tenantId identifier of the association the work is for, never null
     * @param work     the work to call
     * @return the value supplied by the work
     * @throws IllegalArgumentException if the tenant is null, in which case nothing runs
     * @throws IllegalStateException    if called inside an unscoped block, in which case nothing runs
     */
    <T> T callForTenant(String jobName, UUID tenantId, Supplier<T> work);

}
