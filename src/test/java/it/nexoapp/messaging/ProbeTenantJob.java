package it.nexoapp.messaging;

import io.quarkus.scheduler.Scheduled;
import it.nexoapp.common.constant.MDCConstant;
import it.nexoapp.common.context.ExecutionScope;
import it.nexoapp.common.tenant.TenantContext;
import it.nexoapp.common.utils.MDCUtils;
import it.nexoapp.messaging.job.TenantJobRunner;
import jakarta.enterprise.context.ApplicationScoped;
import lombok.RequiredArgsConstructor;

import java.util.UUID;
import java.util.concurrent.atomic.AtomicReference;

/**
 * Test-only scheduled job. It does nothing until a test arms it with a target tenant; then, on its next tick, it runs
 * one piece of work for that tenant through {@link TenantJobRunner} and records what the work and the scheduler thread
 * observed.
 */
@ApplicationScoped
@RequiredArgsConstructor
public class ProbeTenantJob {

    /** Identity of the job in the scheduler. */
    public static final String JOB_NAME = "probe-job";

    /**
     * What the job observed.
     *
     * @param tenantInside        tenant seen by the work, empty string when none
     * @param associationInside   MDC association id seen by the work
     * @param callIdInside        MDC call id seen by the work
     * @param actorInside         audit actor seen by the work
     * @param visibleBefore       number of probe rows visible to the work before it wrote
     * @param tenantAfter         tenant of the scheduler thread after the runner returned, empty string when none
     * @param associationAfter    MDC association id of the scheduler thread after the runner returned
     * @param callIdAfter         MDC call id of the scheduler thread after the runner returned
     * @param actorAfter          audit actor of the scheduler thread after the runner returned
     * @param callIdBefore        MDC call id of the scheduler thread before the runner was called
     * @param error               failure raised by the runner, null when none
     */
    public record Observation(String tenantInside, String associationInside, String callIdInside,
                              String actorInside, long visibleBefore, String tenantAfter, String associationAfter,
                              String callIdAfter, String actorAfter, String callIdBefore, Throwable error) {
    }

    /**
     * What the job has to do on its next tick.
     *
     * @param tenant tenant to run for
     * @param label  label of the row the work writes
     */
    private record Target(UUID tenant, String label) {
    }

    /* - state - */
    private static final AtomicReference<Target> TARGET = new AtomicReference<>();
    private static final AtomicReference<Observation> RESULT = new AtomicReference<>();

    /* - collaborators - */
    private final TenantJobRunner runner;
    private final ProbeRecordRepository repository;

    // -----------------------------------------------------------------------------------------------------------------

    /**
     * Arms the job for exactly one run.
     *
     * @param tenant tenant to run for
     * @param label  label of the row written by the work
     */
    public static void arm(final UUID tenant, final String label) {
        RESULT.set(null);
        TARGET.set(new Target(tenant, label));
    }

    // -----------------------------------------------------------------------------------------------------------------

    /**
     * Result of the last armed run.
     *
     * @return the observation, null until the run has completed
     */
    public static Observation result() {
        return RESULT.get();
    }

    // -----------------------------------------------------------------------------------------------------------------

    /**
     * Forgets the target and the result.
     */
    public static void reset() {
        TARGET.set(null);
        RESULT.set(null);
    }

    // -----------------------------------------------------------------------------------------------------------------

    /**
     * Tick of the job: does nothing unless armed, otherwise runs the work once for the target tenant.
     */
    @Scheduled(identity = JOB_NAME, every = "1s", concurrentExecution = Scheduled.ConcurrentExecution.SKIP)
    void tick() {
        final Target target = TARGET.getAndSet(null);
        if (target == null) {
            return;
        }
        final String callIdBefore = MDCUtils.get(MDCConstant.CALL_ID);
        final String[] inside = new String[4];
        final long[] visible = new long[1];
        Throwable error = null;
        try {
            runner.runForTenant(JOB_NAME, target.tenant(), () -> {
                inside[0] = TenantContext.current().orElse("");
                inside[1] = MDCUtils.get(MDCConstant.ASSOCIATION_ID);
                inside[2] = MDCUtils.get(MDCConstant.CALL_ID);
                inside[3] = ExecutionScope.currentActor().orElse(null);
                visible[0] = repository.count();
                repository.persist(new ProbeRecordEntity(target.label()));
            });
        } catch (final RuntimeException e) {
            error = e;
        }
        RESULT.set(new Observation(inside[0], inside[1], inside[2], inside[3], visible[0],
                TenantContext.current().orElse(""), MDCUtils.get(MDCConstant.ASSOCIATION_ID),
                MDCUtils.get(MDCConstant.CALL_ID), ExecutionScope.currentActor().orElse(null), callIdBefore, error));
    }

}
