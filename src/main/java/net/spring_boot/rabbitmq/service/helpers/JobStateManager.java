package net.spring_boot.rabbitmq.service.helpers;

import net.spring_boot.rabbitmq.enums.JobStatus;
import org.springframework.stereotype.Service;

import java.util.EnumSet;
import java.util.Map;

@Service
public class JobStateManager {

    private static final Map<JobStatus, EnumSet<JobStatus>> TRANSITIONS = Map.of(
            JobStatus.QUEUED, EnumSet.of(JobStatus.PROCESSING),
            JobStatus.PROCESSING, EnumSet.of(JobStatus.COMPLETED, JobStatus.FAILED),
            JobStatus.FAILED, EnumSet.of(JobStatus.RETRYING, JobStatus.DEAD_LETTERED),
            JobStatus.RETRYING, EnumSet.of(JobStatus.PROCESSING),
            JobStatus.DEAD_LETTERED, EnumSet.of(JobStatus.QUEUED),
            JobStatus.COMPLETED, EnumSet.noneOf(JobStatus.class)
    );

    public void validateTransition(JobStatus from, JobStatus to) {
        if (from == null && to == JobStatus.QUEUED) {
            return;
        }
        EnumSet<JobStatus> allowed = TRANSITIONS.get(from);
        if (allowed == null || !allowed.contains(to)) {
            throw new IllegalArgumentException(
                    "Invalid state transition from " + from + " to " + to);
        }
    }
}
