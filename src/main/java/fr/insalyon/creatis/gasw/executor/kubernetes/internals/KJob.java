package fr.insalyon.creatis.gasw.executor.kubernetes.internals;

import java.util.Arrays;

import fr.insalyon.creatis.gasw.execution.GaswStatus;
import fr.insalyon.creatis.gasw.executor.kubernetes.config.KConfiguration;
import io.kubernetes.client.openapi.ApiException;
import io.kubernetes.client.openapi.apis.BatchV1Api;
import io.kubernetes.client.openapi.apis.CoreV1Api;
import io.kubernetes.client.openapi.models.V1Container;
import io.kubernetes.client.openapi.models.V1ContainerStateTerminated;
import io.kubernetes.client.openapi.models.V1ContainerStatus;
import io.kubernetes.client.openapi.models.V1Job;
import io.kubernetes.client.openapi.models.V1JobSpec;
import io.kubernetes.client.openapi.models.V1JobStatus;
import io.kubernetes.client.openapi.models.V1ObjectMeta;
import io.kubernetes.client.openapi.models.V1Pod;
import io.kubernetes.client.openapi.models.V1PodList;
import io.kubernetes.client.openapi.models.V1PodSpec;
import io.kubernetes.client.openapi.models.V1PodTemplateSpec;
import io.kubernetes.client.openapi.models.V1SecurityContext;
import lombok.Getter;
import lombok.Setter;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class KJob {

    private final KConfiguration conf;

    @Getter
    final private KJobData data;
    @Getter
    @Setter
    private boolean terminated = false;

    public KJob(final String jobID, final String workflowName) {
        conf = KConfiguration.getInstance();
        data = new KJobData();
        data.setJobID(jobID);
        data.setWorkflowName(workflowName);
        data.setStatus(GaswStatus.UNDEFINED);

        setKubernetesJobID(jobID);

        data.setContainer(new V1Container()
                .name(data.getKubernetesJobID())
                .securityContext(new V1SecurityContext().privileged(true)));
    }

    private void setKubernetesJobID(final String baseName) {
        for (int i = baseName.length() - 1; i > 0; i--) {
            if ( ! Character.isDigit(baseName.charAt(i))) {
                data.setKubernetesJobID(baseName.substring(i + 1, baseName.length()));
                break;
            }
        }
        data.setKubernetesJobID(data.getWorkflowName().toLowerCase() + "-" + data.getKubernetesJobID());
    }

    private GaswStatus getStatusRequest(final BatchV1Api api) {
        try {
            final V1Job updatedJob = api
                    .readNamespacedJob(data.getJob().getMetadata().getName(), conf.getConfig().getK8sNamespace())
                    .execute();
            final V1JobStatus status = updatedJob.getStatus();

            if (status.getFailed() != null && status.getFailed() > 0) {
                return GaswStatus.ERROR;
            } else if (status.getActive() != null && status.getActive() > 0) {
                return GaswStatus.RUNNING;
            } else if (status.getSucceeded() != null && status.getSucceeded() > 0) {
                return GaswStatus.COMPLETED;
            } else {
                return GaswStatus.QUEUED;
            }
        } catch (ApiException e) {
            log.info("Impossible de récuperer l'état du job" + e.getStackTrace());
            return GaswStatus.UNDEFINED;
        }
    }

    /**
     * This create the V1Job item and configure alls specs
     * 
     * @apiNote Can be easilly upgraded to List<V1Container>
     * @param container
     */
    public void configure() {
        final V1ObjectMeta meta = new V1ObjectMeta().name(data.getKubernetesJobID())
                .namespace(conf.getConfig().getK8sNamespace());

        final V1PodSpec podSpec = new V1PodSpec()
                .containers(Arrays.asList(data.getContainer()))
                .restartPolicy("Never")
                .volumes(data.getVolumes());

        final V1PodTemplateSpec podspecTemplate = new V1PodTemplateSpec().spec(podSpec);

        final V1JobSpec jobSpec = new V1JobSpec()
                .ttlSecondsAfterFinished(conf.getConfig().getOptions().getTtlJob())
                .template(podspecTemplate)
                .backoffLimit(0);

        data.setJob(new V1Job()
                .spec(jobSpec)
                .metadata(meta));
        data.setStatus(GaswStatus.NOT_SUBMITTED);
    }

    /**
     * Send the request against the kubernetes cluster to create the job
     * 
     * @throws ApiException
     */
    public void start() throws ApiException {
        final BatchV1Api api = conf.getBatchApi();

        if (data.getJob() == null) {
            log.error("Impossible to start job value is null (may not be configured)");
        } else {
            api.createNamespacedJob(conf.getConfig().getK8sNamespace(), data.getJob()).execute();
            data.setStatus(GaswStatus.SUCCESSFULLY_SUBMITTED);
        }
    }

    /**
     * Kill method stop running pods and erase job for k8s api memory.
     * 
     * @throws ApiException
     */
    public void kill() throws ApiException {
        final BatchV1Api api = conf.getBatchApi();

        if (data.getStatus() != GaswStatus.NOT_SUBMITTED && data.getJob() != null) {
            api.deleteNamespacedJob(data.getJob().getMetadata().getName(), conf.getConfig().getK8sNamespace())
                    .propagationPolicy("Foreground").execute();
            data.setJob(null);
        }
    }

    /**
     * This function do the same as kill but check for the status to be terminated.
     * 
     * @throws ApiException
     */
    public void clean() throws ApiException {
        if (data.getJob() != null && isTerminated()) {
            kill();
        }
    }

    /**
     * @apiNote The function retry X times with a sleep of Y depending on
     *          K8sConstants.
     * @return GaswStatus.UNDEFINED means that the job weren't configured
     */
    public GaswStatus getStatus() {
        final BatchV1Api api = conf.getBatchApi();
        final GaswStatus status = data.getStatus();
        GaswStatus retrievedStatus;

        if (status == GaswStatus.STALLED) {
            return status;
        }
        if (data.getJob() != null) {
            if (status == GaswStatus.NOT_SUBMITTED) {
                return status;
            }
            for (int i = 0; i < conf.getConfig().getOptions().getStatusRetry(); i++) {
                retrievedStatus = getStatusRequest(api);

                if (retrievedStatus != GaswStatus.UNDEFINED) {
                    return retrievedStatus;
                } else {
                    try {
                        Thread.sleep(conf.getConfig().getOptions().getStatusRetryWait());
                    } catch (InterruptedException e) {
                        log.error("Interrupted exception at getStatus: ", e);
                    }
                }
            }
            return GaswStatus.STALLED;
        }
        return GaswStatus.UNDEFINED;
    }

    /**
     * @implNote Should be adapted if multiple containers / pods per job
     */
    public Integer getExitCode() {
        final CoreV1Api coreApi = conf.getCoreApi();
        final String jobName = data.getJob().getMetadata().getName();

        if (data.getJob() == null) {
            return 1;
        }
        try {
            final V1PodList podsList = coreApi.listNamespacedPod(conf.getConfig().getK8sNamespace())
                    .labelSelector("job-name=" + jobName).execute();
            final V1Pod pod = podsList.getItems().get(0);

            for (final V1ContainerStatus status : pod.getStatus().getContainerStatuses()) {
                final V1ContainerStateTerminated end = status.getState().getTerminated();

                if (end != null && end.getExitCode() != 0) {
                    return end.getExitCode();
                }
            }
            return 0;
        } catch (ApiException e) {
            return 1;
        }
    }
}
