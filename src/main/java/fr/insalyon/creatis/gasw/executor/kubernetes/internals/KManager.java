package fr.insalyon.creatis.gasw.executor.kubernetes.internals;

import java.io.File;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;

import org.joda.time.DateTime;
import org.joda.time.Duration;

import fr.insalyon.creatis.gasw.GaswConfiguration;
import fr.insalyon.creatis.gasw.GaswConstants;
import fr.insalyon.creatis.gasw.GaswException;
import fr.insalyon.creatis.gasw.execution.GaswStatus;
import fr.insalyon.creatis.gasw.executor.kubernetes.config.KConfiguration;
import fr.insalyon.creatis.gasw.executor.kubernetes.config.json.properties.KConfig;
import fr.insalyon.creatis.gasw.executor.kubernetes.config.json.properties.KVolumeData;
import io.kubernetes.client.openapi.ApiException;
import io.kubernetes.client.openapi.apis.CoreV1Api;
import io.kubernetes.client.openapi.models.V1Namespace;
import io.kubernetes.client.openapi.models.V1NamespaceList;
import io.kubernetes.client.openapi.models.V1ObjectMeta;
import lombok.NoArgsConstructor;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class KManager {

    final private String workflowName;
    final private List<KVolume> customVolumes;
    final private KConfig config;
    private KVolume workflowVolume;

    private volatile ArrayList<KJob> jobs;
    private Boolean end;
    private Boolean inited;

    public KManager(final String workflowName) {
        this.workflowName = workflowName;
        this.jobs = new ArrayList<>();
        this.config = KConfiguration.getInstance().getConfig();
        this.customVolumes = new ArrayList<>();
    }

    public void init() {

        log.info("K8s Manager init with " + workflowName);
        try {
            checkNamespace();
            log.info("Namespaces checked !");
            checkAllVolumes();
            log.info("SharedUser volume checked");
            checkOutputsDir();
            log.info("User outputs directories checked !");

            inited = true;
        } catch (ApiException e) {
            log.error("Failed to init the manager", e);
            inited = false;
        }
    }

    public void destroy() {
        end = true;

        try {
            if (this.workflowVolume != null) {
                workflowVolume.deletePVC();
                workflowVolume.deletePV();
            }
            for (final KJob job : jobs) {
                job.clean();
            }
        } catch (ApiException e) {
            log.error("Failed to destroy the manager");
        }
    }

    /**
     * Check if the k8s cluster already have the namespace
     * if it isn't here, then it is created
     */
    public void checkNamespace() throws ApiException {
        final CoreV1Api api = KConfiguration.getInstance().getCoreApi();
        final V1NamespaceList list = api.listNamespace().execute();
        final String targetName = config.getK8sNamespace();

        for (final V1Namespace ns : list.getItems()) {
            final String name = ns.getMetadata().getName();

            if (name.equals(targetName)) {
                return;
            }
        }

        final V1Namespace ns = new V1Namespace()
                .metadata(new V1ObjectMeta().name(targetName));

        api.createNamespace(ns).execute();
    }

    /**
     * Check all of the volumes presents inside the config
     */
    public void checkAllVolumes() throws ApiException {
        // workflow
        final KVolumeData wVolumeData = new KVolumeData()
                .setName(workflowName)
                .setMountPathContainer(config.getWorkflowsLocation() + workflowName)
                .setAccessModes("ReadWriteMany")
                .setNfsFolder(workflowName);

        workflowVolume = new KVolume(KConfiguration.getInstance(), wVolumeData);
        workflowVolume.createPV();
        workflowVolume.createPVC();

        // others
        for (final KVolumeData configVolume : config.getVolumes()) {
            KVolume kVolume = KVolume.retrieve(configVolume);

            if (kVolume == null) {
                kVolume = new KVolume(KConfiguration.getInstance(), configVolume);

                kVolume.createPV();
                kVolume.createPVC();
            }
            customVolumes.add(kVolume);
        }
    }

    /**
     * Check for existance of STDOUR and STDERR from the plugin machine.
     */
    public void checkOutputsDir() {
        final String[] dirs = { GaswConstants.OUT_ROOT, GaswConstants.ERR_ROOT, "./cache" };

        Arrays.stream(dirs)
                .map(File::new)
                .forEach(dir -> {
                    if ( ! dir.exists()) {
                        dir.mkdirs();
                    }
                });
    }

    /**
     * Check if the init state is finished
     */
    private boolean isReady() throws InterruptedException{
        int i = 0;

        while (true) {
            if (inited != null && inited == true) {
                return true;
            } else if (i < config.getOptions().getMaxRetryToPush() || (inited != null && inited == false)) {
                Thread.sleep(10_000);
            } else {
                return false;
            }
            i++;
        }
    }

    /**
     * Create a thread instance that will launch job when ressources
     * are available (volumes).
     * The end variable is used to know if a thread instance
     * has already been launched.
     */
    private void submitter(final KJob exec) {
        if (end == null) {
            end = false;
            new Thread(this.new KRunner()).start();
        }
        synchronized (this) {
            jobs.add(exec);
        }
    }

    /**
     * Public submitter that prepare the K8sJob object and wait for
     * the manager to be initied (in case of slow cluster)
     */
    public void submitter(final String cmd, final String dockerImage, final String jobID) {
        final KJob exec = new KJob(jobID, workflowName);
        final List<KVolume> volumes = new ArrayList<>();

        volumes.add(workflowVolume);
        volumes.addAll(customVolumes);

        try {
            if (isReady()) {
                exec.getData().setCommand(cmd);
                exec.getData().setImage(dockerImage);
                exec.getData().setVolumes(volumes);
                exec.getData().setWorkingDir(workflowVolume.getData().getMountPathContainer());
                exec.configure();
                submitter(exec);
            } else {
                exec.getData().setStatus(GaswStatus.STALLED);
            }
        } catch (InterruptedException e) {
            log.error("Interrupted exception at submitter: ", e);
        }
    }

    public ArrayList<KJob> getUnfinishedJobs() {
        return jobs.stream()
                .filter(job -> ! job.isTerminated())
                .collect(Collectors.toCollection(ArrayList::new));
    }

    public KJob getJob(final String jobId) {
        return jobs.stream()
                .filter(job -> job.getData().getJobID().equals(jobId))
                .findFirst()
                .orElse(null);
    }

    @NoArgsConstructor
    class KRunner implements Runnable {
        private Boolean ready = false;
        private DateTime startedTime;

        @Override
        public void run() {
            try {
                startedTime = DateTime.now();
                loop();
            } catch (GaswException | ApiException | InterruptedException e) {
                log.error(e.getMessage());
                log.error("Something bad happened during the K8sRunner", e);
            }
        }

        private void sleep() throws GaswException, InterruptedException {
            final Duration diff = new Duration(startedTime, DateTime.now());

            if (diff.getStandardSeconds() > config.getOptions().getTimeToVolumeBeReady()) {
                throw new GaswException("Volume wasn't eady in 2 minutes, aborting !");
            } else {
                checker();
                Thread.sleep(GaswConfiguration.getInstance().getDefaultSleeptime());
            }
        }

        private void loop() throws ApiException, InterruptedException, GaswException {
            while (ready == false) {
                sleep();
            }
            while (end == false) {
                synchronized (this) {
                    for (final KJob exec : getUnfinishedJobs()) {
                        if (exec.getStatus() == GaswStatus.NOT_SUBMITTED) {
                            exec.getData().setStatus(GaswStatus.QUEUED);
                            exec.start();
                        }
                    }
                }
                Thread.sleep(GaswConfiguration.getInstance().getDefaultSleeptime());
            }
        }

        private synchronized void checker() {
            if ( ! ready && workflowVolume.isAvailable()) {
                for (final KVolume custom : customVolumes) {
                    if (custom.isAvailable() == false) {
                        break;
                    }
                }
                ready = true;
            }
        }
    }
}
