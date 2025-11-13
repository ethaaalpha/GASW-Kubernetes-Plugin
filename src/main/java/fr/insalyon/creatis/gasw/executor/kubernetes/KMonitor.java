package fr.insalyon.creatis.gasw.executor.kubernetes;

import java.util.Date;

import fr.insalyon.creatis.gasw.GaswConfiguration;
import fr.insalyon.creatis.gasw.GaswException;
import fr.insalyon.creatis.gasw.bean.Job;
import fr.insalyon.creatis.gasw.dao.DAOException;
import fr.insalyon.creatis.gasw.execution.GaswMonitor;
import fr.insalyon.creatis.gasw.execution.GaswStatus;
import fr.insalyon.creatis.gasw.executor.kubernetes.config.KConstants;
import fr.insalyon.creatis.gasw.executor.kubernetes.internals.KJob;
import fr.insalyon.creatis.gasw.executor.kubernetes.internals.KManager;
import io.kubernetes.client.openapi.ApiException;
import lombok.extern.slf4j.Slf4j;

@Slf4j
final public class KMonitor extends GaswMonitor {

    private boolean stop;
    private KManager manager;

    public void setManager(KManager manager) {
        this.manager = manager;
    }

    public KMonitor() {
        super();
        stop = false;
    }

    private boolean notRunningJob(GaswStatus s) {
        return s != GaswStatus.RUNNING
                && s != GaswStatus.QUEUED
                && s != GaswStatus.UNDEFINED
                && s != GaswStatus.NOT_SUBMITTED;
    }

    @Override
    public void run() {
        while (!stop) {
            try {
                for (final KJob job : manager.getUnfinishedJobs()) {
                    final Job daoJob = jobDAO.getJobByID(job.getData().getJobID());
                    final GaswStatus status = job.getStatus();

                    if (notRunningJob(status)) {
                        job.setTerminated(true);
                        if (status == GaswStatus.ERROR || status == GaswStatus.COMPLETED) {
                            daoJob.setExitCode(job.getExitCode());
                            daoJob.setStatus(job.getExitCode() == 0 ? GaswStatus.COMPLETED : GaswStatus.ERROR);
                        } else {
                            daoJob.setStatus(status);
                        }

                        jobDAO.update(daoJob);
                        new KOutputParser(job).start();

                    } else if (status == GaswStatus.RUNNING) {
                        updateJob(job.getData().getJobID(), status);
                    }
                }
                Thread.sleep(GaswConfiguration.getInstance().getDefaultSleeptime());

            } catch (GaswException | DAOException ex) {
                log.error("Ignored exception !", ex);
            } catch (InterruptedException ex) {
                log.error("Interrupted exception, stopping the worker !");
                finish();
            }
        }
    }

    @Override
    public synchronized void add(final String jobID, final String symbolicName, final String fileName, final String parameters) throws GaswException {
        final Job job = new Job(jobID, GaswConfiguration.getInstance().getSimulationID(),
                GaswStatus.QUEUED, symbolicName, fileName, parameters,
                KConstants.EXECUTOR_NAME);

        add(job);
        log.info("Adding job: " + jobID);
        try {
            job.setQueued(new Date());
            jobDAO.update(job);

        } catch (DAOException ex) {
            log.error("Failed to add the job", ex);
        }
    }

    public synchronized void finish() {
        log.info("Monitor is off !");
        this.stop = true;
    }

    public void updateJob(final String jobID, final GaswStatus status) {
        try {
            final Job job = jobDAO.getJobByID(jobID);

            if (job.getStatus() != status) {
                if (status == GaswStatus.RUNNING) {
                    job.setDownload(new Date());
                }

                job.setStatus(status);
                jobDAO.update(job);
            }
        } catch (DAOException e) {
            log.error("Exception while updating job", e);
        }
    }

    @Override
    protected void kill(final Job job) {
        final KJob kJob = manager.getJob(job.getId());

        if (kJob == null) {
            return;
        }
        try {
            kJob.kill();
        } catch (ApiException e) {
            log.error("Failed to kill the job " + job.getId(), e);
        }
    }

    @Override
    protected void reschedule(Job job) {}

    @Override
    protected void replicate(Job job) {}

    @Override
    protected void killReplicas(Job job) {}

    @Override
    protected void resume(Job job) {}
}
