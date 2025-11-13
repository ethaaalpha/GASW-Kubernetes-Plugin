package fr.insalyon.creatis.gasw.executor.kubernetes;

import java.util.stream.Collectors;

import fr.insalyon.creatis.gasw.GaswConstants;
import fr.insalyon.creatis.gasw.GaswException;
import fr.insalyon.creatis.gasw.GaswInput;
import fr.insalyon.creatis.gasw.execution.GaswSubmit;
import fr.insalyon.creatis.gasw.executor.kubernetes.internals.KManager;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class KSubmit extends GaswSubmit {

    final private KManager manager;
    final private KMonitor monitor;

    public KSubmit(final GaswInput gaswInput, final KMinorStatusGenerator minorStatusServiceGenerator, final KManager manager, final KMonitor monitor) throws GaswException {
        super(gaswInput, minorStatusServiceGenerator);
        this.manager = manager;
        this.monitor = monitor;
        scriptName = generateScript();
    }

    @Override
    public String submit() throws GaswException {
        final String cmd = "bash " + GaswConstants.SCRIPT_ROOT + "/" + scriptName;
        final String jobID = scriptName.substring(0, scriptName.lastIndexOf("."));
        final String params = gaswInput.getParameters().stream()
                .collect(Collectors.joining(" "));

        if ( ! monitor.isAlive()) {
            monitor.start();
        }
        monitor.add(jobID, gaswInput.getExecutableName(), jobID, params.toString());
        manager.submitter(cmd, "ethaaalpha/podman-boutiques:latest", jobID);
        log.info("K8s Executor Job ID: " + jobID);

        return jobID;
    }
}
