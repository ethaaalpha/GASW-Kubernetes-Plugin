package fr.insalyon.creatis.gasw.executor.kubernetes;

import java.util.ArrayList;
import java.util.List;

import fr.insalyon.creatis.gasw.GaswConfiguration;
import fr.insalyon.creatis.gasw.GaswException;
import fr.insalyon.creatis.gasw.GaswInput;
import fr.insalyon.creatis.gasw.executor.kubernetes.config.KConfiguration;
import fr.insalyon.creatis.gasw.executor.kubernetes.config.KConstants;
import fr.insalyon.creatis.gasw.executor.kubernetes.internals.KManager;
import fr.insalyon.creatis.gasw.plugin.ExecutorPlugin;
import lombok.NoArgsConstructor;
import net.xeoh.plugins.base.annotations.PluginImplementation;

@PluginImplementation
@NoArgsConstructor
public class KExecutor implements ExecutorPlugin {

    private KSubmit submitter;
    private KManager manager;
    private KMonitor monitor;
    private boolean loaded = false;

    @Override
    public String getName() {
        return KConstants.EXECUTOR_NAME;
    }

    @Override
    public void load(final GaswInput gaswInput) throws GaswException {
        if ( ! loaded) {
            final KConfiguration conf = KConfiguration.getInstance();

            conf.init(KConstants.pluginConfig);
            manager = new KManager(GaswConfiguration.getInstance().getSimulationID());
            manager.init();

            monitor = new KMonitor();
            monitor.setManager(manager);
            loaded = true;
        }
        submitter = new KSubmit(gaswInput, new KMinorStatusGenerator(), manager, monitor);
    }

    @Override
    public List<Class> getPersistentClasses() throws GaswException {
        return new ArrayList<>();
    }

    @Override
    public String submit() throws GaswException {
        return submitter.submit();
    }

    @Override
    public void terminate(boolean force) throws GaswException {
        // Plugin
        manager.destroy();

        // Gasw
        monitor.finish();
    }
}
