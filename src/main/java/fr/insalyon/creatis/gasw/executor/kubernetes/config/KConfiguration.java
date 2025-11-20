package fr.insalyon.creatis.gasw.executor.kubernetes.config;

import java.io.IOException;

import fr.insalyon.creatis.gasw.GaswException;
import fr.insalyon.creatis.gasw.executor.kubernetes.config.json.ConfigBuilder;
import fr.insalyon.creatis.gasw.executor.kubernetes.config.json.properties.KConfig;
import io.kubernetes.client.openapi.ApiClient;
import io.kubernetes.client.openapi.ApiException;
import io.kubernetes.client.openapi.Configuration;
import io.kubernetes.client.openapi.apis.BatchV1Api;
import io.kubernetes.client.openapi.apis.CoreV1Api;
import io.kubernetes.client.openapi.apis.StorageV1Api;
import io.kubernetes.client.openapi.apis.VersionApi;
import io.kubernetes.client.openapi.models.VersionInfo;
import io.kubernetes.client.util.Config;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.extern.slf4j.Slf4j;

@Getter
@Slf4j
@NoArgsConstructor
public class KConfiguration {

    private static KConfiguration instance;

    // K8s objects
    private CoreV1Api coreApi;
    private BatchV1Api batchApi;
    private StorageV1Api storageApi;

    // K8s configuration
    private KConfig config;

    public static KConfiguration getInstance() {
        if (instance == null) {
            instance = new KConfiguration();
        }
        return instance;
    }

    public void init(final String configurationFile) throws GaswException {
        loadConfiguration(configurationFile);
        createLocalClient();
    }

    private void loadConfiguration(final String path) throws GaswException {
        final ConfigBuilder configBuilder = new ConfigBuilder(path);
        config = configBuilder.get();

        if (config == null) {
            throw new GaswException("Client creation failed");
        }
    }

    private void defineApis(final ApiClient client) {
        coreApi = new CoreV1Api(client);
        batchApi = new BatchV1Api(client);
        storageApi = new StorageV1Api(client);

        log.info("Apis were defined successffully");
    }

    /**
     * To use when use .kube local config, useful for debug and develop
     */
    private void createLocalClient() throws GaswException {
        try {
            final int timeout = config.getOptions().getTimeoutInMillis();
            final ApiClient client = Config.fromConfig(config.getK8sKubeConfig());

            client.setReadTimeout(timeout);
            client.setWriteTimeout(timeout);
            client.setConnectTimeout(timeout);

            Configuration.setDefaultApiClient(client);
            defineApis(client);

            // check a VERSION request to verify connection
            VersionInfo info = new VersionApi(client).getCode().execute();
            log.info("Connected to k8s cluster with version: {}", info.getGitVersion());

        } catch (IOException | ApiException e) {
            log.error("Error while creating local client (check the connection to the k8s cluster)", e);
            throw new GaswException("Client creation failed");
        }
    }
}
