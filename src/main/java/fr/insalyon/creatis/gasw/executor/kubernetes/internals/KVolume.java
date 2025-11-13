package fr.insalyon.creatis.gasw.executor.kubernetes.internals;

import java.util.Arrays;
import java.util.Map;

import fr.insalyon.creatis.gasw.executor.kubernetes.config.KConfiguration;
import fr.insalyon.creatis.gasw.executor.kubernetes.config.KConstants;
import fr.insalyon.creatis.gasw.executor.kubernetes.config.json.properties.KVolumeData;
import io.kubernetes.client.custom.Quantity;
import io.kubernetes.client.openapi.ApiException;
import io.kubernetes.client.openapi.apis.CoreV1Api;
import io.kubernetes.client.openapi.models.V1NFSVolumeSource;
import io.kubernetes.client.openapi.models.V1ObjectMeta;
import io.kubernetes.client.openapi.models.V1PersistentVolume;
import io.kubernetes.client.openapi.models.V1PersistentVolumeClaim;
import io.kubernetes.client.openapi.models.V1PersistentVolumeClaimSpec;
import io.kubernetes.client.openapi.models.V1PersistentVolumeSpec;
import io.kubernetes.client.openapi.models.V1VolumeResourceRequirements;
import lombok.Getter;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;

@Slf4j
@RequiredArgsConstructor
public class KVolume {

    private final KConfiguration conf;

    private V1PersistentVolume pv;
    private V1PersistentVolumeClaim pvc;

    @Getter
    final private KVolumeData data;

    public String getClaimName() {
        return getKubernetesName() + "-claim";
    }

    public String getNfsSubMountPath() {
        return conf.getConfig().getNfsPath() + data.getNfsFolder() + "/";
    }

    /**
     * This function return in lowercase to conform to RFC 1123 (volume name)
     */
    public String getKubernetesName() {
        return data.getName().toLowerCase();
    }

    public void createPV() throws ApiException {
        log.info("Volume creation submitted for " + getKubernetesName());
        pv = new V1PersistentVolume()
                .metadata(new V1ObjectMeta().name(getKubernetesName()))
                .spec(new V1PersistentVolumeSpec()
                        .accessModes(Arrays.asList(data.getAccessModes()))
                        .capacity(Map.of("storage", new Quantity("1Gi")))
                        .nfs(new V1NFSVolumeSource()
                                .path(getNfsSubMountPath())
                                .server(conf.getConfig().getNfsAddress())));
        conf.getCoreApi().createPersistentVolume(pv).execute();
    }

    public void createPVC() throws ApiException {
        log.info("VolumeClaim creation submitted for " + getClaimName());
        pvc = new V1PersistentVolumeClaim()
                .metadata(new V1ObjectMeta().name(getClaimName()).namespace(conf.getConfig().getK8sNamespace()))
                .spec(new V1PersistentVolumeClaimSpec()
                        .storageClassName(conf.getConfig().getOptions().getStorageClassName())
                        .resources(new V1VolumeResourceRequirements()
                                .requests(Map.of("storage", new Quantity("1Gi"))))
                        .addAccessModesItem(data.getAccessModes())
                        .volumeName(getKubernetesName()));

        conf.getCoreApi().createNamespacedPersistentVolumeClaim(conf.getConfig().getK8sNamespace(), pvc).execute();
    }

    public void deletePVC() throws ApiException {
        final CoreV1Api api = conf.getCoreApi();

        if (pvc != null) {
            api.deleteNamespacedPersistentVolumeClaim(getClaimName(), conf.getConfig().getK8sNamespace()).execute();
        }
    }

    public void deletePV() throws ApiException {
        final CoreV1Api api = conf.getCoreApi();

        if (pv != null) {
            api.deletePersistentVolume(getKubernetesName()).execute();
        }
    }

    /**
     * Check if the volume pv and pvc is bounded (the phase status)
     * 
     * @return
     */
    public boolean isAvailable() {
        final CoreV1Api api = conf.getCoreApi();

        try {
            final V1PersistentVolume requestPv = api.readPersistentVolume(getKubernetesName()).execute();
            final V1PersistentVolumeClaim requestPvc = api
                    .readNamespacedPersistentVolumeClaim(getClaimName(), conf.getConfig().getK8sNamespace()).execute();

            return "Bound".equalsIgnoreCase(requestPv.getStatus().getPhase())
                    && "Bound".equalsIgnoreCase(requestPvc.getStatus().getPhase());

        } catch (ApiException e) {
            log.error("Failed to check if the volume PV and PVC were available !", e);
            return false;
        }
    }

    public static KVolume retrieve(final KVolumeData data) {
        final KConfiguration conf = KConfiguration.getInstance();
        final CoreV1Api api = conf.getCoreApi();

        try {
            final KVolume volume = new KVolume(conf, data);
            final V1PersistentVolume pv = api.readPersistentVolume(volume.getKubernetesName()).execute();
            final V1PersistentVolumeClaim pvc = api
                    .readNamespacedPersistentVolumeClaim(volume.getClaimName(), conf.getConfig().getK8sNamespace())
                    .execute();

            volume.pv = pv;
            volume.pvc = pvc;
            return volume;
        } catch (ApiException e) {
            if (e.getCode() != KConstants.NOT_FOUND_CODE) {
                log.error("Failed to retrieve the volume " + data.getName() + " exist (pv and pvc)");
            }
            return null;
        }
    }
}