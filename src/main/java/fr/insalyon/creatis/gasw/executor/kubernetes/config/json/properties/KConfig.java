package fr.insalyon.creatis.gasw.executor.kubernetes.config.json.properties;

import java.util.List;

import com.fasterxml.jackson.annotation.JsonProperty;

import lombok.Getter;
import lombok.NoArgsConstructor;

@Getter
@NoArgsConstructor
public class KConfig {

    @JsonProperty(value = "k8sConfig")
    private String k8sKubeConfig;

    @JsonProperty(value = "k8sNamespace", required = true)
    private String k8sNamespace;

    @JsonProperty(value = "nfsAddress", required = true)
    private String nfsAddress;

    @JsonProperty(value = "nfsPath", required = true)
    private String nfsPath;

    @JsonProperty(value = "workflowsLocation", required = true)
    private String workflowsLocation;

    @JsonProperty("options")
    private KOptions options;

    @JsonProperty(value = "volumes", required = true)
    private List<KVolumeData> volumes;
}
