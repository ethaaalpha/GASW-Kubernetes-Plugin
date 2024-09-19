package fr.insalyon.creatis.gasw.executor.kubernetes.config.json.properties;

import java.util.ArrayList;
import java.util.List;

import com.fasterxml.jackson.annotation.JsonProperty;

import lombok.Getter;

@Getter
public class KConfig {
    
    @JsonProperty(value = "k8sAddress", required = true)
    private String k8sAddress;

    @JsonProperty(value = "k8sToken")
    private String k8sToken;

    @JsonProperty(value = "k8sKubeConfig")
    private String k8sKubeConfig;

    @JsonProperty(value = "k8sNamespace", required = true)
    private String k8sNamespace;

    @JsonProperty(value = "nfsAddress", required = true)
    private String nfsAddress;

    @JsonProperty(value = "nfsPath", required = true)
    private String nfsPath;

    @JsonProperty("options")
    private KOptions options;

    @JsonProperty("volumes")
    private List<KVolumeData> volumes = new ArrayList<KVolumeData>();
}