package fr.insalyon.creatis.gasw.executor.kubernetes.config.json.properties;

import com.fasterxml.jackson.annotation.JsonProperty;

import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;

@Getter
@Setter
@NoArgsConstructor
public class KOptions {

    @JsonProperty("ttlJobInSeconds")
    private int ttlJob = 500;

    @JsonProperty("statusRetry")
    private int statusRetry = 5;

    @JsonProperty("statusRetryWaitInMillis")
    private int statusRetryWait = 2000;

    @JsonProperty("maxRetryToPush")
    private int maxRetryToPush = 5;

    @JsonProperty("timeToVolumeBeReadyInSeconds")
    private int timeToVolumeBeReady = 120;

    @JsonProperty("storageClassName")
    private String storageClassName = "";

    @JsonProperty("timeoutInMillis")
    private int timeoutInMillis = 10000;
}
