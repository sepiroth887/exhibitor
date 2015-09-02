package com.netflix.exhibitor.core.config.azure;

public class AzureClientConfig {

    private final String azureAccountName;
    private final String azureAccountKey;
    private final String configContainer;

    public AzureClientConfig(String azureAccountName, String azureAccountKey, String configContainer) {
        this.azureAccountName = azureAccountName;
        this.azureAccountKey = azureAccountKey;
        this.configContainer = configContainer;
    }

    public String getAzureAccountName() {
        return azureAccountName;
    }

    public String getAzureAccountKey() {
        return azureAccountKey;
    }

    public String getConfigContainer() {
        return configContainer;
    }
}
