package com.netflix.exhibitor.core.config.azure;

public class AzureClientConfig {

    private final String azureAccountName;
    private final String azureAccountKey;
    private final String configContainer;
    private final Boolean isFullBackup;

    public AzureClientConfig(String azureAccountName, String azureAccountKey, String configContainer, Boolean isFullBackup) {
        this.azureAccountName = azureAccountName;
        this.azureAccountKey = azureAccountKey;
        this.configContainer = configContainer;
        this.isFullBackup = isFullBackup;
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

    public Boolean getIsFullBackup() {
        return isFullBackup;
    }
}
