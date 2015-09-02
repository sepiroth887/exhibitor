package com.netflix.exhibitor.core.backup.azureblob;


import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.Lists;
import com.microsoft.azure.storage.CloudStorageAccount;
import com.microsoft.azure.storage.StorageException;
import com.microsoft.azure.storage.blob.CloudBlobClient;
import com.microsoft.azure.storage.blob.CloudBlobContainer;
import com.microsoft.azure.storage.blob.CloudBlockBlob;
import com.netflix.exhibitor.core.Exhibitor;
import com.netflix.exhibitor.core.activity.ActivityLog;
import com.netflix.exhibitor.core.backup.BackupConfigSpec;
import com.netflix.exhibitor.core.backup.BackupMetaData;
import com.netflix.exhibitor.core.backup.BackupProvider;
import com.netflix.exhibitor.core.backup.BackupStream;
import com.netflix.exhibitor.core.config.azure.AzureClientConfig;

import java.io.*;
import java.net.URISyntaxException;
import java.security.InvalidKeyException;
import java.util.List;
import java.util.Map;

import static com.google.common.base.Strings.isNullOrEmpty;

public class AzureBlobBackupProvider implements BackupProvider {

    private static final BackupConfigSpec CONFIG_CONTAINER = new BackupConfigSpec("container", "Container", "Azure Storage container name (created if not present)", "", BackupConfigSpec.Type.STRING);
    private static final List<BackupConfigSpec> CONFIGS = Lists.newArrayList(CONFIG_CONTAINER);

    private CloudBlobClient blobClient;
    private ObjectMapper mapper;

    public AzureBlobBackupProvider(AzureClientConfig azureClientConfig) {
        setup(azureClientConfig.getAzureAccountName(), azureClientConfig.getAzureAccountKey());
    }

    private void setup(String accountName, String key){
        try {
            String connString = String.format("DefaultEndpointsProtocol=https;AccountName=%s;AccountKey=%s", accountName, key);
            CloudStorageAccount account = CloudStorageAccount.parse(connString);
            blobClient = account.createCloudBlobClient();
            mapper = new ObjectMapper();
        } catch (URISyntaxException e) {
            throw new IllegalStateException(e);
        } catch (InvalidKeyException e) {
            throw new IllegalStateException(e);
        }
    }


    @Override
    public List<BackupConfigSpec> getConfigs() {
        return CONFIGS;
    }

    @Override
    public boolean isValidConfig(Exhibitor exhibitor, Map<String, String> configValues) {
        return configValues.containsKey(CONFIG_CONTAINER.getKey()) && !isNullOrEmpty(configValues.get(CONFIG_CONTAINER.getKey()));
    }

    @Override
    public UploadResult uploadBackup(Exhibitor exhibitor, BackupMetaData metaData, File source, Map<String, String> configValues) throws Exception {
        List<BackupMetaData> availableBackups = getAvailableBackups(exhibitor, configValues);

        if(availableBackups.contains(metaData)){
            return UploadResult.DUPLICATE;
        }

        CloudBlobContainer container = getContainer(configValues, exhibitor);
        UploadResult result = UploadResult.FAILED;
        try{
            exhibitor.getLog().add(ActivityLog.Type.INFO, String.format("Attempting backup upload for: %s-%s from file: %s", metaData.getName(), metaData.getModifiedDate(), source.getAbsolutePath()));
            CloudBlockBlob blob = container.getBlockBlobReference(String.format("%s-%s", metaData.getName(), metaData.getModifiedDate()));
            if(blob.exists()){
                exhibitor.getLog().add(ActivityLog.Type.INFO, "Blob already exists. Overwriting");
            }

            List<BackupMetaData> toRemove = Lists.newArrayList();
            for(BackupMetaData meta : availableBackups){
                if (meta.getName().equalsIgnoreCase(metaData.getName())){
                    toRemove.add(meta);
                    container.getBlockBlobReference(String.format("%s-%s", meta.getName(), meta.getModifiedDate())).delete();
                    result = UploadResult.REPLACED_OLD_VERSION;
                }
            }

            blob.uploadFromFile(source.getAbsolutePath());

            for(BackupMetaData meta : toRemove){
                availableBackups.remove(meta);
            }
            availableBackups.add(metaData);
            uploadMetadata(availableBackups, container.getBlockBlobReference("metadata"), exhibitor.getLog());

            result = UploadResult.SUCCEEDED;

        }catch (Exception ex){
            exhibitor.getLog().add(ActivityLog.Type.ERROR,"Failed to upload backup", ex);
        }

        return result;
    }

    private void uploadMetadata(List<BackupMetaData> data, CloudBlockBlob blob, ActivityLog log) throws Exception{
        byte[] buf = mapper.writeValueAsBytes(data);
        ByteArrayInputStream inputStream = new ByteArrayInputStream(buf);

        try {
            log.add(ActivityLog.Type.INFO, "Updating metadata");
            blob.upload(inputStream,buf.length);
        } finally {
            inputStream.close();
        }
    }

    @Override
    public List<BackupMetaData> getAvailableBackups(Exhibitor exhibitor, Map<String, String> configValues) throws Exception {
        List<BackupMetaData> returnData = Lists.newArrayList();
        try {
            CloudBlobContainer containerRef = getContainer(configValues, exhibitor);
            CloudBlockBlob metaData = containerRef.getBlockBlobReference("metadata");
            if (!metaData.exists()) {
                exhibitor.getLog().add(ActivityLog.Type.INFO, "No metadata exists yet.");
                return returnData;
            }

            returnData = mapper.readValue(metaData.downloadText(), new TypeReference<List<BackupMetaData>>() {});
        } catch (Exception ex) {
            exhibitor.getLog().add(ActivityLog.Type.ERROR, "Failed to download metadata", ex);
        }

        return returnData;

    }

    private CloudBlobContainer getContainer(Map<String, String> configValues, Exhibitor exhibitor) {
        try {
            String container = configValues.get(CONFIG_CONTAINER.getKey());
            exhibitor.getLog().add(ActivityLog.Type.INFO, "Attempting to find container: "+ container);
            CloudBlobContainer containerRef = blobClient.getContainerReference(container);

            containerRef.createIfNotExists();

            return containerRef;
        } catch (URISyntaxException e) {
            exhibitor.getLog().add(ActivityLog.Type.ERROR, "Invalid URI Syntax", e);
        } catch (StorageException e) {
            exhibitor.getLog().add(ActivityLog.Type.ERROR, "Azure Storage Failure", e);
        }
        throw new IllegalStateException("Failed to fetch container");
    }

    @Override
    public BackupStream getBackupStream(final Exhibitor exhibitor, final BackupMetaData metaData, Map<String, String> configValues) throws Exception {
        final CloudBlobContainer container = getContainer(configValues, exhibitor);

        exhibitor.getLog().add(ActivityLog.Type.INFO, String.format("Fetching inputstream from Azure for: %s-%s", metaData.getName(), metaData.getModifiedDate()));
        final CloudBlockBlob blob = container.getBlockBlobReference(String.format("%s-%s", metaData.getName(), metaData.getModifiedDate()));

        ByteArrayOutputStream out = new ByteArrayOutputStream();
        blob.download(out);

        byte[] buf = out.toByteArray();

        final InputStream inputStream = new ByteArrayInputStream(buf);
        out.close();
        if (blob.exists()) {
            return new BackupStream() {
                @Override
                public InputStream getStream() {
                    return inputStream;
                }

                @Override
                public void close() throws IOException {
                    inputStream.close();
                }
            };
        }
        throw new IllegalStateException("No such blob");
    }

    @Override
    public void deleteBackup(Exhibitor exhibitor, BackupMetaData backup, Map<String, String> configValues) throws Exception {
        CloudBlobContainer container = getContainer(configValues, exhibitor);

        CloudBlockBlob blob = container.getBlockBlobReference(String.format("%s-%s",backup.getName(), backup.getModifiedDate()));
        if(blob.exists()){
            blob.delete();
            List<BackupMetaData> metaDatas = getAvailableBackups(exhibitor, configValues);
            metaDatas.remove(backup);
            uploadMetadata(metaDatas, container.getBlockBlobReference("metadata"), exhibitor.getLog());
        }
    }

    @Override
    public void downloadBackup(Exhibitor exhibitor, BackupMetaData backup, OutputStream destination, Map<String, String> configValues) throws Exception {
        CloudBlobContainer container = getContainer(configValues, exhibitor);

        CloudBlockBlob blob = container.getBlockBlobReference(String.format("%s-%s", backup.getName(), backup.getModifiedDate()));
        if (blob.exists()) {
            blob.download(destination);
        }
    }
}
