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
import com.netflix.exhibitor.core.config.IntConfigs;
import com.netflix.exhibitor.core.config.StringConfigs;
import com.netflix.exhibitor.core.config.azure.AzureClientConfig;
import org.apache.commons.compress.archivers.ArchiveEntry;
import org.apache.commons.compress.archivers.ArchiveStreamFactory;
import org.apache.commons.compress.archivers.tar.TarArchiveEntry;
import org.apache.commons.compress.archivers.tar.TarArchiveInputStream;
import org.apache.commons.compress.archivers.tar.TarArchiveOutputStream;
import org.apache.commons.compress.utils.IOUtils;
import org.joda.time.Duration;
import org.slf4j.Logger;

import java.io.*;
import java.net.URISyntaxException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.security.InvalidKeyException;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Executor;
import java.util.concurrent.Executors;

import static com.google.common.base.Strings.isNullOrEmpty;

public class AzureBlobBackupProvider implements BackupProvider {

    private static final BackupConfigSpec BACKUP_CONTAINER = new BackupConfigSpec("container", "Container", "Azure Storage container name (created if not present)", "", BackupConfigSpec.Type.STRING);
    private static final BackupConfigSpec FULL_BACKUP_FACTOR = new BackupConfigSpec("backup_factor", "Full Backup Factor", "Factor multiplied by Backup frequency to determine full backup trigger", "", BackupConfigSpec.Type.STRING);
    private static final BackupConfigSpec FULL_BACKUP_RETENTION = new BackupConfigSpec("backup_retention", "Full Backup Retention", "Retention duration of full backups in ISO8601 Seconds notation(e.g. 7 days 3 hour 4min : PT615840S)", "PT604800S", BackupConfigSpec.Type.STRING);
    private static final List<BackupConfigSpec> CONFIGS = Lists.newArrayList(BACKUP_CONTAINER, FULL_BACKUP_FACTOR, FULL_BACKUP_RETENTION);

    private static final String FULL_BACKUP_PREFIX = "full-%s-%d";
    private CloudBlobClient blobClient;
    private ObjectMapper mapper;

    private boolean isFullBackup = false;
    private long lastFullBackup = 0;
    private final Executor fullBackupExecutor = Executors.newSingleThreadExecutor();

    public AzureBlobBackupProvider(AzureClientConfig azureClientConfig) {
        setup(azureClientConfig.getAzureAccountName(), azureClientConfig.getAzureAccountKey());
        isFullBackup = azureClientConfig.getIsFullBackup();
    }

    private void setup(String accountName, String key) {
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
        return configValues.containsKey(BACKUP_CONTAINER.getKey()) && !isNullOrEmpty(configValues.get(BACKUP_CONTAINER.getKey()))
                && configValues.containsKey(FULL_BACKUP_FACTOR.getKey()) && verifyBackupFactor(configValues.get(FULL_BACKUP_FACTOR.getKey())) && verifyRetentionConfig(configValues);
    }

    private boolean verifyRetentionConfig(Map<String, String> configValues) {
        if (configValues.containsKey(FULL_BACKUP_RETENTION.getKey())) {
            try {
                Duration.parse(configValues.get(FULL_BACKUP_RETENTION.getKey()));
            } catch (Exception ex) {
                // Invalid duration
                return false;
            }
        }
        return true;
    }

    private boolean verifyBackupFactor(String duration) {
        try {
            return Integer.parseInt(duration) >= 0;
        } catch (Exception ex) {
            // failure to parse
            return false;
        }
    }

    private void handleRetention(Duration retention, String backupContainerPrefix, ActivityLog log) {
        String backupPrefix = String.format("full-%s", backupContainerPrefix);
        Duration timestamp;
        Duration now = Duration.millis(System.currentTimeMillis());
        for (CloudBlobContainer containerRef : blobClient.listContainers(backupPrefix)) {
            try {
                timestamp = Duration.millis(Long.parseLong(containerRef.getName().replace(backupPrefix + "-", "")));
                if (now.minus(timestamp).isLongerThan(retention)){
                    log.add(ActivityLog.Type.INFO, "Found expired backup: "+ timestamp.toString());
                    containerRef.deleteIfExists();
                }

            } catch (Exception ex) {
                log.add(ActivityLog.Type.ERROR, "Failed to parse timestamp during retention check.", ex);
            }
        }
    }

    @Override
    public UploadResult uploadBackup(Exhibitor exhibitor, BackupMetaData metaData, File source, Map<String, String> configValues) throws Exception {
        List<BackupMetaData> availableBackups = getAvailableBackups(exhibitor, configValues);

        if (availableBackups.contains(metaData)) {
            return UploadResult.DUPLICATE;
        }

        CloudBlobContainer container = getContainer(configValues, exhibitor.getLog());
        UploadResult result = UploadResult.FAILED;
        try {
            exhibitor.getLog().add(ActivityLog.Type.INFO, String.format("Attempting backup upload for: %s-%s from file: %s", metaData.getName(), metaData.getModifiedDate(), source.getAbsolutePath()));
            CloudBlockBlob blob = container.getBlockBlobReference(String.format("%s-%s", metaData.getName(), metaData.getModifiedDate()));
            if (blob.exists()) {
                exhibitor.getLog().add(ActivityLog.Type.INFO, "Blob already exists. Overwriting");
            }

            List<BackupMetaData> toRemove = Lists.newArrayList();
            for (BackupMetaData meta : availableBackups) {
                if (meta.getName().equalsIgnoreCase(metaData.getName())) {
                    toRemove.add(meta);
                    container.getBlockBlobReference(String.format("%s-%s", meta.getName(), meta.getModifiedDate())).delete();
                    result = UploadResult.REPLACED_OLD_VERSION;
                }
            }

            blob.uploadFromFile(source.getAbsolutePath());

            for (BackupMetaData meta : toRemove) {
                availableBackups.remove(meta);
            }
            availableBackups.add(metaData);
            uploadMetadata(availableBackups, container.getBlockBlobReference("metadata"), exhibitor.getLog());

            triggerFullBackup(exhibitor, configValues);
            String retentionStr = configValues.containsKey(FULL_BACKUP_RETENTION.getKey()) ?
                    configValues.get(FULL_BACKUP_RETENTION.getKey()) : FULL_BACKUP_RETENTION.getDefaultValue();
            handleRetention(Duration.parse(retentionStr), configValues.get(BACKUP_CONTAINER.getKey()), exhibitor.getLog());

            result = UploadResult.SUCCEEDED;
        } catch (Exception ex) {
            exhibitor.getLog().add(ActivityLog.Type.ERROR, "Failed to upload backup", ex);
        }

        return result;
    }

    private void triggerFullBackup(final Exhibitor exhibitor, final Map<String, String> configValues) {
        if (!isFullBackup) {
            return;
        }

        int backupFactor = Integer.parseInt(configValues.get(FULL_BACKUP_FACTOR.getKey()));
        long backupMS = exhibitor.getConfigManager().getConfig().getInt(IntConfigs.BACKUP_PERIOD_MS);
        final ActivityLog log = exhibitor.getLog();

        final long current = System.currentTimeMillis();
        if (current - (backupFactor * backupMS) > lastFullBackup) {
            log.add(ActivityLog.Type.INFO, "Triggering full backup.");
            fullBackupExecutor.execute(new Runnable() {
                @Override
                public void run() {
                    try {
                        String snapshotDir = exhibitor.getConfigManager().getConfig().getString(StringConfigs.ZOOKEEPER_DATA_DIRECTORY);
                        String logDir = exhibitor.getConfigManager().getConfig().getString(StringConfigs.ZOOKEEPER_LOG_DIRECTORY);
                        String containerName = String.format(FULL_BACKUP_PREFIX, configValues.get(BACKUP_CONTAINER.getKey()), current);

                        CloudBlobContainer container = blobClient.getContainerReference(containerName);
                        container.createIfNotExists();

                        log.add(ActivityLog.Type.INFO, "Uploading snapshot data");
                        uploadCompressedDir(snapshotDir, "snapshot", container, log);
                        log.add(ActivityLog.Type.INFO, "Uploading log data");
                        uploadCompressedDir(logDir, "log", container, log);
                        lastFullBackup = current;
                    } catch (Exception e) {
                        log.add(ActivityLog.Type.ERROR, "Failed to run full backup", e);
                    }

                }
            });
        }
    }

    private void uploadCompressedDir(String dir, String blobName, CloudBlobContainer container, ActivityLog log) throws Exception {
        CloudBlockBlob blob = container.getBlockBlobReference(blobName);

        String archiveFile = String.format("/tmp/%s.tar", blobName);
        FileOutputStream out = new FileOutputStream(archiveFile);
        TarArchiveOutputStream outputStream = (TarArchiveOutputStream) new ArchiveStreamFactory().createArchiveOutputStream("tar", out);

        Path dataDir = Paths.get(dir, "version-2");
        boolean hasFiles = false;
        if (Files.exists(dataDir) && Files.isDirectory(dataDir)) {
            for (File file : dataDir.toFile().listFiles()) {
                if (file.getName().contains(blobName)) {
                    log.add(ActivityLog.Type.INFO, "Adding file to archive: " + file.getName());
                    hasFiles = true;
                    TarArchiveEntry entry = new TarArchiveEntry(file, file.getName());
                    entry.setSize(file.length());
                    outputStream.putArchiveEntry(entry);
                    IOUtils.copy(new FileInputStream(file), outputStream);
                    outputStream.closeArchiveEntry();
                }
            }
        }
        outputStream.finish();
        outputStream.close();

        if (!hasFiles) {
            log.add(ActivityLog.Type.INFO, "Archive has no files, skipping upload");
            if (!new File(archiveFile).delete()) {
                log.add(ActivityLog.Type.ERROR, "Failed to delete tmp archive file: " + archiveFile);
            }
            return;
        }

        log.add(ActivityLog.Type.INFO, "Compressed dir " + dir + ". Starting upload");
        blob.uploadFromFile(archiveFile);
        log.add(ActivityLog.Type.INFO, "Upload completed");
        if (!new File(archiveFile).delete()) {
            log.add(ActivityLog.Type.ERROR, "Failed to remove temp backup file");
        }
    }


    private void uploadMetadata(List<BackupMetaData> data, CloudBlockBlob blob, ActivityLog log) throws Exception {
        byte[] buf = mapper.writeValueAsBytes(data);
        ByteArrayInputStream inputStream = new ByteArrayInputStream(buf);

        try {
            log.add(ActivityLog.Type.INFO, "Updating metadata");
            blob.upload(inputStream, buf.length);
        } finally {
            inputStream.close();
        }
    }

    @Override
    public List<BackupMetaData> getAvailableBackups(Exhibitor exhibitor, Map<String, String> configValues) throws Exception {
        List<BackupMetaData> returnData = Lists.newArrayList();
        try {
            CloudBlobContainer containerRef = getContainer(configValues, exhibitor.getLog());
            CloudBlockBlob metaData = containerRef.getBlockBlobReference("metadata");
            if (!metaData.exists()) {
                exhibitor.getLog().add(ActivityLog.Type.INFO, "No metadata exists yet.");
                return returnData;
            }

            returnData = mapper.readValue(metaData.downloadText(), new TypeReference<List<BackupMetaData>>() {
            });
        } catch (Exception ex) {
            exhibitor.getLog().add(ActivityLog.Type.ERROR, "Failed to download metadata", ex);
        }

        return returnData;

    }

    private CloudBlobContainer getContainer(Map<String, String> configValues, ActivityLog log) {
        try {
            String container = configValues.get(BACKUP_CONTAINER.getKey());
            log.add(ActivityLog.Type.INFO, "Attempting to find container: " + container);
            CloudBlobContainer containerRef = blobClient.getContainerReference(container);

            containerRef.createIfNotExists();

            return containerRef;
        } catch (URISyntaxException e) {
            log.add(ActivityLog.Type.ERROR, "Invalid URI Syntax", e);
        } catch (StorageException e) {
            log.add(ActivityLog.Type.ERROR, "Azure Storage Failure", e);
        }
        throw new IllegalStateException("Failed to fetch container");
    }

    @Override
    public BackupStream getBackupStream(final Exhibitor exhibitor, final BackupMetaData metaData, Map<String, String> configValues) throws Exception {
        final CloudBlobContainer container = getContainer(configValues, exhibitor.getLog());

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
        CloudBlobContainer container = getContainer(configValues, exhibitor.getLog());

        CloudBlockBlob blob = container.getBlockBlobReference(String.format("%s-%s", backup.getName(), backup.getModifiedDate()));
        if (blob.exists()) {
            blob.delete();
            List<BackupMetaData> metaDatas = getAvailableBackups(exhibitor, configValues);
            metaDatas.remove(backup);
            uploadMetadata(metaDatas, container.getBlockBlobReference("metadata"), exhibitor.getLog());
        }
    }

    @Override
    public void downloadBackup(Exhibitor exhibitor, BackupMetaData backup, OutputStream destination, Map<String, String> configValues) throws Exception {
        CloudBlobContainer container = getContainer(configValues, exhibitor.getLog());

        CloudBlockBlob blob = container.getBlockBlobReference(String.format("%s-%s", backup.getName(), backup.getModifiedDate()));
        if (blob.exists()) {
            blob.download(destination);
        }
    }

    public String findLatestSnapshot(String containerPrefix, Logger log) {
        String fullPrefix = "full-" + containerPrefix;

        long latest = 0;
        log.info("Retrieving existing container for prefix: " + fullPrefix);
        for (CloudBlobContainer container : blobClient.listContainers(fullPrefix)) {
            long timestamp = Long.parseLong(container.getName().replace(fullPrefix, ""));
            if (timestamp > latest) {
                latest = timestamp;
            }
        }
        log.info("Found latest snapshot: " + latest);
        return Long.toString(latest);
    }

    public void restoreAndExit(String snapshot, String containerPrefix, String logDir, String snapshotDir, Logger log) throws Exception {
        log.info("Starting recovery for snapshot: " + snapshot);
        CloudBlobContainer containerRef = blobClient.getContainerReference(String.format("full-%s-%s", containerPrefix, snapshot));

        CloudBlockBlob logBlob = containerRef.getBlockBlobReference("log");
        CloudBlockBlob snapshotBlob = containerRef.getBlockBlobReference("snapshot");

        if (!logBlob.exists()) {
            log.error("No Log blob found. Exiting.");
            System.exit(1);
        }

        if (snapshotBlob.exists()) {
            extractToDir(snapshotDir, snapshotBlob, log);
        }

        extractToDir(logDir, logBlob, log);
        log.info("Recovery done. Exiting");
        System.exit(0);
    }

    private void extractToDir(String snapshotDir, CloudBlockBlob snapshotBlob, Logger log) throws Exception {
        String tarFile = String.format("/tmp/%s", snapshotBlob.getName());
        log.info("Downloading " + snapshotBlob.getName());
        snapshotBlob.downloadToFile(tarFile);
        log.info("Downloading completed. Extracting to " + snapshotDir);

        TarArchiveInputStream tarStream = (TarArchiveInputStream) new ArchiveStreamFactory()
                .createArchiveInputStream("tar", new FileInputStream(tarFile));

        ArchiveEntry entry;
        while ((entry = tarStream.getNextTarEntry()) != null) {
            OutputStream outstream = new FileOutputStream(String.format("%s/%s", snapshotDir, entry.getName()));

            if (entry.isDirectory()) {
                continue;
            }

            IOUtils.copy(tarStream, outstream);

            outstream.close();
        }

        log.info("Finished extracting " + tarFile);
        tarStream.close();
        if (!new File(tarFile).delete()) {
            log.warn("Failed to delete temporary archive: " + tarFile);
        }
    }
}
