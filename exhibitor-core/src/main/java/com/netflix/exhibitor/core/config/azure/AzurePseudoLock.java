package com.netflix.exhibitor.core.config.azure;

import com.google.common.collect.Lists;
import com.microsoft.azure.storage.blob.CloudBlobContainer;
import com.microsoft.azure.storage.blob.CloudBlockBlob;
import com.microsoft.azure.storage.blob.ListBlobItem;
import com.netflix.exhibitor.core.config.PseudoLockBase;

import java.util.List;
import java.util.concurrent.TimeUnit;

public class AzurePseudoLock extends PseudoLockBase {

    public static final String LOCK_NAME_PREFIX = "lock";
    private final CloudBlobContainer container;

    public AzurePseudoLock(CloudBlobContainer container){
        super(LOCK_NAME_PREFIX, (int) TimeUnit.MINUTES.toMillis(15), 1000);
        this.container = container;
    }


    // Azure blob names have some restrictions
    private String toAzureBlobName(String in){
        return in.replaceAll("_", "-");
    }

    private String fromAzureBlobName(String in){
        return in.replaceAll("-", "_");
    }

    @Override
    protected void createFile(String key, byte[] contents) throws Exception {
        CloudBlockBlob blob = container.getBlockBlobReference(toAzureBlobName(key));
        blob.uploadFromByteArray(contents, 0, contents.length);
    }

    @Override
    protected void deleteFile(String key) throws Exception {
        CloudBlockBlob blob = container.getBlockBlobReference(toAzureBlobName(key));
        blob.deleteIfExists();
    }

    @Override
    protected List<String> getFileNames(String lockPrefix) throws Exception {
        List<String> blobs = Lists.newArrayList();
        for (ListBlobItem item : container.listBlobs(lockPrefix)){
            String blobUri = item.getUri().toString();
            String blobName = blobUri.substring(blobUri.lastIndexOf("/")+1, blobUri.length());
            blobs.add(fromAzureBlobName(blobName));
        }
        return blobs;
    }
}
