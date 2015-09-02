package com.netflix.exhibitor.core.backup.azure;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.Lists;
import com.netflix.exhibitor.core.backup.BackupMetaData;
import org.testng.Assert;
import org.testng.annotations.Test;

import java.io.IOException;
import java.util.List;

public class TestAzureMetaData {

    @Test
    public void testSerialize() throws IOException {
        List<BackupMetaData> metaDataList = Lists.newArrayList(new BackupMetaData("test", 1l), new BackupMetaData("test", 2l));

        ObjectMapper mapper = new ObjectMapper();

        byte[] data = mapper.writeValueAsBytes(metaDataList);

        List<BackupMetaData> deserialized = mapper.readValue(data, new TypeReference<List<BackupMetaData>>() {});

        Assert.assertEquals(deserialized.size(), 2);
    }
}
