package com.gojek.mc2pinot.io.oss;

import com.aliyun.oss.OSS;
import com.aliyun.oss.model.ListObjectsRequest;
import com.aliyun.oss.model.OSSObject;
import com.aliyun.oss.model.OSSObjectSummary;
import com.aliyun.oss.model.ObjectListing;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
class OSSReaderTest {

    @Mock
    private OSS ossClient;

    @Mock
    private ObjectListing objectListing;

    @Mock
    private OSSObject ossObject;

    private OSSReader ossReader;

    @BeforeEach
    void setUp() {
        ossReader = new OSSReader(ossClient, "oss://my-bucket/data/prefix");
    }

    @AfterEach
    void tearDown() throws IOException {
        ossReader.close();
    }

    @Test
    void shouldDownloadFilesFromOSS() throws IOException {
        OSSObjectSummary summary = new OSSObjectSummary();
        summary.setKey("data/prefix/part-00000.json");

        when(objectListing.getObjectSummaries()).thenReturn(List.of(summary));
        when(objectListing.isTruncated()).thenReturn(false);
        when(ossClient.listObjects(any(ListObjectsRequest.class))).thenReturn(objectListing);

        byte[] content = "{\"id\":1}".getBytes(StandardCharsets.UTF_8);
        when(ossObject.getObjectContent()).thenReturn(new ByteArrayInputStream(content));
        when(ossClient.getObject("my-bucket", "data/prefix/part-00000.json")).thenReturn(ossObject);

        List<Path> files = ossReader.read();

        assertEquals(1, files.size());
        assertTrue(Files.exists(files.get(0)));
        assertEquals("{\"id\":1}", Files.readString(files.get(0)));
    }

    @Test
    void shouldSkipDirectoryEntries() throws IOException {
        OSSObjectSummary dirSummary = new OSSObjectSummary();
        dirSummary.setKey("data/prefix/subdir/");

        OSSObjectSummary fileSummary = new OSSObjectSummary();
        fileSummary.setKey("data/prefix/part-00000.json");

        when(objectListing.getObjectSummaries()).thenReturn(List.of(dirSummary, fileSummary));
        when(objectListing.isTruncated()).thenReturn(false);
        when(ossClient.listObjects(any(ListObjectsRequest.class))).thenReturn(objectListing);

        byte[] content = "test".getBytes(StandardCharsets.UTF_8);
        when(ossObject.getObjectContent()).thenReturn(new ByteArrayInputStream(content));
        when(ossClient.getObject("my-bucket", "data/prefix/part-00000.json")).thenReturn(ossObject);

        List<Path> files = ossReader.read();

        assertEquals(1, files.size());
    }

    @Test
    void shouldReturnEmptyListWhenNoFiles() throws IOException {
        when(objectListing.getObjectSummaries()).thenReturn(List.of());
        when(objectListing.isTruncated()).thenReturn(false);
        when(ossClient.listObjects(any(ListObjectsRequest.class))).thenReturn(objectListing);

        List<Path> files = ossReader.read();

        assertTrue(files.isEmpty());
    }

    @Test
    void shouldCleanupTempFilesOnClose() throws IOException {
        OSSObjectSummary summary = new OSSObjectSummary();
        summary.setKey("data/prefix/part-00000.json");

        when(objectListing.getObjectSummaries()).thenReturn(List.of(summary));
        when(objectListing.isTruncated()).thenReturn(false);
        when(ossClient.listObjects(any(ListObjectsRequest.class))).thenReturn(objectListing);

        byte[] content = "data".getBytes(StandardCharsets.UTF_8);
        when(ossObject.getObjectContent()).thenReturn(new ByteArrayInputStream(content));
        when(ossClient.getObject("my-bucket", "data/prefix/part-00000.json")).thenReturn(ossObject);

        List<Path> files = ossReader.read();
        Path downloadedFile = files.get(0);
        assertTrue(Files.exists(downloadedFile));

        ossReader.close();

        assertFalse(Files.exists(downloadedFile));
    }

    @Test
    void shouldSkipSegmentsSubdirectory() throws IOException {
        OSSObjectSummary dataSummary = new OSSObjectSummary();
        dataSummary.setKey("data/prefix/part-00000.parquet");

        OSSObjectSummary segmentSummary = new OSSObjectSummary();
        segmentSummary.setKey("data/prefix/segments/table_0.tar.gz");

        when(objectListing.getObjectSummaries()).thenReturn(List.of(dataSummary, segmentSummary));
        when(objectListing.isTruncated()).thenReturn(false);
        when(ossClient.listObjects(any(ListObjectsRequest.class))).thenReturn(objectListing);

        byte[] content = "data".getBytes(StandardCharsets.UTF_8);
        when(ossObject.getObjectContent()).thenReturn(new ByteArrayInputStream(content));
        when(ossClient.getObject("my-bucket", "data/prefix/part-00000.parquet")).thenReturn(ossObject);

        List<Path> files = ossReader.read();

        assertEquals(1, files.size());
        assertTrue(files.get(0).getFileName().toString().endsWith(".parquet"));
    }
}

