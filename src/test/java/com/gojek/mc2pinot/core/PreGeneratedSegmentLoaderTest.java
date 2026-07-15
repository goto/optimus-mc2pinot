package com.gojek.mc2pinot.core;

import com.aliyun.oss.OSS;
import com.aliyun.oss.model.ListObjectsRequest;
import com.aliyun.oss.model.OSSObject;
import com.aliyun.oss.model.OSSObjectSummary;
import com.aliyun.oss.model.ObjectListing;
import org.apache.commons.compress.archivers.tar.TarArchiveEntry;
import org.apache.commons.compress.archivers.tar.TarArchiveInputStream;
import org.apache.commons.compress.archivers.tar.TarArchiveOutputStream;
import org.apache.commons.compress.compressors.gzip.GzipCompressorInputStream;
import org.apache.commons.compress.compressors.gzip.GzipCompressorOutputStream;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.lenient;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
class PreGeneratedSegmentLoaderTest {

    @Mock
    private OSS ossClient;

    @Mock
    private ObjectListing objectListing;

    private Path workDir;

    @BeforeEach
    void setUp() throws IOException {
        workDir = Files.createTempDirectory("skip-meta-test-");
    }

    @AfterEach
    void tearDown() throws IOException {
        try (var paths = Files.walk(workDir)) {
            paths.sorted(java.util.Comparator.reverseOrder())
                    .forEach(p -> p.toFile().delete());
        }
    }

    @Test
    void shouldLoadSegmentAndStageMetadataTarball() throws IOException {
        OSSObjectSummary summary = new OSSObjectSummary();
        summary.setKey("pinot/mySeg.tar.gz");
        summary.setSize(4096L);

        when(objectListing.getObjectSummaries()).thenReturn(List.of(summary));
        when(objectListing.isTruncated()).thenReturn(false);
        when(ossClient.listObjects(any(ListObjectsRequest.class))).thenReturn(objectListing);
        stubObject("pinot/mySeg.tar.gz", buildSegmentTarGz("mySeg", 42L, true));

        GenerationResult result =
                new PreGeneratedSegmentLoader(ossClient, "oss://my-bucket/pinot", workDir).load();

        assertEquals(0L, result.inputRecordCount());
        assertEquals(0L, result.inputRecordSize());
        assertEquals(1, result.segments().size());

        SegmentInfo segment = result.segments().get(0);
        assertEquals("mySeg", segment.segmentName());
        assertEquals("oss://my-bucket/pinot/mySeg.tar.gz", segment.remoteURI());
        assertNull(segment.localPath());
        assertEquals(42L, segment.outputRecordCount());
        assertEquals(4096L, segment.outputRecordSize());

        assertNotNull(segment.metadataPath());
        assertTrue(Files.exists(segment.metadataPath()));
        assertEquals(workDir.resolve("mySeg.metadata.tar.gz"), segment.metadataPath());

        Map<String, byte[]> metadataEntries = readTarGz(Files.readAllBytes(segment.metadataPath()));
        assertTrue(metadataEntries.containsKey("mySeg/metadata.properties"));
        assertTrue(metadataEntries.containsKey("mySeg/creation.meta"));
    }

    @Test
    void shouldSkipDirectoriesAndMetadataSidecarsAndNonSegmentObjects() throws IOException {
        OSSObjectSummary dir = new OSSObjectSummary();
        dir.setKey("pinot/");
        OSSObjectSummary sidecar = new OSSObjectSummary();
        sidecar.setKey("pinot/mySeg.metadata.tar.gz");
        OSSObjectSummary other = new OSSObjectSummary();
        other.setKey("pinot/_SUCCESS");
        OSSObjectSummary segment = new OSSObjectSummary();
        segment.setKey("pinot/mySeg.tar.gz");
        segment.setSize(10L);

        when(objectListing.getObjectSummaries()).thenReturn(List.of(dir, sidecar, other, segment));
        when(objectListing.isTruncated()).thenReturn(false);
        when(ossClient.listObjects(any(ListObjectsRequest.class))).thenReturn(objectListing);
        stubObject("pinot/mySeg.tar.gz", buildSegmentTarGz("mySeg", 7L, true));

        GenerationResult result =
                new PreGeneratedSegmentLoader(ossClient, "oss://my-bucket/pinot", workDir).load();

        assertEquals(1, result.segments().size());
        assertEquals("mySeg", result.segments().get(0).segmentName());
    }

    @Test
    void shouldFallBackToFileNameWhenSegmentNameAbsent() throws IOException {
        OSSObjectSummary summary = new OSSObjectSummary();
        summary.setKey("pinot/segments_20260714/derived_name.tar.gz");
        summary.setSize(1L);

        when(objectListing.getObjectSummaries()).thenReturn(List.of(summary));
        when(objectListing.isTruncated()).thenReturn(false);
        when(ossClient.listObjects(any(ListObjectsRequest.class))).thenReturn(objectListing);
        // metadata.properties present but without a segment.name entry.
        stubObject("pinot/segments_20260714/derived_name.tar.gz",
                buildSegmentTarGzWithoutName("orig", 3L));

        GenerationResult result =
                new PreGeneratedSegmentLoader(ossClient, "oss://my-bucket/pinot", workDir).load();

        assertEquals("derived_name", result.segments().get(0).segmentName());
    }

    @Test
    void shouldThrowWhenNoSegmentsFound() {
        when(objectListing.getObjectSummaries()).thenReturn(List.of());
        when(objectListing.isTruncated()).thenReturn(false);
        when(ossClient.listObjects(any(ListObjectsRequest.class))).thenReturn(objectListing);

        IOException ex = assertThrows(IOException.class,
                () -> new PreGeneratedSegmentLoader(ossClient, "oss://my-bucket/pinot", workDir).load());
        assertTrue(ex.getMessage().contains("No segment"));
    }

    @Test
    void shouldRejectNonOssBucketPath() {
        assertThrows(IllegalArgumentException.class,
                () -> new PreGeneratedSegmentLoader(ossClient, "file:///tmp/segments", workDir).load());
    }

    private void stubObject(String key, byte[] content) {
        OSSObject object = new OSSObject();
        object.setObjectContent(new ByteArrayInputStream(content));
        lenient().when(ossClient.getObject(eq("my-bucket"), eq(key))).thenReturn(object);
    }

    private static byte[] buildSegmentTarGz(String segmentName, long totalDocs, boolean withCreationMeta)
            throws IOException {
        Map<String, byte[]> entries = new LinkedHashMap<>();
        String props = "segment.name = " + segmentName + "\n"
                + "segment.total.docs = " + totalDocs + "\n";
        entries.put(segmentName + "/metadata.properties", props.getBytes(StandardCharsets.UTF_8));
        if (withCreationMeta) {
            entries.put(segmentName + "/creation.meta", new byte[]{1, 2, 3, 4});
        }
        // Non-target payload files that must be ignored.
        entries.put(segmentName + "/v3/columns.psf", new byte[2048]);
        return tarGz(entries, List.of(segmentName + "/", segmentName + "/v3/"));
    }

    private static byte[] buildSegmentTarGzWithoutName(String dirName, long totalDocs) throws IOException {
        Map<String, byte[]> entries = new LinkedHashMap<>();
        String props = "segment.total.docs = " + totalDocs + "\n";
        entries.put(dirName + "/metadata.properties", props.getBytes(StandardCharsets.UTF_8));
        entries.put(dirName + "/creation.meta", new byte[]{9});
        return tarGz(entries, List.of(dirName + "/"));
    }

    private static byte[] tarGz(Map<String, byte[]> files, List<String> dirs) throws IOException {
        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        try (GzipCompressorOutputStream gzos = new GzipCompressorOutputStream(baos);
             TarArchiveOutputStream taos = new TarArchiveOutputStream(gzos)) {
            taos.setLongFileMode(TarArchiveOutputStream.LONGFILE_GNU);
            for (String dir : dirs) {
                taos.putArchiveEntry(new TarArchiveEntry(dir));
                taos.closeArchiveEntry();
            }
            for (Map.Entry<String, byte[]> file : files.entrySet()) {
                TarArchiveEntry entry = new TarArchiveEntry(file.getKey());
                entry.setSize(file.getValue().length);
                taos.putArchiveEntry(entry);
                taos.write(file.getValue());
                taos.closeArchiveEntry();
            }
        }
        return baos.toByteArray();
    }

    private static Map<String, byte[]> readTarGz(byte[] tarGz) throws IOException {
        Map<String, byte[]> result = new LinkedHashMap<>();
        try (TarArchiveInputStream tais = new TarArchiveInputStream(
                new GzipCompressorInputStream(new ByteArrayInputStream(tarGz)))) {
            TarArchiveEntry entry;
            while ((entry = tais.getNextEntry()) != null) {
                if (entry.isDirectory()) {
                    continue;
                }
                ByteArrayOutputStream buffer = new ByteArrayOutputStream();
                byte[] chunk = new byte[8192];
                int n;
                while ((n = tais.read(chunk)) != -1) {
                    buffer.write(chunk, 0, n);
                }
                result.put(entry.getName(), buffer.toByteArray());
            }
        }
        return result;
    }
}
