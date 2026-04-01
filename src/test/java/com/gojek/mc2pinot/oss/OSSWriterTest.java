package com.gojek.mc2pinot.oss;

import com.aliyun.oss.OSS;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.api.io.TempDir;
import org.mockito.ArgumentCaptor;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.verify;

@ExtendWith(MockitoExtension.class)
class OSSWriterTest {

    @Mock
    private OSS ossClient;

    private OSSWriter ossWriter;

    @TempDir
    Path tempDir;

    @BeforeEach
    void setUp() {
        ossWriter = new OSSWriter(ossClient, "oss://my-bucket/data/prefix");
    }

    @Test
    void shouldUploadFileToCorrectPath() throws IOException {
        Path localFile = tempDir.resolve("test_segment.tar.gz");
        Files.writeString(localFile, "segment-data");

        String resultURI = ossWriter.write("test_segment.tar.gz", localFile);

        assertEquals("oss://my-bucket/data/prefix/segments/test_segment.tar.gz", resultURI);

        ArgumentCaptor<String> bucketCaptor = ArgumentCaptor.forClass(String.class);
        ArgumentCaptor<String> keyCaptor = ArgumentCaptor.forClass(String.class);
        ArgumentCaptor<File> fileCaptor = ArgumentCaptor.forClass(File.class);

        verify(ossClient).putObject(bucketCaptor.capture(), keyCaptor.capture(), fileCaptor.capture());

        assertEquals("my-bucket", bucketCaptor.getValue());
        assertEquals("data/prefix/segments/test_segment.tar.gz", keyCaptor.getValue());
        assertEquals(localFile.toFile(), fileCaptor.getValue());
    }

    @Test
    void shouldHandleTrailingSlashInURI() throws IOException {
        ossWriter = new OSSWriter(ossClient, "oss://my-bucket/data/prefix/");

        Path localFile = tempDir.resolve("seg.tar.gz");
        Files.writeString(localFile, "data");

        String resultURI = ossWriter.write("seg.tar.gz", localFile);

        assertEquals("oss://my-bucket/data/prefix/segments/seg.tar.gz", resultURI);
    }
}

