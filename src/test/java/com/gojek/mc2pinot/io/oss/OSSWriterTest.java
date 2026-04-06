package com.gojek.mc2pinot.io.oss;

import com.aliyun.oss.OSS;
import com.aliyun.oss.model.UploadFileRequest;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.api.io.TempDir;
import org.mockito.ArgumentCaptor;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.doThrow;
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
        ossWriter = new OSSWriter(ossClient, "oss://my-bucket/data/prefix/segments", 5);
    }

    @Test
    void shouldUploadFileToCorrectPath() throws Throwable {
        Path localFile = tempDir.resolve("test_segment.tar.gz");
        Files.writeString(localFile, "segment-data");

        String resultURI = ossWriter.write("test_segment.tar.gz", localFile);

        assertEquals("oss://my-bucket/data/prefix/segments/test_segment.tar.gz", resultURI);

        ArgumentCaptor<UploadFileRequest> requestCaptor = ArgumentCaptor.forClass(UploadFileRequest.class);
        verify(ossClient).uploadFile(requestCaptor.capture());

        UploadFileRequest captured = requestCaptor.getValue();
        assertEquals("my-bucket", captured.getBucketName());
        assertEquals("data/prefix/segments/test_segment.tar.gz", captured.getKey());
        assertEquals(localFile.toAbsolutePath().toString(), captured.getUploadFile());
        assertEquals(5, captured.getTaskNum());
        assertFalse(captured.isEnableCheckpoint());
    }

    @Test
    void shouldHandleTrailingSlashInURI() throws IOException {
        ossWriter = new OSSWriter(ossClient, "oss://my-bucket/data/prefix/segments/", 5);

        Path localFile = tempDir.resolve("seg.tar.gz");
        Files.writeString(localFile, "data");

        String resultURI = ossWriter.write("seg.tar.gz", localFile);

        assertEquals("oss://my-bucket/data/prefix/segments/seg.tar.gz", resultURI);
    }

    @Test
    void shouldSetWriterTaskNumberOnUploadRequest() throws Throwable {
        ossWriter = new OSSWriter(ossClient, "oss://my-bucket/data/prefix/segments", 3);

        Path localFile = tempDir.resolve("seg.tar.gz");
        Files.writeString(localFile, "data");

        ossWriter.write("seg.tar.gz", localFile);

        ArgumentCaptor<UploadFileRequest> requestCaptor = ArgumentCaptor.forClass(UploadFileRequest.class);
        verify(ossClient).uploadFile(requestCaptor.capture());

        assertEquals(3, requestCaptor.getValue().getTaskNum());
    }

    @Test
    void shouldWrapUploadExceptionInIOException() throws Throwable {
        doThrow(new RuntimeException("upload failed")).when(ossClient).uploadFile(org.mockito.ArgumentMatchers.any());

        Path localFile = tempDir.resolve("seg.tar.gz");
        Files.writeString(localFile, "data");

        assertThrows(IOException.class, () -> ossWriter.write("seg.tar.gz", localFile));
    }
}

