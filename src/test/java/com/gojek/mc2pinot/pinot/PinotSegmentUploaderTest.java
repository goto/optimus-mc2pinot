package com.gojek.mc2pinot.pinot;

import com.gojek.mc2pinot.core.SegmentInfo;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.api.io.TempDir;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.Mockito.*;

@ExtendWith(MockitoExtension.class)
class PinotSegmentUploaderTest {

    @Mock
    private PinotClient pinotClient;

    @TempDir
    Path tempDir;

    private PinotSegmentUploader uploader;

    @BeforeEach
    void setUp() {
        uploader = new PinotSegmentUploader(pinotClient);
    }

    @Test
    void shouldUseRemoteUriWhenOnlyRemoteUriIsSet() throws IOException {
        List<SegmentInfo> segments = List.of(
                new SegmentInfo("seg_0", "oss://bucket/segments/seg_0.tar.gz", null),
                new SegmentInfo("seg_1", "gs://bucket/segments/seg_1.tar.gz", null),
                new SegmentInfo("seg_2", "s3://bucket/segments/seg_2.tar.gz", null)
        );

        when(pinotClient.triggerUploadFromUri(anyString(), anyString())).thenReturn("ok");

        uploader.upload(segments, "my_table_OFFLINE");

        verify(pinotClient).triggerUploadFromUri("oss://bucket/segments/seg_0.tar.gz", "my_table_OFFLINE");
        verify(pinotClient).triggerUploadFromUri("gs://bucket/segments/seg_1.tar.gz", "my_table_OFFLINE");
        verify(pinotClient).triggerUploadFromUri("s3://bucket/segments/seg_2.tar.gz", "my_table_OFFLINE");
        verifyNoMoreInteractions(pinotClient);
    }

    @Test
    void shouldUseLocalPathWhenOnlyLocalPathIsSet() throws IOException {
        Path seg0 = Files.createFile(tempDir.resolve("seg_0.tar.gz"));
        Path seg1 = Files.createFile(tempDir.resolve("seg_1.tar.gz"));

        List<SegmentInfo> segments = List.of(
                new SegmentInfo("seg_0", null, seg0),
                new SegmentInfo("seg_1", null, seg1)
        );

        when(pinotClient.triggerUpload(any(Path.class), anyString())).thenReturn("ok");

        uploader.upload(segments, "my_table_OFFLINE");

        verify(pinotClient).triggerUpload(seg0, "my_table_OFFLINE");
        verify(pinotClient).triggerUpload(seg1, "my_table_OFFLINE");
        verifyNoMoreInteractions(pinotClient);
    }

    @Test
    void shouldPreferLocalPathWhenBothAreSet() throws IOException {
        Path localFile = Files.createFile(tempDir.resolve("seg_0.tar.gz"));

        List<SegmentInfo> segments = List.of(
                new SegmentInfo("seg_0", "oss://bucket/segments/seg_0.tar.gz", localFile)
        );

        when(pinotClient.triggerUpload(any(Path.class), anyString())).thenReturn("ok");

        uploader.upload(segments, "my_table_OFFLINE");

        verify(pinotClient).triggerUpload(localFile, "my_table_OFFLINE");
        verifyNoMoreInteractions(pinotClient);
    }

    @Test
    void shouldHandleEmptySegmentList() throws IOException {
        uploader.upload(List.of(), "my_table_OFFLINE");

        verifyNoInteractions(pinotClient);
    }

    @Test
    void shouldThrowWhenNeitherLocalPathNorRemoteUriIsSet() {
        List<SegmentInfo> segments = List.of(
                new SegmentInfo("seg_0", null, null)
        );

        assertThrows(IllegalArgumentException.class, () -> uploader.upload(segments, "my_table_OFFLINE"));
    }

    @Test
    void shouldPropagateExceptionOnUploadFailure() throws IOException {
        List<SegmentInfo> segments = List.of(
                new SegmentInfo("seg_0", "oss://bucket/segments/seg_0.tar.gz", null)
        );

        when(pinotClient.triggerUploadFromUri(anyString(), anyString()))
                .thenThrow(new IOException("upload failed"));

        assertThrows(IOException.class, () -> uploader.upload(segments, "my_table_OFFLINE"));
    }
}

