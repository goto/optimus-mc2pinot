package com.gojek.mc2pinot.pinot;

import com.gojek.mc2pinot.core.SegmentInfo;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.api.io.TempDir;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.function.Function;

import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.Mockito.*;

@ExtendWith(MockitoExtension.class)
class PinotSegmentUploaderTest {

    @Mock
    private PinotClient pinotClient;

    @TempDir
    Path tempDir;

    private final Function<SegmentInfo, String> fixedPayload = seg -> "{}";

    @Test
    void shouldUseRemoteUriInUriMode() throws IOException {
        PinotSegmentUploader uploader = new PinotSegmentUploader(pinotClient, UploadMode.URI);
        List<SegmentInfo> segments = List.of(
                new SegmentInfo("seg_0", "oss://bucket/segments/seg_0.tar.gz", null, 10, 1024),
                new SegmentInfo("seg_1", "gs://bucket/segments/seg_1.tar.gz", null, 20, 2048),
                new SegmentInfo("seg_2", "s3://bucket/segments/seg_2.tar.gz", null, 30, 3072)
        );

        when(pinotClient.triggerUploadFromUri(anyString(), anyString(), anyString())).thenReturn("ok");

        uploader.upload(segments, "my_table_OFFLINE", fixedPayload);

        verify(pinotClient).triggerUploadFromUri("oss://bucket/segments/seg_0.tar.gz", "my_table_OFFLINE", "{}");
        verify(pinotClient).triggerUploadFromUri("gs://bucket/segments/seg_1.tar.gz", "my_table_OFFLINE", "{}");
        verify(pinotClient).triggerUploadFromUri("s3://bucket/segments/seg_2.tar.gz", "my_table_OFFLINE", "{}");
        verifyNoMoreInteractions(pinotClient);
    }

    @Test
    void shouldUseLocalPathInFileMode() throws IOException {
        PinotSegmentUploader uploader = new PinotSegmentUploader(pinotClient, UploadMode.FILE);
        Path seg0 = Files.createFile(tempDir.resolve("seg_0.tar.gz"));
        Path seg1 = Files.createFile(tempDir.resolve("seg_1.tar.gz"));

        List<SegmentInfo> segments = List.of(
                new SegmentInfo("seg_0", null, seg0, 5, 512),
                new SegmentInfo("seg_1", null, seg1, 8, 768)
        );

        when(pinotClient.triggerUpload(any(Path.class), anyString())).thenReturn("ok");

        uploader.upload(segments, "my_table_OFFLINE", fixedPayload);

        verify(pinotClient).triggerUpload(seg0, "my_table_OFFLINE");
        verify(pinotClient).triggerUpload(seg1, "my_table_OFFLINE");
        verifyNoMoreInteractions(pinotClient);
    }

    @Test
    void shouldUseRemoteUriInUriModeEvenWhenLocalPathIsSet() throws IOException {
        PinotSegmentUploader uploader = new PinotSegmentUploader(pinotClient, UploadMode.URI);
        Path localFile = Files.createFile(tempDir.resolve("seg_0.tar.gz"));

        List<SegmentInfo> segments = List.of(
                new SegmentInfo("seg_0", "oss://bucket/segments/seg_0.tar.gz", localFile, 3, 256)
        );

        when(pinotClient.triggerUploadFromUri(anyString(), anyString(), anyString())).thenReturn("ok");

        uploader.upload(segments, "my_table_OFFLINE", fixedPayload);

        verify(pinotClient).triggerUploadFromUri("oss://bucket/segments/seg_0.tar.gz", "my_table_OFFLINE", "{}");
        verifyNoMoreInteractions(pinotClient);
    }

    @Test
    void shouldUseLocalPathInFileModeEvenWhenRemoteUriIsSet() throws IOException {
        PinotSegmentUploader uploader = new PinotSegmentUploader(pinotClient, UploadMode.FILE);
        Path localFile = Files.createFile(tempDir.resolve("seg_0.tar.gz"));

        List<SegmentInfo> segments = List.of(
                new SegmentInfo("seg_0", "oss://bucket/segments/seg_0.tar.gz", localFile, 3, 256)
        );

        when(pinotClient.triggerUpload(any(Path.class), anyString())).thenReturn("ok");

        uploader.upload(segments, "my_table_OFFLINE", fixedPayload);

        verify(pinotClient).triggerUpload(localFile, "my_table_OFFLINE");
        verifyNoMoreInteractions(pinotClient);
    }

    @Test
    void shouldNotInvokePayloadSupplierInFileMode() throws IOException {
        PinotSegmentUploader uploader = new PinotSegmentUploader(pinotClient, UploadMode.FILE);
        Path localFile = Files.createFile(tempDir.resolve("seg_0.tar.gz"));

        List<SegmentInfo> segments = List.of(
                new SegmentInfo("seg_0", null, localFile, 3, 256)
        );

        when(pinotClient.triggerUpload(any(Path.class), anyString())).thenReturn("ok");

        @SuppressWarnings("unchecked")
        Function<SegmentInfo, String> payloadSpy = mock(Function.class);
        uploader.upload(segments, "my_table_OFFLINE", payloadSpy);

        verifyNoInteractions(payloadSpy);
    }

    @Test
    void shouldHandleEmptySegmentList() throws IOException {
        PinotSegmentUploader uploader = new PinotSegmentUploader(pinotClient, UploadMode.FILE);
        uploader.upload(List.of(), "my_table_OFFLINE", fixedPayload);
        verifyNoInteractions(pinotClient);
    }

    @Test
    void shouldThrowWhenUriModeButRemoteUriIsNull() {
        PinotSegmentUploader uploader = new PinotSegmentUploader(pinotClient, UploadMode.URI);
        List<SegmentInfo> segments = List.of(
                new SegmentInfo("seg_0", null, null, 0, 0)
        );

        assertThrows(IllegalArgumentException.class,
                () -> uploader.upload(segments, "my_table_OFFLINE", fixedPayload));
    }

    @Test
    void shouldThrowWhenFileModeButLocalPathIsNull() {
        PinotSegmentUploader uploader = new PinotSegmentUploader(pinotClient, UploadMode.FILE);
        List<SegmentInfo> segments = List.of(
                new SegmentInfo("seg_0", "oss://bucket/seg_0.tar.gz", null, 0, 0)
        );

        assertThrows(IllegalArgumentException.class,
                () -> uploader.upload(segments, "my_table_OFFLINE", fixedPayload));
    }

    @Test
    void shouldPropagateExceptionOnUploadFailure() throws IOException {
        PinotSegmentUploader uploader = new PinotSegmentUploader(pinotClient, UploadMode.URI);
        List<SegmentInfo> segments = List.of(
                new SegmentInfo("seg_0", "oss://bucket/segments/seg_0.tar.gz", null, 5, 512)
        );

        when(pinotClient.triggerUploadFromUri(anyString(), anyString(), anyString()))
                .thenThrow(new IOException("upload failed"));

        assertThrows(IOException.class,
                () -> uploader.upload(segments, "my_table_OFFLINE", fixedPayload));
    }

    @Test
    void shouldPassPerSegmentRenderedPayloadInUriMode() throws IOException {
        PinotSegmentUploader uploader = new PinotSegmentUploader(pinotClient, UploadMode.URI);
        List<SegmentInfo> segments = List.of(
                new SegmentInfo("seg_0", "oss://bucket/segments/seg_0.tar.gz", null, 10, 1024),
                new SegmentInfo("seg_1", "oss://bucket/segments/seg_1.tar.gz", null, 20, 2048)
        );

        when(pinotClient.triggerUploadFromUri(anyString(), anyString(), anyString())).thenReturn("ok");

        Function<SegmentInfo, String> payloadFn = seg -> "{\"segment\":\"" + seg.segmentName() + "\"}";
        uploader.upload(segments, "my_table_OFFLINE", payloadFn);

        verify(pinotClient).triggerUploadFromUri(
                "oss://bucket/segments/seg_0.tar.gz", "my_table_OFFLINE", "{\"segment\":\"seg_0\"}");
        verify(pinotClient).triggerUploadFromUri(
                "oss://bucket/segments/seg_1.tar.gz", "my_table_OFFLINE", "{\"segment\":\"seg_1\"}");
    }
}

