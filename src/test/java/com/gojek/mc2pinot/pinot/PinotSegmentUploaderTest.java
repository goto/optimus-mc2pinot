package com.gojek.mc2pinot.pinot;

import com.gojek.mc2pinot.core.SegmentInfo;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import java.io.IOException;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.Mockito.*;

@ExtendWith(MockitoExtension.class)
class PinotSegmentUploaderTest {

    @Mock
    private PinotClient pinotClient;

    private PinotSegmentUploader uploader;

    @BeforeEach
    void setUp() {
        uploader = new PinotSegmentUploader(pinotClient);
    }

    @Test
    void shouldUploadAllSegments() throws IOException {
        List<SegmentInfo> segments = List.of(
                new SegmentInfo("seg_0", "oss://bucket/segments/seg_0.tar.gz", null),
                new SegmentInfo("seg_1", "oss://bucket/segments/seg_1.tar.gz", null),
                new SegmentInfo("seg_2", "oss://bucket/segments/seg_2.tar.gz", null)
        );

        when(pinotClient.triggerUploadFromUri(anyString(), anyString())).thenReturn("ok");

        uploader.upload(segments, "my_table_OFFLINE");

        verify(pinotClient).triggerUploadFromUri("oss://bucket/segments/seg_0.tar.gz", "my_table_OFFLINE");
        verify(pinotClient).triggerUploadFromUri("oss://bucket/segments/seg_1.tar.gz", "my_table_OFFLINE");
        verify(pinotClient).triggerUploadFromUri("oss://bucket/segments/seg_2.tar.gz", "my_table_OFFLINE");
        verifyNoMoreInteractions(pinotClient);
    }

    @Test
    void shouldHandleEmptySegmentList() throws IOException {
        uploader.upload(List.of(), "my_table_OFFLINE");

        verifyNoInteractions(pinotClient);
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

