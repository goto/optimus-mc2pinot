package com.gojek.mc2pinot.io.oss;

import com.aliyun.oss.OSS;
import com.aliyun.oss.model.ListObjectsRequest;
import com.aliyun.oss.model.OSSObjectSummary;
import com.aliyun.oss.model.ObjectListing;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import java.io.IOException;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.*;

@ExtendWith(MockitoExtension.class)
class OSSCleanerTest {

    @Mock
    private OSS ossClient;

    @Mock
    private ObjectListing objectListing;

    @Test
    void shouldDeleteAllObjectsUnderPrefix() throws IOException {
        OSSObjectSummary summary = new OSSObjectSummary();
        summary.setKey("data/prefix/part-0.json");

        when(ossClient.listObjects(any(ListObjectsRequest.class))).thenReturn(objectListing);
        when(objectListing.getObjectSummaries()).thenReturn(List.of(summary));
        when(objectListing.isTruncated()).thenReturn(false);

        new OSSCleaner(ossClient).clean("oss://my-bucket/data/prefix/");

        verify(ossClient).deleteObject("my-bucket", "data/prefix/part-0.json");
    }

    @Test
    void shouldBeNoOpWhenDestinationIsEmpty() {
        when(ossClient.listObjects(any(ListObjectsRequest.class))).thenReturn(objectListing);
        when(objectListing.getObjectSummaries()).thenReturn(List.of());
        when(objectListing.isTruncated()).thenReturn(false);

        assertDoesNotThrow(() -> new OSSCleaner(ossClient).clean("oss://my-bucket/data/empty/"));

        verify(ossClient, never()).deleteObject(any(), any());
    }

    @Test
    void shouldHandlePaginatedResults() throws IOException {
        OSSObjectSummary page1 = new OSSObjectSummary();
        page1.setKey("data/prefix/file-1.json");
        OSSObjectSummary page2 = new OSSObjectSummary();
        page2.setKey("data/prefix/file-2.json");

        ObjectListing listing1 = mock(ObjectListing.class);
        ObjectListing listing2 = mock(ObjectListing.class);

        when(ossClient.listObjects(any(ListObjectsRequest.class)))
                .thenReturn(listing1)
                .thenReturn(listing2);
        when(listing1.getObjectSummaries()).thenReturn(List.of(page1));
        when(listing1.isTruncated()).thenReturn(true);
        when(listing1.getNextMarker()).thenReturn("marker-1");
        when(listing2.getObjectSummaries()).thenReturn(List.of(page2));
        when(listing2.isTruncated()).thenReturn(false);

        new OSSCleaner(ossClient).clean("oss://my-bucket/data/prefix/");

        verify(ossClient).deleteObject("my-bucket", "data/prefix/file-1.json");
        verify(ossClient).deleteObject("my-bucket", "data/prefix/file-2.json");
    }

    @Test
    void shouldDeleteSingleObjectWhenUriHasNoTrailingSlash() throws IOException {
        new OSSCleaner(ossClient).clean("oss://my-bucket/data/segments/seg_0.tar.gz");

        verify(ossClient).deleteObject("my-bucket", "data/segments/seg_0.tar.gz");
        verifyNoMoreInteractions(ossClient);
    }

    @Test
    void shouldParseNestedKeyCorrectlyOnSingleFileClean() throws IOException {
        new OSSCleaner(ossClient).clean("oss://my-bucket/a/b/c/seg.tar.gz");

        verify(ossClient).deleteObject("my-bucket", "a/b/c/seg.tar.gz");
    }
}
