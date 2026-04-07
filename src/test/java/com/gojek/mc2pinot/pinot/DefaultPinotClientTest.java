package com.gojek.mc2pinot.pinot;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.api.io.TempDir;
import org.mockito.ArgumentCaptor;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import java.io.IOException;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.file.Files;
import java.nio.file.Path;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
class DefaultPinotClientTest {

    @Mock
    private HttpClient httpClient;

    @Mock
    private HttpResponse<String> httpResponse;

    @TempDir
    Path tempDir;

    private DefaultPinotClient pinotClient;

    @BeforeEach
    void setUp() {
        pinotClient = new DefaultPinotClient("http://localhost:9000", httpClient);
    }

    private Path createTempSegmentFile(String name) throws IOException {
        Path file = tempDir.resolve(name);
        Files.write(file, new byte[]{1, 2, 3});
        return file;
    }

    @Test
    void shouldSendCorrectUploadRequest() throws Exception {
        when(httpResponse.statusCode()).thenReturn(200);
        when(httpResponse.body()).thenReturn("{\"status\":\"ok\"}");
        when(httpClient.send(any(HttpRequest.class), any(HttpResponse.BodyHandler.class)))
                .thenReturn(httpResponse);

        Path segmentFile = createTempSegmentFile("dummy_table_OFFLINE_0.tar.gz");
        String result = pinotClient.triggerUpload(segmentFile, "dummy_table_OFFLINE", "{}");

        assertEquals("{\"status\":\"ok\"}", result);

        ArgumentCaptor<HttpRequest> requestCaptor = ArgumentCaptor.forClass(HttpRequest.class);
        verify(httpClient).send(requestCaptor.capture(), any(HttpResponse.BodyHandler.class));

        HttpRequest request = requestCaptor.getValue();
        assertEquals("POST", request.method());
        assertTrue(request.uri().toString().contains("/v2/segments"));
        assertTrue(request.uri().toString().contains("tableName=dummy_table"));
        assertTrue(request.uri().toString().contains("tableType=OFFLINE"));
        assertTrue(request.headers().firstValue("Content-Type").orElse("").startsWith("multipart/form-data"));
    }

    @Test
    void shouldThrowOnNon2xxResponse() throws Exception {
        when(httpResponse.statusCode()).thenReturn(500);
        when(httpResponse.body()).thenReturn("Internal Server Error");
        when(httpClient.send(any(HttpRequest.class), any(HttpResponse.BodyHandler.class)))
                .thenReturn(httpResponse);

        Path segmentFile = createTempSegmentFile("seg.tar.gz");
        IOException ex = assertThrows(IOException.class, () ->
                pinotClient.triggerUpload(segmentFile, "table_OFFLINE", "{}"));
        assertTrue(ex.getMessage().contains("500"));
    }

    @Test
    void shouldHandleInterruptedException() throws Exception {
        when(httpClient.send(any(HttpRequest.class), any(HttpResponse.BodyHandler.class)))
                .thenThrow(new InterruptedException("interrupted"));

        Path segmentFile = createTempSegmentFile("seg.tar.gz");
        assertThrows(IOException.class, () ->
                pinotClient.triggerUpload(segmentFile, "table_OFFLINE", "{}"));
        assertTrue(Thread.currentThread().isInterrupted());
        Thread.interrupted();
    }

    @Test
    void shouldHandleTableNameWithoutTypeSuffix() throws Exception {
        when(httpResponse.statusCode()).thenReturn(200);
        when(httpResponse.body()).thenReturn("ok");
        when(httpClient.send(any(HttpRequest.class), any(HttpResponse.BodyHandler.class)))
                .thenReturn(httpResponse);

        Path segmentFile = createTempSegmentFile("seg.tar.gz");
        pinotClient.triggerUpload(segmentFile, "my_table", "{}");

        ArgumentCaptor<HttpRequest> requestCaptor = ArgumentCaptor.forClass(HttpRequest.class);
        verify(httpClient).send(requestCaptor.capture(), any(HttpResponse.BodyHandler.class));
        assertTrue(requestCaptor.getValue().uri().toString().contains("tableType=OFFLINE"));
        assertTrue(requestCaptor.getValue().uri().toString().contains("tableName=my_table"));
    }

    @Test
    void shouldThrowOnGetSchema() {
        assertThrows(UnsupportedOperationException.class,
                () -> pinotClient.getSchema("table"));
    }

    @Test
    void shouldThrowOnGetTableConfig() {
        assertThrows(UnsupportedOperationException.class,
                () -> pinotClient.getTableConfig("table"));
    }

    @Test
    void shouldSendCorrectUploadFromUriRequest() throws Exception {
        when(httpResponse.statusCode()).thenReturn(200);
        when(httpResponse.body()).thenReturn("{\"status\":\"ok\"}");
        when(httpClient.send(any(HttpRequest.class), any(HttpResponse.BodyHandler.class)))
                .thenReturn(httpResponse);

        String ossUri = "oss://bucket/segments/dummy_table_OFFLINE_0.tar.gz";
        String result = pinotClient.triggerUploadFromUri(ossUri, "dummy_table_OFFLINE", "{}");

        assertEquals("{\"status\":\"ok\"}", result);

        ArgumentCaptor<HttpRequest> requestCaptor = ArgumentCaptor.forClass(HttpRequest.class);
        verify(httpClient).send(requestCaptor.capture(), any(HttpResponse.BodyHandler.class));

        HttpRequest request = requestCaptor.getValue();
        assertEquals("POST", request.method());
        assertTrue(request.uri().toString().contains("/v2/segments"));
        assertTrue(request.uri().toString().contains("tableName=dummy_table"));
        assertTrue(request.uri().toString().contains("tableType=OFFLINE"));
        assertEquals("URI", request.headers().firstValue("UPLOAD_TYPE").orElse(""));
        assertEquals(ossUri, request.headers().firstValue("DOWNLOAD_URI").orElse(""));
        assertEquals("application/json", request.headers().firstValue("Content-Type").orElse(""));
    }

    @Test
    void shouldThrowOnNon2xxResponseForUploadFromUri() throws Exception {
        when(httpResponse.statusCode()).thenReturn(500);
        when(httpResponse.body()).thenReturn("Internal Server Error");
        when(httpClient.send(any(HttpRequest.class), any(HttpResponse.BodyHandler.class)))
                .thenReturn(httpResponse);

        IOException ex = assertThrows(IOException.class, () ->
                pinotClient.triggerUploadFromUri("oss://bucket/seg.tar.gz", "table_OFFLINE", "{}"));
        assertTrue(ex.getMessage().contains("500"));
    }

    @Test
    void shouldHandleInterruptedExceptionForUploadFromUri() throws Exception {
        when(httpClient.send(any(HttpRequest.class), any(HttpResponse.BodyHandler.class)))
                .thenThrow(new InterruptedException("interrupted"));

        assertThrows(IOException.class, () ->
                pinotClient.triggerUploadFromUri("oss://bucket/seg.tar.gz", "table_OFFLINE", "{}"));
        assertTrue(Thread.currentThread().isInterrupted());
        Thread.interrupted();
    }
}

