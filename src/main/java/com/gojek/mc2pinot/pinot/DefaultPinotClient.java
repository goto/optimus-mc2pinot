package com.gojek.mc2pinot.pinot;

import java.io.IOException;
import java.net.URI;
import java.net.URLEncoder;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.UUID;

public class DefaultPinotClient implements PinotClient {

    private final String host;
    private final HttpClient httpClient;

    public DefaultPinotClient(String host, HttpClient httpClient) {
        this.host = host;
        this.httpClient = httpClient;
    }

    @Override
    public String triggerUpload(Path segmentFile, String tableName) throws IOException {
        String tableType = extractTableType(tableName);
        String baseTableName = extractBaseTableName(tableName, tableType);

        String url = String.format("%s/v2/segments?tableName=%s&tableType=%s",
                host,
                URLEncoder.encode(baseTableName, StandardCharsets.UTF_8),
                URLEncoder.encode(tableType, StandardCharsets.UTF_8));

        String boundary = UUID.randomUUID().toString();
        String fileName = segmentFile.getFileName().toString();
        byte[] fileBytes = Files.readAllBytes(segmentFile);
        byte[] body = buildMultipartBody(boundary, fileName, fileBytes);

        HttpRequest request = HttpRequest.newBuilder()
                .uri(URI.create(url))
                .header("Content-Type", "multipart/form-data; boundary=" + boundary)
                .POST(HttpRequest.BodyPublishers.ofByteArray(body))
                .build();

        try {
            HttpResponse<String> response = httpClient.send(request, HttpResponse.BodyHandlers.ofString());
            if (response.statusCode() < 200 || response.statusCode() >= 300) {
                throw new IOException("Pinot upload failed with status " + response.statusCode()
                        + ": " + response.body());
            }
            return response.body();
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new IOException("Pinot upload interrupted", e);
        }
    }

    private byte[] buildMultipartBody(String boundary, String fileName, byte[] fileBytes) {
        String header = "--" + boundary + "\r\n"
                + "Content-Disposition: form-data; name=\"file\"; filename=\"" + fileName + "\"\r\n"
                + "Content-Type: application/octet-stream\r\n\r\n";
        String footer = "\r\n--" + boundary + "--\r\n";

        byte[] headerBytes = header.getBytes(StandardCharsets.UTF_8);
        byte[] footerBytes = footer.getBytes(StandardCharsets.UTF_8);

        byte[] body = new byte[headerBytes.length + fileBytes.length + footerBytes.length];
        System.arraycopy(headerBytes, 0, body, 0, headerBytes.length);
        System.arraycopy(fileBytes, 0, body, headerBytes.length, fileBytes.length);
        System.arraycopy(footerBytes, 0, body, headerBytes.length + fileBytes.length, footerBytes.length);
        return body;
    }

    @Override
    public String triggerUploadFromUri(String uri, String tableName, String customPayload) throws IOException {
        String tableType = extractTableType(tableName);
        String baseTableName = extractBaseTableName(tableName, tableType);

        String url = String.format("%s/v2/segments?tableName=%s&tableType=%s",
                host,
                URLEncoder.encode(baseTableName, StandardCharsets.UTF_8),
                URLEncoder.encode(tableType, StandardCharsets.UTF_8));

        String body = (customPayload == null || customPayload.isBlank()) ? "{}" : customPayload;

        HttpRequest request = HttpRequest.newBuilder()
                .uri(URI.create(url))
                .header("Content-Type", "application/json")
                .header("UPLOAD_TYPE", "URI")
                .header("DOWNLOAD_URI", uri)
                .POST(HttpRequest.BodyPublishers.ofString(body, StandardCharsets.UTF_8))
                .build();

        try {
            HttpResponse<String> response = httpClient.send(request, HttpResponse.BodyHandlers.ofString());
            if (response.statusCode() < 200 || response.statusCode() >= 300) {
                throw new IOException("Pinot upload failed with status " + response.statusCode()
                        + ": " + response.body());
            }
            return response.body();
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new IOException("Pinot upload interrupted", e);
        }
    }

    @Override
    public String getSchema(String tableName) {
        throw new UnsupportedOperationException("REST-based schema fetch not yet implemented");
    }

    @Override
    public String getTableConfig(String tableName) {
        throw new UnsupportedOperationException("REST-based table config fetch not yet implemented");
    }

    private String extractTableType(String tableName) {
        if (tableName.endsWith("_OFFLINE")) {
            return "OFFLINE";
        }
        if (tableName.endsWith("_REALTIME")) {
            return "REALTIME";
        }
        return "OFFLINE";
    }

    private String extractBaseTableName(String tableName, String tableType) {
        String suffix = "_" + tableType;
        if (tableName.endsWith(suffix)) {
            return tableName.substring(0, tableName.length() - suffix.length());
        }
        return tableName;
    }
}

