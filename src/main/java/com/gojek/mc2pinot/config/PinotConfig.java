package com.gojek.mc2pinot.config;

import java.net.URI;
import java.util.Map;

public class PinotConfig {

    private final String host;
    private final String customHeadersPath;
    private final String customPayloadTemplatePath;
    private final String segmentKey;
    private final String inputFormat;
    private final String schemaFilePath;
    private final String tableConfigFilePath;
    private final String deepStorageURI;
    private final String deepStorageURIUploadType;
    private final long segmentPushDelayInSeconds;
    private final int segmentCount;
    private final boolean segmentGenerationSkip;
    private final String segmentGenerationBucketPath;
    private final OSSConfig deepStorageOssConfig;

    private static final long DEFAULT_SEGMENT_PUSH_DELAY_IN_SECONDS = 30L;
    /** 0 means "not configured" — fall back to the table's partition count. */
    private static final int DEFAULT_SEGMENT_COUNT = 0;

    public PinotConfig(Map<String, String> env) {
        this.segmentGenerationSkip = ConfigHelper.optionalBooleanWithDefault(
                env, Constant.PINOT_SEGMENT_GENERATION_SKIP, false);

        this.host = ConfigHelper.requireNonEmpty(env, Constant.PINOT_HOST);
        this.customHeadersPath = env.get(Constant.PINOT_CUSTOM_HEADERS_PATH);
        this.customPayloadTemplatePath = env.get(Constant.PINOT_CUSTOM_PAYLOAD_TEMPLATE_PATH);
        // Segment-key, input-format and schema only drive segment *generation*; when generation is
        // skipped the segments already exist, so these become optional.
        this.segmentKey = segmentGenerationSkip
                ? env.get(Constant.PINOT_SEGMENT_KEY)
                : ConfigHelper.requireNonEmpty(env, Constant.PINOT_SEGMENT_KEY);
        this.inputFormat = segmentGenerationSkip
                ? env.get(Constant.PINOT_INPUT_FORMAT)
                : ConfigHelper.requireNonEmpty(env, Constant.PINOT_INPUT_FORMAT);
        this.schemaFilePath = segmentGenerationSkip
                ? env.get(Constant.PINOT_SCHEMA_FILE_PATH)
                : ConfigHelper.requireNonEmpty(env, Constant.PINOT_SCHEMA_FILE_PATH);
        // Table config is always required — the push needs the table name either way.
        this.tableConfigFilePath = ConfigHelper.requireNonEmpty(env, Constant.PINOT_TABLE_CONFIG_FILE_PATH);
        // The folder of pre-generated segments; required only when generation is skipped.
        this.segmentGenerationBucketPath = segmentGenerationSkip
                ? ConfigHelper.requireNonEmpty(env, Constant.PINOT_SEGMENT_GENERATION_BUCKET_PATH)
                : env.get(Constant.PINOT_SEGMENT_GENERATION_BUCKET_PATH);
        this.deepStorageURI = env.get(Constant.PINOT_DEEP_STORAGE_URI);
        this.deepStorageURIUploadType = ConfigHelper.optionalWithDefault(
                env, Constant.PINOT_DEEP_STORAGE_URI_UPLOAD_TYPE, "METADATA").toUpperCase();
        this.segmentPushDelayInSeconds = ConfigHelper.optionalLongWithDefault(
                env, Constant.PINOT_SEGMENT_PUSH_DELAY_IN_SECONDS, DEFAULT_SEGMENT_PUSH_DELAY_IN_SECONDS);
        this.segmentCount = ConfigHelper.optionalIntWithDefault(
                env, Constant.PINOT_SEGMENT_COUNT, DEFAULT_SEGMENT_COUNT);
        if (this.segmentCount < 0) {
            throw new IllegalArgumentException(
                    Constant.PINOT_SEGMENT_COUNT + " must be >= 1 (or unset to use the table partition count)");
        }
        // OSS credentials are needed either for the deep-storage sink or, in skip mode, for reading
        // the pre-generated segment bucket. Both reuse PINOT__DEEP_STORAGE_OSS_SERVICE_ACCOUNT.
        boolean needsOss = "oss".equals(resolveScheme(deepStorageURI))
                || (segmentGenerationSkip && "oss".equals(resolveScheme(segmentGenerationBucketPath)));
        this.deepStorageOssConfig = needsOss ? new OSSConfig(env) : null;
    }

    public String getHost() {
        return host;
    }

    public String getCustomHeadersPath() {
        return customHeadersPath;
    }

    public String getCustomPayloadTemplatePath() {
        return customPayloadTemplatePath;
    }

    public String getSegmentKey() {
        return segmentKey;
    }

    public String getInputFormat() {
        return inputFormat == null ? null : inputFormat.toLowerCase();
    }

    public String getSchemaFilePath() {
        return schemaFilePath;
    }

    public String getTableConfigFilePath() {
        return tableConfigFilePath;
    }

    public String getDeepStorageURI() {
        return deepStorageURI;
    }

    public String getDeepStorageURIUploadType() {
        return deepStorageURIUploadType;
    }

    public long getSegmentPushDelayInSeconds() {
        return segmentPushDelayInSeconds;
    }

    /** Configured physical segment count, or 0 to fall back to the table partition count. */
    public int getSegmentCount() {
        return segmentCount;
    }

    public OSSConfig getDeepStorageOssConfig() {
        return deepStorageOssConfig;
    }

    /** When {@code true}, segment generation is skipped and pre-generated segments are pushed as-is. */
    public boolean isSegmentGenerationSkip() {
        return segmentGenerationSkip;
    }

    /** Bucket folder holding the pre-generated segment {@code .tar.gz} files (skip mode only). */
    public String getSegmentGenerationBucketPath() {
        return segmentGenerationBucketPath;
    }

    private static String resolveScheme(String uri) {
        if (uri == null || uri.isBlank()) {
            return null;
        }
        try {
            return URI.create(uri).getScheme();
        } catch (IllegalArgumentException e) {
            return null;
        }
    }
}
