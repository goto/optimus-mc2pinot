package com.gojek.mc2pinot;

import com.aliyun.odps.Odps;
import com.aliyun.odps.account.Account;
import com.aliyun.odps.account.AliyunAccount;
import com.aliyun.oss.OSS;
import com.aliyun.oss.OSSClientBuilder;
import com.gojek.mc2pinot.config.CustomHeadersLoader;
import com.gojek.mc2pinot.config.MaxcomputeConfig;
import com.gojek.mc2pinot.config.OSSConfig;
import com.gojek.mc2pinot.config.PayloadTemplateRenderer;
import com.gojek.mc2pinot.config.PinotConfig;
import com.gojek.mc2pinot.core.GenerationResult;
import com.gojek.mc2pinot.core.PinotSegmenter;
import com.gojek.mc2pinot.core.PreGeneratedSegmentLoader;
import com.gojek.mc2pinot.core.partition.PartitionFunction;
import com.gojek.mc2pinot.core.partition.PartitionFunctionFactory;
import com.gojek.mc2pinot.io.Cleaner;
import com.gojek.mc2pinot.io.FsFactory;
import com.gojek.mc2pinot.io.oss.OSSCleaner;
import com.gojek.mc2pinot.io.oss.OSSReader;
import com.gojek.mc2pinot.mc.MCUnloader;
import com.gojek.mc2pinot.mc.QueryUnloadBuilder;
import com.gojek.mc2pinot.metrics.SegmentPayloadContext;
import com.gojek.mc2pinot.pinot.DefaultPinotClient;
import com.gojek.mc2pinot.pinot.PinotClient;
import com.gojek.mc2pinot.pinot.PinotSegmentUploader;
import com.gojek.mc2pinot.pinot.UploadMode;
import org.apache.pinot.spi.config.table.ColumnPartitionConfig;
import org.apache.pinot.spi.config.table.SegmentPartitionConfig;
import org.apache.pinot.spi.config.table.TableConfig;
import org.apache.pinot.spi.data.Schema;
import org.apache.pinot.spi.utils.JsonUtils;

import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.net.http.HttpClient;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.security.SecureRandom;
import java.util.Map;
import java.util.logging.Handler;
import java.util.logging.Logger;

public class Main {

    private static final Logger LOG = Logger.getLogger(Main.class.getName());

    static {
        LogFormatter formatter = new LogFormatter();
        Logger rootLogger = Logger.getLogger("");
        for (Handler handler : rootLogger.getHandlers()) {
            handler.setFormatter(formatter);
        }
    }

    public static void main(String[] args) throws Exception {
        LOG.info("starting...");
        Map<String, String> env = System.getenv();

        PinotConfig pinotConfig = new PinotConfig(env);

        if (pinotConfig.isSegmentGenerationSkip()) {
            runSkipGeneration(pinotConfig);
            LOG.info("success");
            return;
        }

        MaxcomputeConfig mcConfig = new MaxcomputeConfig(env);

        String query = Files.readString(Paths.get(mcConfig.getQueryFilePath()), StandardCharsets.UTF_8);

        OSS mcOssClient = buildMcOssClient(mcConfig);
        try {
            Odps odpsClient = buildOdpsClient(mcConfig);
            MCUnloader mcUnloader = new MCUnloader(
                    odpsClient,
                    new QueryUnloadBuilder(),
                    mcConfig.getOssRoleArn(),
                    pinotConfig.getInputFormat());

            Thread shutdownHook = new Thread(mcUnloader::kill, "mc-shutdown-hook");
            Runtime.getRuntime().addShutdownHook(shutdownHook);

            new OSSCleaner(mcOssClient).clean(mcConfig.getOssDestinationURI() + "/");
            mcUnloader.unload(query, mcConfig.getOssDestinationURI());

            Schema schema = loadSchema(pinotConfig.getSchemaFilePath());
            TableConfig tableConfig = loadTableConfig(pinotConfig.getTableConfigFilePath());
            String tableName = tableConfig.getTableName();

            String segmentFolderURI = buildSegmentFolderURI(
                    pinotConfig.getDeepStorageURI(), tableName, pinotConfig.getSegmentKey());

            PartitionFunction partitionFunction = PartitionFunctionFactory.create(
                    resolvePartitionFunctionName(tableConfig));

            try (OSSReader ossReader = new OSSReader(mcOssClient, mcConfig.getOssDestinationURI());
                 FsFactory.FsComponents fs = FsFactory.create(pinotConfig, segmentFolderURI)) {

                fs.cleaner().clean(segmentFolderURI + "/");

                UploadMode uploadMode = resolveUploadMode(
                        pinotConfig.getDeepStorageURI(), pinotConfig.getDeepStorageURIUploadType());
                LOG.info("resolved upload mode: " + uploadMode);

                PinotSegmenter segmenter = new PinotSegmenter(
                        ossReader, fs.writer(), pinotConfig.getSegmentKey(),
                        pinotConfig.getInputFormat(), schema, tableConfig, partitionFunction)
                        .setSegmentCount(pinotConfig.getSegmentCount())
                        // Keep the local tar only when the uploader pushes the file itself (FILE mode);
                        // keep local metadata unless URI mode ignores it. This lets URI/METADATA runs
                        // free segment tars as soon as they reach deep storage.
                        .setLocalArtifactRetention(
                                uploadMode == UploadMode.FILE,
                                uploadMode != UploadMode.URI);

                GenerationResult result = segmenter.generateSegment();

                pushSegments(pinotConfig, uploadMode, fs.cleaner(), tableName, result);
                new OSSCleaner(mcOssClient).clean(mcConfig.getOssDestinationURI() + "/");
            }
        } finally {
            mcOssClient.shutdown();
        }
        LOG.info("success");
    }

    /**
     * Skip-generation path: the segments already exist in an OSS folder, so there is no Maxcompute
     * unload and no segment build. We read each segment's metadata, stage a metadata sidecar, and
     * push to Pinot with the same uploader as a normal run. The source bucket is never cleaned —
     * the pre-generated segments are the caller's and remain the deep-store download target.
     */
    private static void runSkipGeneration(PinotConfig pinotConfig) throws Exception {
        TableConfig tableConfig = loadTableConfig(pinotConfig.getTableConfigFilePath());
        String tableName = tableConfig.getTableName();
        String bucketPath = pinotConfig.getSegmentGenerationBucketPath();

        OSSConfig ossConfig = pinotConfig.getDeepStorageOssConfig();
        if (ossConfig == null) {
            throw new IllegalArgumentException(
                    "Segment generation skip requires an oss:// bucket path and "
                            + "PINOT__DEEP_STORAGE_OSS_SERVICE_ACCOUNT credentials");
        }

        UploadMode uploadMode = resolveUploadMode(bucketPath, pinotConfig.getDeepStorageURIUploadType());
        LOG.info("segment generation skipped; loading pre-generated segments from " + bucketPath
                + " (upload mode: " + uploadMode + ")");

        OSS ossClient = buildOSSClient(ossConfig);
        Path workDir = Files.createTempDirectory("mc2pinot-skip-meta-");
        try {
            GenerationResult result =
                    new PreGeneratedSegmentLoader(ossClient, bucketPath, workDir).load();
            // No-op cleaner: never delete the caller-owned source segments.
            pushSegments(pinotConfig, uploadMode, uri -> {}, tableName, result);
        } finally {
            ossClient.shutdown();
            deleteDirectoryQuietly(workDir);
        }
    }

    /**
     * Renders the payload template per segment and pushes every segment to the Pinot controller.
     * Shared by the normal and skip-generation paths.
     */
    private static void pushSegments(PinotConfig pinotConfig, UploadMode uploadMode, Cleaner cleaner,
                                     String tableName, GenerationResult result) throws Exception {
        long inputRecordCount = result.inputRecordCount();
        long inputRecordSize = result.inputRecordSize();

        PayloadTemplateRenderer renderer = new PayloadTemplateRenderer(
                pinotConfig.getCustomPayloadTemplatePath());

        HttpClient httpClient = HttpClient.newBuilder()
                .version(HttpClient.Version.HTTP_1_1)
                .build();
        Map<String, String> customHeaders =
                new CustomHeadersLoader(pinotConfig.getCustomHeadersPath()).load();
        PinotClient pinotClient = new DefaultPinotClient(pinotConfig.getHost(), httpClient, customHeaders);
        PinotSegmentUploader uploader = new PinotSegmentUploader(
                pinotClient, uploadMode, cleaner, pinotConfig.getSegmentPushDelayInSeconds(),
                pinotConfig.isForceReloadAfterPush());
        uploader.upload(result.segments(), tableName, segment -> {
            SegmentPayloadContext ctx = new SegmentPayloadContext(
                    inputRecordCount,
                    inputRecordSize,
                    tableName,
                    segment.segmentName(),
                    segment.outputRecordCount(),
                    segment.outputRecordSize()
            );
            try {
                return renderer.render(ctx);
            } catch (IOException e) {
                throw new RuntimeException("Failed to render payload template for segment "
                        + segment.segmentName(), e);
            }
        });
    }

    private static void deleteDirectoryQuietly(Path dir) {
        try (var paths = Files.walk(dir)) {
            paths.sorted(java.util.Comparator.reverseOrder())
                    .forEach(p -> {
                        try {
                            Files.deleteIfExists(p);
                        } catch (IOException ignored) {
                        }
                    });
        } catch (IOException e) {
            LOG.warning("could not clean up temp dir " + dir + ": " + e.getMessage());
        }
    }

    private static OSS buildOSSClient(OSSConfig config) {
        return new OSSClientBuilder().build(
                config.getEndpoint(),
                config.getAccessKeyId(),
                config.getAccessKeySecret());
    }

    static String buildSegmentFolderURI(String deepStorageURI, String tableName, String segmentKey) {
        String base = (deepStorageURI == null || deepStorageURI.isBlank())
                ? "file:///tmp/mc2pinot"
                : stripTrailingSlash(deepStorageURI);
        return base + "/" + tableName + "/segments_" + segmentKey + "-" + randomHex();
    }

    /** 8-character random hex suffix, e.g. {@code a1b2c3d4}. */
    private static String randomHex() {
        byte[] bytes = new byte[4];
        new SecureRandom().nextBytes(bytes);
        StringBuilder hex = new StringBuilder(8);
        for (byte b : bytes) {
            hex.append(String.format("%02x", b));
        }
        return hex.toString();
    }

    private static String stripTrailingSlash(String value) {
        return value.endsWith("/") ? value.substring(0, value.length() - 1) : value;
    }

    private static Odps buildOdpsClient(MaxcomputeConfig config) {
        Account account = new AliyunAccount(config.getAccessId(), config.getAccessKey());
        Odps odps = new Odps(account);
        odps.setDefaultProject(config.getProjectName());
        odps.setEndpoint(config.getEndpoint());
        return odps;
    }

    private static OSS buildMcOssClient(MaxcomputeConfig config) {
        return new OSSClientBuilder().build(
                config.getOssEndpoint(),
                config.getOssAccessKeyId(),
                config.getOssAccessKeySecret());
    }

    private static Schema loadSchema(String filePath) throws Exception {
        try (InputStream is = new FileInputStream(filePath)) {
            return Schema.fromInputStream(is);
        }
    }

    private static TableConfig loadTableConfig(String filePath) throws Exception {
        try (InputStream is = new FileInputStream(filePath)) {
            String json = new String(is.readAllBytes(), StandardCharsets.UTF_8);
            return JsonUtils.stringToObject(json, TableConfig.class);
        }
    }

    private static UploadMode resolveUploadMode(String deepStorageURI, String uriUploadType) {
        if (deepStorageURI == null || deepStorageURI.isBlank()) {
            return UploadMode.FILE;
        }
        try {
            String scheme = java.net.URI.create(deepStorageURI).getScheme();
            if (scheme == null || "file".equals(scheme)) {
                return UploadMode.FILE;
            }
            return "URI".equalsIgnoreCase(uriUploadType) ? UploadMode.URI : UploadMode.METADATA;
        } catch (IllegalArgumentException e) {
            return UploadMode.FILE;
        }
    }

    private static String resolvePartitionFunctionName(TableConfig tableConfig) {
        SegmentPartitionConfig partitionConfig = tableConfig.getIndexingConfig() != null
                ? tableConfig.getIndexingConfig().getSegmentPartitionConfig() : null;
        if (partitionConfig == null) {
            return null;
        }
        Map<String, ColumnPartitionConfig> columnMap = partitionConfig.getColumnPartitionMap();
        if (columnMap == null || columnMap.isEmpty()) {
            return null;
        }
        return columnMap.values().iterator().next().getFunctionName();
    }
}
