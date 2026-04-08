package com.gojek.mc2pinot;

import com.aliyun.odps.Odps;
import com.aliyun.odps.account.Account;
import com.aliyun.odps.account.AliyunAccount;
import com.aliyun.oss.OSS;
import com.aliyun.oss.OSSClientBuilder;
import com.gojek.mc2pinot.config.MaxcomputeConfig;
import com.gojek.mc2pinot.config.PayloadTemplateRenderer;
import com.gojek.mc2pinot.config.PinotConfig;
import com.gojek.mc2pinot.core.GenerationResult;
import com.gojek.mc2pinot.core.PinotSegmenter;
import com.gojek.mc2pinot.core.SegmentInfo;
import com.gojek.mc2pinot.core.partition.PartitionFunction;
import com.gojek.mc2pinot.core.partition.PartitionFunctionFactory;
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
import java.nio.file.Paths;
import java.util.List;
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

        MaxcomputeConfig mcConfig = new MaxcomputeConfig(env);
        PinotConfig pinotConfig = new PinotConfig(env);

        String query = Files.readString(Paths.get(mcConfig.getQueryFilePath()), StandardCharsets.UTF_8);

        OSS mcOssClient = buildMcOssClient(mcConfig);
        try {
            Odps odpsClient = buildOdpsClient(mcConfig);
            MCUnloader mcUnloader = new MCUnloader(
                    odpsClient,
                    new QueryUnloadBuilder(),
                    mcConfig.getOssRoleArn(),
                    pinotConfig.getInputFormat());

            new OSSCleaner(mcOssClient).clean(mcConfig.getOssDestinationURI());
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

                fs.cleaner().clean(segmentFolderURI);

                PinotSegmenter segmenter = new PinotSegmenter(
                        ossReader, fs.writer(), pinotConfig.getSegmentKey(),
                        pinotConfig.getInputFormat(), schema, tableConfig, partitionFunction);

                GenerationResult result = segmenter.generateSegment();
                List<SegmentInfo> segments = result.segments();

                long inputRecordCount = result.inputRecordCount();
                long inputRecordSize = result.inputRecordSize();

                PayloadTemplateRenderer renderer = new PayloadTemplateRenderer(
                        pinotConfig.getCustomPayloadTemplatePath());

                UploadMode uploadMode = resolveUploadMode(pinotConfig.getDeepStorageURI());
                LOG.info("resolved upload mode: " + uploadMode);

                HttpClient httpClient = HttpClient.newBuilder()
                        .version(HttpClient.Version.HTTP_1_1)
                        .build();
                PinotClient pinotClient = new DefaultPinotClient(pinotConfig.getHost(), httpClient);
                PinotSegmentUploader uploader = new PinotSegmentUploader(pinotClient, uploadMode);
                try {
                    uploader.upload(segments, tableName, segment -> {
                        SegmentPayloadContext ctx = new SegmentPayloadContext(
                                inputRecordCount,
                                inputRecordSize,
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
                } finally {
                    for (SegmentInfo seg : segments) {
                        if (seg.localPath() != null) {
                            seg.localPath().toFile().delete();
                        }
                    }
                }

                fs.cleaner().clean(segmentFolderURI);
            }
        } finally {
            mcOssClient.shutdown();
        }
        LOG.info("success");
    }

    private static String buildSegmentFolderURI(String deepStorageURI, String tableName, String segmentKey) {
        String base = (deepStorageURI == null || deepStorageURI.isBlank())
                ? "file:///tmp/mc2pinot"
                : deepStorageURI.endsWith("/")
                ? deepStorageURI.substring(0, deepStorageURI.length() - 1)
                : deepStorageURI;
        return base + "/" + tableName + "/segments_" + segmentKey;
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

    private static UploadMode resolveUploadMode(String deepStorageURI) {
        if (deepStorageURI == null || deepStorageURI.isBlank()) {
            return UploadMode.FILE;
        }
        try {
            String scheme = java.net.URI.create(deepStorageURI).getScheme();
            if (scheme == null || "file".equals(scheme)) {
                return UploadMode.FILE;
            }
            return UploadMode.URI;
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
