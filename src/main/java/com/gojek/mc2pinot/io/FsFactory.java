package com.gojek.mc2pinot.io;

import com.aliyun.oss.OSS;
import com.aliyun.oss.OSSClientBuilder;
import com.gojek.mc2pinot.config.OSSConfig;
import com.gojek.mc2pinot.config.PinotConfig;
import com.gojek.mc2pinot.io.local.LocalCleaner;
import com.gojek.mc2pinot.io.local.LocalWriter;
import com.gojek.mc2pinot.io.oss.OSSCleaner;
import com.gojek.mc2pinot.io.oss.OSSWriter;

import java.io.Closeable;
import java.io.IOException;
import java.net.URI;

public class FsFactory {

    public static FsComponents create(PinotConfig config, String segmentFolderURI) {
        String deepStorageURI = config.getDeepStorageURI();

        if (deepStorageURI == null || deepStorageURI.isBlank()) {
            return new FsComponents(
                    new LocalWriter(segmentFolderURI),
                    new LocalCleaner(),
                    () -> {}
            );
        }

        String scheme = resolveScheme(deepStorageURI);
        return switch (scheme == null ? "file" : scheme) {
            case "file" -> new FsComponents(
                    new LocalWriter(segmentFolderURI),
                    new LocalCleaner(),
                    () -> {}
            );
            case "oss" -> {
                OSSConfig ossConfig = config.getDeepStorageOssConfig();
                OSS ossClient = buildOSSClient(ossConfig);
                yield new FsComponents(
                        new OSSWriter(ossClient, segmentFolderURI, ossConfig.getWriterTaskNumber()),
                        new OSSCleaner(ossClient),
                        ossClient::shutdown
                );
            }
            case "s3" -> throw new UnsupportedOperationException("S3 is not yet implemented");
            case "gs" -> throw new UnsupportedOperationException("GCS is not yet implemented");
            default -> throw new IllegalArgumentException("Unknown FS scheme: " + scheme + " in URI: " + deepStorageURI);
        };
    }

    private static OSS buildOSSClient(OSSConfig config) {
        return new OSSClientBuilder().build(
                config.getEndpoint(),
                config.getAccessKeyId(),
                config.getAccessKeySecret()
        );
    }

    private static String resolveScheme(String uri) {
        try {
            return URI.create(uri).getScheme();
        } catch (IllegalArgumentException e) {
            return null;
        }
    }

    public record FsComponents(Writer writer, Cleaner cleaner, Runnable closer) implements Closeable {

        @Override
        public void close() throws IOException {
            closer.run();
        }
    }
}
