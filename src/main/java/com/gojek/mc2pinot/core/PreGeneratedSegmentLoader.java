package com.gojek.mc2pinot.core;

import com.aliyun.oss.OSS;
import com.aliyun.oss.model.ListObjectsRequest;
import com.aliyun.oss.model.OSSObjectSummary;
import com.aliyun.oss.model.ObjectListing;
import org.apache.commons.compress.archivers.tar.TarArchiveEntry;
import org.apache.commons.compress.archivers.tar.TarArchiveInputStream;
import org.apache.commons.compress.archivers.tar.TarArchiveOutputStream;
import org.apache.commons.compress.compressors.gzip.GzipCompressorInputStream;
import org.apache.commons.compress.compressors.gzip.GzipCompressorOutputStream;

import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.URI;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.logging.Logger;

/**
 * Loads segments that were <em>already generated</em> and staged in an OSS folder, skipping the
 * Maxcompute unload and segment-generation phases entirely.
 *
 * <p>For every {@code <segmentName>.tar.gz} under the configured bucket path this:
 * <ol>
 *   <li>streams the object and extracts only {@code metadata.properties} and {@code creation.meta}
 *       (a few KB) — the full segment is never downloaded to disk;</li>
 *   <li>reads the authoritative {@code segment.name} and {@code segment.total.docs} from the
 *       metadata;</li>
 *   <li>repackages the two files into a {@code <segmentName>.metadata.tar.gz} (flattened under
 *       {@code <segmentName>/}), matching {@link PinotSegmenter}'s METADATA-upload sidecar;</li>
 *   <li>records the OSS object URI as the segment's deep-store {@code DOWNLOAD_URI}.</li>
 * </ol>
 *
 * The resulting {@link SegmentInfo}s feed the same uploader as a freshly generated run.
 */
public class PreGeneratedSegmentLoader {

    private static final Logger LOG = Logger.getLogger(PreGeneratedSegmentLoader.class.getName());

    private static final Set<String> TARGET_FILES = Set.of("metadata.properties", "creation.meta");
    private static final String SEGMENT_SUFFIX = ".tar.gz";
    private static final String METADATA_SUFFIX = ".metadata.tar.gz";

    private final OSS ossClient;
    private final String bucketURI;
    private final Path workDir;

    /**
     * @param ossClient authenticated OSS client (built from the deep-storage OSS credentials)
     * @param bucketURI {@code oss://<bucket>/<prefix>} folder holding the segment tarballs
     * @param workDir   local directory where the generated metadata tarballs are written
     */
    public PreGeneratedSegmentLoader(OSS ossClient, String bucketURI, Path workDir) {
        this.ossClient = ossClient;
        this.bucketURI = bucketURI;
        this.workDir = workDir;
    }

    public GenerationResult load() throws IOException {
        URI uri = URI.create(bucketURI);
        String scheme = uri.getScheme();
        if (scheme == null || !"oss".equals(scheme)) {
            throw new IllegalArgumentException(
                    "Segment generation skip only supports oss:// bucket paths, got: " + bucketURI);
        }
        String bucket = uri.getHost();
        String prefix = uri.getPath().substring(1);
        if (!prefix.isEmpty() && !prefix.endsWith("/")) {
            prefix = prefix + "/";
        }

        List<SegmentInfo> segments = new ArrayList<>();
        String marker = null;
        boolean hasMore = true;

        while (hasMore) {
            ListObjectsRequest request = new ListObjectsRequest(bucket);
            request.setPrefix(prefix);
            request.setMaxKeys(1000);
            if (marker != null) {
                request.setMarker(marker);
            }

            ObjectListing listing = ossClient.listObjects(request);
            for (OSSObjectSummary summary : listing.getObjectSummaries()) {
                String key = summary.getKey();
                if (!key.endsWith(SEGMENT_SUFFIX) || key.endsWith(METADATA_SUFFIX)) {
                    // Skip directory markers, non-segment objects and any metadata sidecars.
                    continue;
                }
                segments.add(loadSegment(bucket, key, summary.getSize()));
            }

            hasMore = listing.isTruncated();
            marker = listing.getNextMarker();
        }

        if (segments.isEmpty()) {
            throw new IOException("No segment " + SEGMENT_SUFFIX + " files found under " + bucketURI);
        }
        LOG.info("transient(core): loaded " + segments.size() + " pre-generated segment(s) from " + bucketURI);
        // Input record count/size are generation-time metrics that do not exist in skip mode.
        return new GenerationResult(segments, 0L, 0L);
    }

    private SegmentInfo loadSegment(String bucket, String key, long tarSize) throws IOException {
        String remoteURI = "oss://" + bucket + "/" + key;
        LOG.info("transient(oss): reading metadata from pre-generated segment " + remoteURI);

        Map<String, byte[]> collected = extractTargetFiles(bucket, key);
        byte[] metadataBytes = collected.get("metadata.properties");
        if (metadataBytes == null) {
            throw new IOException("metadata.properties not found inside " + remoteURI);
        }

        Properties props = new Properties();
        props.load(new java.io.ByteArrayInputStream(metadataBytes));
        String segmentName = props.getProperty("segment.name");
        if (segmentName == null || segmentName.isBlank()) {
            // Fall back to the file name (minus .tar.gz) when the property is absent.
            String fileName = key.substring(key.lastIndexOf('/') + 1);
            segmentName = fileName.substring(0, fileName.length() - SEGMENT_SUFFIX.length());
        }
        long totalDocs = parseLong(props.getProperty("segment.total.docs"));

        if (!collected.containsKey("creation.meta")) {
            LOG.warning("transient(oss): creation.meta not found in " + remoteURI + " (continuing)");
        }

        Path metadataTar = workDir.resolve(segmentName + METADATA_SUFFIX);
        writeMetadataTarGz(segmentName, collected, metadataTar.toFile());
        LOG.info("transient(core): staged metadata tarball for segment " + segmentName
                + " (" + totalDocs + " docs, tar " + tarSize + " bytes)");

        return new SegmentInfo(segmentName, remoteURI, null, metadataTar, totalDocs, tarSize);
    }

    /**
     * Streams the OSS segment tar and pulls out only {@code metadata.properties} and
     * {@code creation.meta}, stopping as soon as both are found. The full segment is never
     * materialised on disk.
     */
    private Map<String, byte[]> extractTargetFiles(String bucket, String key) throws IOException {
        Map<String, byte[]> result = new LinkedHashMap<>();
        try (InputStream objectStream = ossClient.getObject(bucket, key).getObjectContent();
             InputStream bis = new BufferedInputStream(objectStream);
             GzipCompressorInputStream gzis = new GzipCompressorInputStream(bis);
             TarArchiveInputStream tais = new TarArchiveInputStream(gzis)) {

            TarArchiveEntry entry;
            while ((entry = tais.getNextEntry()) != null) {
                if (entry.isDirectory()) {
                    continue;
                }
                String fileName = new File(entry.getName()).getName();
                if (TARGET_FILES.contains(fileName)) {
                    result.put(fileName, readAllBytes(tais));
                    if (result.keySet().containsAll(TARGET_FILES)) {
                        break;
                    }
                }
            }
        }
        return result;
    }

    /** Mirrors {@link PinotSegmenter}'s metadata sidecar: files flattened under {@code <segmentName>/}. */
    private void writeMetadataTarGz(String segmentName, Map<String, byte[]> files, File metadataTar)
            throws IOException {
        try (OutputStream fos = new BufferedOutputStream(new FileOutputStream(metadataTar));
             GzipCompressorOutputStream gzos = new GzipCompressorOutputStream(fos);
             TarArchiveOutputStream taos = new TarArchiveOutputStream(gzos)) {
            taos.setLongFileMode(TarArchiveOutputStream.LONGFILE_GNU);

            taos.putArchiveEntry(new TarArchiveEntry(segmentName + "/"));
            taos.closeArchiveEntry();

            for (Map.Entry<String, byte[]> file : files.entrySet()) {
                TarArchiveEntry fileEntry = new TarArchiveEntry(segmentName + "/" + file.getKey());
                fileEntry.setSize(file.getValue().length);
                taos.putArchiveEntry(fileEntry);
                taos.write(file.getValue());
                taos.closeArchiveEntry();
            }
        }
    }

    private static byte[] readAllBytes(InputStream in) throws IOException {
        ByteArrayOutputStream buffer = new ByteArrayOutputStream();
        byte[] chunk = new byte[8192];
        int n;
        while ((n = in.read(chunk)) != -1) {
            buffer.write(chunk, 0, n);
        }
        return buffer.toByteArray();
    }

    private static long parseLong(String value) {
        if (value == null || value.isBlank()) {
            return 0L;
        }
        try {
            return Long.parseLong(value.trim());
        } catch (NumberFormatException e) {
            return 0L;
        }
    }
}
