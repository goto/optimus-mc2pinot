package com.gojek.mc2pinot.oss;

/**
 * @deprecated Moved to {@link com.gojek.mc2pinot.io.oss.OSSWriter}.
 */
@Deprecated
public class OSSWriter extends com.gojek.mc2pinot.io.oss.OSSWriter {
    public OSSWriter(com.aliyun.oss.OSS ossClient, String ossURI) {
        super(ossClient, ossURI);
    }
}
