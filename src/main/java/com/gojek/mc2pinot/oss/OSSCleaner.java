package com.gojek.mc2pinot.oss;

/**
 * @deprecated Moved to {@link com.gojek.mc2pinot.io.oss.OSSCleaner}.
 */
@Deprecated
public class OSSCleaner extends com.gojek.mc2pinot.io.oss.OSSCleaner {
    public OSSCleaner(com.aliyun.oss.OSS ossClient) {
        super(ossClient);
    }
}
