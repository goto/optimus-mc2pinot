package com.gojek.mc2pinot.oss;

/**
 * @deprecated Moved to {@link com.gojek.mc2pinot.io.oss.OSSReader}.
 */
@Deprecated
public class OSSReader extends com.gojek.mc2pinot.io.oss.OSSReader {
    public OSSReader(com.aliyun.oss.OSS ossClient, String ossURI) {
        super(ossClient, ossURI);
    }
}
