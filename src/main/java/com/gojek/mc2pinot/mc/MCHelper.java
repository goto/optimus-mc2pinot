package com.gojek.mc2pinot.mc;

import com.aliyun.odps.Instance;
import com.aliyun.odps.Odps;
import com.aliyun.odps.OdpsException;

import java.net.URI;
import java.net.URISyntaxException;
import java.net.URLDecoder;
import java.net.URLEncoder;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.stream.Collectors;

public class MCHelper {
    private static final String LOGVIEW_HOST = "http://service.id-all.maxcompute.aliyun-inc.com/api";

    public static String generateLogView(Odps odpsClient, Instance instance) throws OdpsException {
        String rawUrl = odpsClient.logview().generateLogView(instance, 24);
        try {
            URI uri = new URI(rawUrl);
            String rewrittenQuery = Arrays.stream(uri.getRawQuery().split("&"))
                    .map(param -> {
                        String[] parts = param.split("=", 2);
                        String key = URLDecoder.decode(parts[0], StandardCharsets.UTF_8);
                        if ("h".equals(key)) {
                            return "h=" + URLEncoder.encode(LOGVIEW_HOST, StandardCharsets.UTF_8);
                        }
                        return param;
                    })
                    .collect(Collectors.joining("&"));

            String base = uri.getScheme() + "://" + uri.getAuthority() + uri.getPath();
            return base + "?" + rewrittenQuery;
        } catch (URISyntaxException e) {
            throw new OdpsException("Failed to rewrite logview URL: " + rawUrl, e);
        }
    }
}
