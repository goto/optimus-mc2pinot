package com.gojek.mc2pinot.mc;

import com.aliyun.odps.Instance;
import com.aliyun.odps.Odps;
import com.aliyun.odps.OdpsException;
import com.aliyun.odps.task.SQLTask;

import java.net.URI;
import java.net.URISyntaxException;
import java.net.URLDecoder;
import java.net.URLEncoder;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.Map;
import java.util.logging.Logger;
import java.util.stream.Collectors;

public class MCUnloader {

    private static final Logger LOG = Logger.getLogger(MCUnloader.class.getName());

    private final Odps odpsClient;
    private final QueryUnloadBuilder queryUnloadBuilder;
    private final String ossRoleARN;
    private final String outputFormat;

    public MCUnloader(Odps odpsClient, QueryUnloadBuilder queryUnloadBuilder,
                      String ossRoleARN, String outputFormat) {
        this.odpsClient = odpsClient;
        this.queryUnloadBuilder = queryUnloadBuilder;
        this.ossRoleARN = ossRoleARN;
        this.outputFormat = outputFormat;
    }

    public void unload(String query, String ossDestinationURI) throws OdpsException {
        LOG.info("source(mc): trigger unload");

        String unloadSQL = queryUnloadBuilder
                .setQuery(query)
                .setRoleARN(ossRoleARN)
                .setOSSDestinationURI(ossDestinationURI)
                .setOutputFormat(outputFormat)
                .toString();

        Map<String, String> hints = queryUnloadBuilder.getHints();

        LOG.info("source(mc): running query:\n" + unloadSQL);

        Instance instance = SQLTask.run(odpsClient, odpsClient.getDefaultProject(), unloadSQL, hints, null);
        String logView = MCHelper.generateLogView(odpsClient, instance);
        LOG.info("source(mc): logview: " + logView);
        instance.waitForSuccess();
    }
}

