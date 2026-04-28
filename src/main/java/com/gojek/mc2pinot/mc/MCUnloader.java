package com.gojek.mc2pinot.mc;

import com.aliyun.odps.Instance;
import com.aliyun.odps.Odps;
import com.aliyun.odps.OdpsException;
import com.aliyun.odps.task.SQLTask;

import java.util.Map;
import java.util.logging.Logger;

public class MCUnloader {

    private static final Logger LOG = Logger.getLogger(MCUnloader.class.getName());

    private volatile Instance runningInstance;
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

    public void kill() {
        Instance instance = runningInstance;
        if (instance == null) {
            return;
        }
        try {
            LOG.info("source(mc): killing running instance due to shutdown signal");
            instance.stop();
            LOG.info("source(mc): instance killed successfully");
        } catch (OdpsException e) {
            LOG.warning("source(mc): failed to kill instance: " + e.getMessage());
        }
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
        runningInstance = instance;
        try {
            String logView = MCHelper.generateLogView(odpsClient, instance);
            LOG.info("source(mc): logview: " + logView);
            instance.waitForSuccess();
        } finally {
            runningInstance = null;
        }
    }
}

