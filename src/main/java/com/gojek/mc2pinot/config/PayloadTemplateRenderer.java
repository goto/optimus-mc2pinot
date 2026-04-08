package com.gojek.mc2pinot.config;

import com.gojek.mc2pinot.metrics.SegmentPayloadContext;
import freemarker.template.Configuration;
import freemarker.template.Template;
import freemarker.template.TemplateException;
import freemarker.template.TemplateExceptionHandler;

import java.io.File;
import java.io.IOException;
import java.io.StringWriter;
import java.nio.file.Path;
import java.util.HashMap;
import java.util.Map;

public class PayloadTemplateRenderer {

    private final Template template;

    public PayloadTemplateRenderer(String templatePath) throws IOException {
        if (templatePath == null || templatePath.isBlank()) {
            this.template = null;
            return;
        }

        Path templateFile = Path.of(templatePath).toAbsolutePath();
        File templateDir = templateFile.getParent().toFile();
        String templateName = templateFile.getFileName().toString();

        Configuration cfg = new Configuration(Configuration.VERSION_2_3_33);
        cfg.setDirectoryForTemplateLoading(templateDir);
        cfg.setNumberFormat("0");
        cfg.setDefaultEncoding("UTF-8");
        cfg.setTemplateExceptionHandler(TemplateExceptionHandler.RETHROW_HANDLER);
        cfg.setLogTemplateExceptions(false);

        this.template = cfg.getTemplate(templateName);
    }

    public String render(SegmentPayloadContext context) throws IOException {
        if (template == null) {
            return "{}";
        }

        Map<String, Object> model = new HashMap<>();
        model.put("inputRecordCount", context.inputRecordCount());
        model.put("inputRecordSize", context.inputRecordSize());
        model.put("segmentName", context.segmentName());
        model.put("outputRecordCount", context.outputRecordCount());
        model.put("outputRecordSize", context.outputRecordSize());

        StringWriter writer = new StringWriter();
        try {
            template.process(model, writer);
        } catch (TemplateException e) {
            throw new IOException("Failed to process FreeMarker template", e);
        }
        return writer.toString();
    }
}

