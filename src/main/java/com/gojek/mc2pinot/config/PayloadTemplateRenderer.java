package com.gojek.mc2pinot.config;

import com.gojek.mc2pinot.metrics.SegmentPayloadContext;
import gg.jte.ContentType;
import gg.jte.TemplateEngine;
import gg.jte.output.StringOutput;
import gg.jte.resolve.DirectoryCodeResolver;

import java.io.IOException;
import java.nio.file.Path;

public class PayloadTemplateRenderer {

    private final String templatePath;

    public PayloadTemplateRenderer(String templatePath) {
        this.templatePath = templatePath;
    }

    public String render(SegmentPayloadContext context) throws IOException {
        if (templatePath == null || templatePath.isBlank()) {
            return "{}";
        }

        Path templateFile = Path.of(templatePath).toAbsolutePath();
        Path templateDir = templateFile.getParent();
        String templateName = templateFile.getFileName().toString();

        DirectoryCodeResolver codeResolver = new DirectoryCodeResolver(templateDir);
        TemplateEngine engine = TemplateEngine.create(
                codeResolver,
                templateDir,
                ContentType.Plain,
                PayloadTemplateRenderer.class.getClassLoader()
        );

        StringOutput output = new StringOutput();
        engine.render(templateName, context, output);
        return output.toString();
    }
}

