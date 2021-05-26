
package org.example.stage.processor.mongo.uuid.generator;

import com.streamsets.pipeline.api.ConfigDef;
import com.streamsets.pipeline.api.ConfigGroups;
import com.streamsets.pipeline.api.GenerateResourceBundle;
import com.streamsets.pipeline.api.StageDef;

@StageDef(
        version = 1,
        label = "UUID Generator Processor",
        description = "",
        icon = "default.png",
        onlineHelpRefUrl = ""
)
@ConfigGroups(Groups.class)
@GenerateResourceBundle
public class SampleDProcessor extends UUIDGeneratorProcessor {

    @ConfigDef(
            required = true,
            type = ConfigDef.Type.STRING,
            defaultValue = "default",
            label = "UUID Generator Processor",
            displayPosition = 10,
            group = "SAMPLE"
    )
    public String config;

    /**
     * {@inheritDoc}
     */
    @Override
    public String getConfig() {
        return config;
    }

}
