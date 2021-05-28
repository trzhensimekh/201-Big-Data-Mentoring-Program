
package org.example.stage.processor.mongo.writer;

import com.streamsets.pipeline.api.ConfigDef;
import com.streamsets.pipeline.api.ConfigGroups;
import com.streamsets.pipeline.api.GenerateResourceBundle;
import com.streamsets.pipeline.api.StageDef;

@StageDef(
        version = 1,
        label = "MongoDB Writer",
        description = "",
        icon = "default.png",
        onlineHelpRefUrl = ""
)
@ConfigGroups(Groups.class)
@GenerateResourceBundle
public class SampleDProcessor extends MongoDBWriter {

    @ConfigDef(
            required = true,
            type = ConfigDef.Type.STRING,
            defaultValue = "default",
            label = "MongoDB Writer",
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
