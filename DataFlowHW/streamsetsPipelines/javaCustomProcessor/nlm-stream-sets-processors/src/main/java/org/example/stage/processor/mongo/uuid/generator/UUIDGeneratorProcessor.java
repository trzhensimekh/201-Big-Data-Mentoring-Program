
package org.example.stage.processor.mongo.uuid.generator;

import com.streamsets.pipeline.api.Field;
import com.streamsets.pipeline.api.Record;
import com.streamsets.pipeline.api.StageException;
import com.streamsets.pipeline.api.base.SingleLaneRecordProcessor;
import org.example.stage.lib.sample.Errors;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.Map;
import java.util.UUID;

public abstract class UUIDGeneratorProcessor extends SingleLaneRecordProcessor {
    private static final Logger LOG = LoggerFactory.getLogger(UUIDGeneratorProcessor.class);

    /**
     * Gives access to the UI configuration of the stage provided by the {@link SampleDProcessor} class.
     */
    public abstract String getConfig();

    /**
     * {@inheritDoc}
     */
    @Override
    protected List<ConfigIssue> init() {
        // Validate configuration values and open any required resources.
        List<ConfigIssue> issues = super.init();

        if (getConfig().equals("invalidValue")) {
            issues.add(
                    getContext().createConfigIssue(
                            Groups.SAMPLE.name(), "config", Errors.SAMPLE_00, "Here's what's wrong..."
                    )
            );
        }

        // If issues is not empty, the UI will inform the user of each configuration issue in the list.
        return issues;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void destroy() {
        // Clean up any open resources.
        super.destroy();
    }

    /**
     * Generates UUID based on unique data  and adds it in the record
     *
     * @param record     the record to process
     * @param batchMaker to add the record
     * @throws StageException if had an error while processing records
     */
    @Override
    protected void process(Record record, SingleLaneBatchMaker batchMaker) throws StageException {
        LOG.info("Processing a record: {}", record);

        final Field uniqueData = record.get("/article/PubmedData/ArticleIdList/ArticleId[1]");

        final Map<String, Field> articleIdMap = uniqueData.getValueAsMap();
        StringBuilder sb = new StringBuilder();
        for (String key : articleIdMap.keySet()) {
            sb.append(key);
            sb.append(articleIdMap.get(key).getValueAsString());
        }

        final String stringToGenerateUUID = sb.toString();
        final String ArticleUUID = UUID.nameUUIDFromBytes(stringToGenerateUUID.getBytes()).toString();
        record.set("/_id", Field.create(ArticleUUID));
        LOG.info("UUID: {} has been generated for Article by the Field: {}", ArticleUUID, uniqueData);
        LOG.info("A new Field: {} has been added to the Record", "_id");
        batchMaker.addRecord(record);
    }
}


