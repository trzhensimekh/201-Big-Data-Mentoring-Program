
package org.example.stage.processor.aws.s3.reader;

import com.streamsets.pipeline.api.Field;
import org.example.stage.lib.sample.Errors;

import com.streamsets.pipeline.api.Record;
import com.streamsets.pipeline.api.StageException;
import com.streamsets.pipeline.api.base.SingleLaneRecordProcessor;
import org.json.JSONArray;
import org.json.JSONObject;
import org.json.XML;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.util.List;

public abstract class AwsS3ReaderProcessor extends SingleLaneRecordProcessor {
    private static final Logger LOG = LoggerFactory.getLogger(AwsS3ReaderProcessor.class);

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
     * Gets coordinates to {@link com.amazonaws.services.s3.model.S3Object}  from the previous stage, a.k.a region, bucket name, key
     * Parses XML data from S3Object to JSON
     * Adds new records with parsed data
     *
     * @param record     the record to process
     * @param batchMaker to add the record
     * @throws StageException if had an error while processing records
     */
    @Override
    protected void process(Record record, SingleLaneBatchMaker batchMaker) throws StageException {
        LOG.info("Processing a record: {}", record);

        final String awsRegion = record.get("/Records[0]/awsRegion").getValueAsString();
        final String bucketName = record.get("/Records[0]/s3/bucket/name").getValueAsString();
        final String key = record.get("/Records[0]/s3/object/key").getValueAsString();

        final InputStream inputStream = S3Reader.getS3ObjectDataStream(awsRegion, bucketName, key);
        String xmlAsString = "";
        try {
            xmlAsString = getInputStreamAsString(inputStream);
        } catch (IOException e) {
            LOG.error("can not read from InputStream", e);
        }

       final JSONObject jsonObj = XML.toJSONObject(xmlAsString);
       final JSONObject pubMedList = jsonObj.getJSONObject("PubmedArticleSet");
       final JSONArray array = pubMedList.getJSONArray("PubmedArticle");

        for (int i = 0; i < array.length(); i++) {
            Record newRecord = getContext().createRecord(record.getHeader().getSourceId());
            newRecord.set(Field.create(array.get(i).toString()));
            batchMaker.addRecord(newRecord);
        }
    }

    private String getInputStreamAsString(InputStream inputStream) throws IOException {
        BufferedReader reader = new BufferedReader(new InputStreamReader(inputStream));
        StringBuilder sb = new StringBuilder();
        String line;
        while ((line = reader.readLine()) != null) {
            sb.append(line);
        }
        return sb.toString();
    }
}


