package org.example.stage.processor.aws.s3.reader;

import com.amazonaws.regions.Regions;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3ClientBuilder;
import com.amazonaws.services.s3.model.GetObjectRequest;
import com.amazonaws.services.s3.model.S3Object;
import com.amazonaws.services.s3.model.S3ObjectInputStream;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class S3Reader {
    private static final Logger LOG = LoggerFactory.getLogger(S3Reader.class);

    /**
     * Reads data from AWS S3 bucket by the given coordinates
     * @param region  where the bucket is located
     * @param bucketName where to read
     * @param key path to the file
     * @return {@link S3ObjectInputStream}
     */
    static S3ObjectInputStream getS3ObjectDataStream(String region, String bucketName, String key) {
        S3Object fullObject;

        AmazonS3 s3Client = AmazonS3ClientBuilder.standard()
                .withRegion(Regions.fromName(region))
                .build();

        fullObject = s3Client.getObject(new GetObjectRequest(bucketName, key));
        LOG.info("Successful get S3Object from s3: {}", key);
        return fullObject.getObjectContent();
    }
}
