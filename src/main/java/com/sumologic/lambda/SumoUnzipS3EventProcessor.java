/**
 *
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package com.sumologic.lambda;

import com.amazonaws.services.lambda.runtime.Context;
import com.amazonaws.services.lambda.runtime.RequestHandler;
import com.amazonaws.services.lambda.runtime.events.S3Event;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3Client;
import com.amazonaws.services.s3.event.S3EventNotification;
import com.amazonaws.services.s3.model.GetObjectRequest;
import com.amazonaws.services.s3.model.S3Object;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.sumologic.model.SumoEndpointConfig;
import org.apache.commons.io.FilenameUtils;
import org.apache.commons.io.IOUtils;
import org.apache.http.HttpResponse;
import org.apache.http.client.HttpClient;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.entity.ContentType;
import org.apache.http.entity.StringEntity;
import org.apache.http.impl.client.HttpClientBuilder;

import java.io.IOException;
import java.net.URLDecoder;
import java.nio.charset.Charset;
import java.util.zip.ZipEntry;
import java.util.zip.ZipInputStream;

public class SumoUnzipS3EventProcessor implements RequestHandler<S3Event, String> {

    private static final String ENDPOINT_CONFIG_FILE_NAME = "sumo-endpoint-config.json";
    private AmazonS3 s3Client = new AmazonS3Client();

    public String handleRequest(S3Event s3Event, Context context) {
        s3Event.getRecords().forEach(this::processRecord);
        return "done processing request";
    }

    private void processRecord(S3EventNotification.S3EventNotificationRecord record) {
        try {
            String bucketName = record.getS3().getBucket().getName();
            String fileName = getFileName(record);
            if (!isZipObject(fileName)) {
                System.out.println("skipping non zip file for key: " + fileName);
                return;
            }
            S3Object s3Object = s3Client.getObject(new GetObjectRequest(bucketName, fileName));
            processS3Object(s3Object);

        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    private String getFileName(S3EventNotification.S3EventNotificationRecord record) throws IOException {
        String fileName = record.getS3().getObject().getKey().replace('+', ' ');
        return URLDecoder.decode(fileName, "UTF-8");
    }

    private boolean isZipObject(String fileName) throws IOException {
        String extension = FilenameUtils.getExtension(fileName);
        return extension.equals("zip");
    }

    private void processS3Object(S3Object s3Object) throws IOException {
        byte[] buffer = new byte[1024];
        ZipInputStream zipInputStream = new ZipInputStream(s3Object.getObjectContent());
        ZipEntry zipEntry = zipInputStream.getNextEntry();
        int read;
        while (zipEntry != null) {
            StringBuilder data = new StringBuilder();
            while ((read = zipInputStream.read(buffer, 0, 1024)) >= 0) {
                data.append(new String(buffer, 0, read));
            }
            sendToSumo(data.toString(), getConfig(s3Object.getBucketName()));
            zipEntry = zipInputStream.getNextEntry();
        }
    }

    private void sendToSumo(String data, SumoEndpointConfig config) throws IOException {
        HttpClient client = HttpClientBuilder.create().build();
        HttpPost post = new HttpPost(config.getEndpoint());
        post.setEntity(new StringEntity(data, ContentType.TEXT_PLAIN));
        HttpResponse response = client.execute(post);
        int statusCode = response.getStatusLine().getStatusCode();
        System.out.println(String.format("Received HTTP Status Code from Sumo Service: %d", statusCode));
    }

    private SumoEndpointConfig getConfig(String bucketName) throws IOException {
        S3Object config = s3Client.getObject(new GetObjectRequest(bucketName, ENDPOINT_CONFIG_FILE_NAME));
        String jsonInString = IOUtils.toString(config.getObjectContent(), Charset.defaultCharset());
        ObjectMapper mapper = new ObjectMapper();
        return mapper.readValue(jsonInString, SumoEndpointConfig.class);
    }
}