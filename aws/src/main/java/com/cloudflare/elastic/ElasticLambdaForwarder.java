/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.cloudflare.elastic;

import com.amazonaws.auth.AWSCredentials;
import com.amazonaws.services.lambda.runtime.LambdaLogger;
import com.amazonaws.services.lambda.runtime.RequestHandler;
import com.amazonaws.services.lambda.runtime.Context;
import com.amazonaws.services.lambda.runtime.events.S3Event;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3Client;
import com.amazonaws.services.s3.AmazonS3ClientBuilder;
import com.amazonaws.services.s3.event.S3EventNotification;
import com.amazonaws.services.s3.model.GetObjectRequest;
import com.amazonaws.services.s3.model.S3Object;
import com.amazonaws.services.s3.model.S3ObjectInputStream;

import org.apache.http.HttpHost;
import org.apache.http.auth.AuthScope;
import org.apache.http.auth.UsernamePasswordCredentials;
import org.apache.http.client.CredentialsProvider;
import org.apache.http.impl.client.BasicCredentialsProvider;
import org.apache.http.impl.nio.client.HttpAsyncClientBuilder;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.admin.cluster.health.ClusterHealthRequest;
import org.elasticsearch.action.admin.cluster.health.ClusterHealthResponse;
import org.elasticsearch.action.bulk.BackoffPolicy;
import org.elasticsearch.action.bulk.BulkProcessor;
import org.elasticsearch.action.bulk.BulkRequest;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestClientBuilder;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.cluster.health.ClusterHealthStatus;
import org.elasticsearch.common.unit.ByteSizeUnit;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.common.xcontent.XContentType;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.PrintWriter;
import java.io.StringWriter;
import java.nio.charset.StandardCharsets;
import java.util.concurrent.TimeUnit;
import java.util.function.BiConsumer;
import java.util.zip.GZIPInputStream;

public class ElasticLambdaForwarder implements RequestHandler<S3Event, Void>
{
    private static final String ENV_ELASTIC_HOST         = "elastic_hostname";
    private static final String ENV_ELASTIC_PORT         = "elastic_port";
    private static final String ENV_ELASTIC_INDEX        = "elastic_index";
    private static final String ENV_ELASTIC_USERNAME     = "elastic_username";
    private static final String ENV_ELASTIC_PASSWORD     = "elastic_password";
    private static final String ENV_ELASTIC_PIPELINE     = "elastic_pipeline";
    private static final String ENV_ELASTIC_HTTPS        = "elastic_use_https";
    private static final String ENV_ELASTIC_BULK_ACTIONS = "elastic_bulk_actions";
    private static final String ENV_ELASTIC_CONCURRENCY  = "elastic_bulk_concurrency";
    private static final String ENV_ELASTIC_DEBUG        = "elastic_debug";
    private static final String ENV_AWS_ACCESS_KEY       = "aws_access_key";
    private static final String ENV_AWS_SECRET_KEY       = "aws_secret_key";

    private LambdaLogger logger = null;

    private static int    BULK_ACTIONS         = 100;
    private static int    BULK_CONCURRENCY     = 2;
    private static int    ELASTIC_PORT         = 9243;
    private static String ELASTIC_INDEX        = "cloudflare";
    private static String ELASTIC_PIPELINE     = "cloudflare-pipeline-weekly";
    private static String ELASTIC_HOSTNAME     = null;
    private static String ELASTIC_USERNAME     = null;
    private static String ELASTIC_PASSWORD     = null;
    private static boolean ELASTIC_HTTPS       = true;
    private static boolean ELASTIC_DEBUG       = false;
    private static boolean USE_AWS_CREDENTIALS = false;

    RestHighLevelClient es;

    @Override
    public Void handleRequest(S3Event event, Context context)
    {
        logger = context.getLogger();

        try {
            initialize();

            logger.log(String.format(
                    "Parameters: hostname [%s:%d] index [%s], username [%s], pipeline [%s], ssl/tls [%b]",
                    ELASTIC_HOSTNAME, ELASTIC_PORT, ELASTIC_INDEX, ELASTIC_USERNAME, ELASTIC_PIPELINE, ELASTIC_HTTPS));

            if (es == null) {
                es = client(ELASTIC_HOSTNAME, ELASTIC_PORT, ELASTIC_USERNAME, ELASTIC_PASSWORD, ELASTIC_HTTPS);
            }

            AmazonS3 s3Client = getS3Client();
            BulkProcessor processor = processor(es, BULK_ACTIONS, BULK_CONCURRENCY);

            try {
                process(es, s3Client, event, processor, ELASTIC_INDEX, ELASTIC_PIPELINE);
            }
            finally {
                logger.log("Finished processing; flushing any remaining logs...");
                processor.flush();
                processor.awaitClose(60L, TimeUnit.SECONDS);
                logger.log("Elasticsearch processing done");
            }
        }
        catch (Exception e) {
            logger.log(String.format("%s\n%s", e.getMessage(), trace(e)));
            try {
                if (es != null) {
                    es.close();
                }
            } catch (IOException ignored) {

            } finally {
                es = null;
            }
        }

        return null;
    }

    private void process(RestHighLevelClient es, AmazonS3 s3Client, S3Event event, BulkProcessor processor, String index, String pipeline)
    {
        if (ELASTIC_DEBUG) {
            logger.log("S3 event record: " + event.toJson());
        }

        for (S3EventNotification.S3EventNotificationRecord record : event.getRecords())
        {
            final String key    = record.getS3().getObject().getKey();
            final String bucket = record.getS3().getBucket().getName();

            logger.log("Processing: " + bucket + ":" + key);
            S3Object object = s3Client.getObject(new GetObjectRequest(bucket, key));

            S3ObjectInputStream s3stream = object.getObjectContent();

            try (GZIPInputStream gzip = new GZIPInputStream(s3stream);
                    BufferedReader br = new BufferedReader(new InputStreamReader(gzip, StandardCharsets.UTF_8)))
            {
                for (String line; (line = br.readLine()) != null;) {
                    processor.add(new IndexRequest(index).setPipeline(pipeline).source(line, XContentType.JSON));
                }
            }
            catch (IOException e) {
                e.printStackTrace();
            }
        }
    }

    private BulkProcessor processor(RestHighLevelClient client, int bulkActions, int bulkConcurrency)
    {
        BulkProcessor.Listener listener = new BulkProcessor.Listener() {
            @Override
            public void beforeBulk(long executionId, BulkRequest request) {
                logger.log(String.format("Flushing [%s] logs to elasticsearch", request.numberOfActions()));
            }

            @Override
            public void afterBulk(long executionId, BulkRequest request, BulkResponse response) {
                if (response.hasFailures()) {
                    logger.log(response.buildFailureMessage());
                }
            }

            @Override
            public void afterBulk(long executionId, BulkRequest request, Throwable failure) {
                logger.log(String.format("%s\n%s", failure.getMessage(), trace(failure)));
            }
        };

        BiConsumer<BulkRequest, ActionListener<BulkResponse>> bulkConsumer =
                (request, bulkListener) ->
                        client.bulkAsync(request, RequestOptions.DEFAULT, bulkListener);

        BulkProcessor.Builder builder = BulkProcessor.builder(bulkConsumer, listener);

        builder.setBulkActions(bulkActions);
        builder.setBulkSize(new ByteSizeValue(10L, ByteSizeUnit.MB));
        builder.setConcurrentRequests(bulkConcurrency);
        builder.setFlushInterval(TimeValue.timeValueSeconds(2L));
        builder.setBackoffPolicy(
                BackoffPolicy.exponentialBackoff(TimeValue.timeValueSeconds(1), 10));

        return builder.build();
    }

    private RestHighLevelClient client(String endpoint, int port, String username, String password, boolean https) throws IOException
    {
        CredentialsProvider provider = new BasicCredentialsProvider();
        provider.setCredentials(AuthScope.ANY, new UsernamePasswordCredentials(username, password));

        RestHighLevelClient client = new RestHighLevelClient(
                RestClient.builder(
                        new HttpHost(endpoint, port, https ? "https" : "http")).
                        setHttpClientConfigCallback(
                                new RestClientBuilder.HttpClientConfigCallback() {
                                    @Override
                                    public HttpAsyncClientBuilder customizeHttpClient(HttpAsyncClientBuilder builder)
                                    {
                                        return builder.setDefaultCredentialsProvider(provider);
                                    }}));

        // Verify connection
        ClusterHealthResponse health = client.cluster().health(new ClusterHealthRequest(), RequestOptions.DEFAULT);
        ClusterHealthStatus status = health.getStatus();
        logger.log("Connected to cluster: [" + health.getClusterName() + "] status: [" + status + "]");

        if (status == ClusterHealthStatus.RED) {
            throw new RuntimeException("Elasticsearch cluster is in RED state; aborting");
        }

        return client;
    }

    private AmazonS3 getS3Client()
    {
        if (USE_AWS_CREDENTIALS) {
            return new AmazonS3Client(new AWSCredentials() {
                @Override
                public String getAWSAccessKeyId() {
                    return System.getenv(ENV_AWS_ACCESS_KEY);
                }

                @Override
                public String getAWSSecretKey() {
                    return System.getenv(ENV_AWS_SECRET_KEY);
                }
            });
        }
        else {
            return AmazonS3ClientBuilder.defaultClient();
        }
    }

    private void initialize()
    {
        String bulk = System.getenv(ENV_ELASTIC_BULK_ACTIONS);
        if (bulk != null) {
            BULK_ACTIONS = Integer.parseInt(bulk);
        }

        String concurrency = System.getenv(ENV_ELASTIC_CONCURRENCY);
        if (concurrency != null) {
            BULK_CONCURRENCY = Integer.parseInt(concurrency);
        }

        String port = System.getenv(ENV_ELASTIC_PORT);
        if (port != null) {
            ELASTIC_PORT = Integer.parseInt(port);
        }

        String index = System.getenv(ENV_ELASTIC_INDEX);
        if (index != null && !index.isEmpty()) {
            ELASTIC_INDEX = index;
        }

        String pipeline = System.getenv(ENV_ELASTIC_PIPELINE);
        if (pipeline != null && !pipeline.isEmpty()) {
            ELASTIC_PIPELINE = pipeline;
        }

        String https = System.getenv(ENV_ELASTIC_HTTPS);
        if (https != null) {
            ELASTIC_HTTPS = Boolean.parseBoolean(https);
        }

        String debug = System.getenv(ENV_ELASTIC_DEBUG);
        if (debug != null) {
            ELASTIC_DEBUG = Boolean.parseBoolean(debug);
        }

        ELASTIC_HOSTNAME = System.getenv(ENV_ELASTIC_HOST);
        if (ELASTIC_HOSTNAME == null || ELASTIC_HOSTNAME.isEmpty()) {
            throw new RuntimeException("Elastic hostname not set; please set '" + ENV_ELASTIC_HOST + "'");
        }

        ELASTIC_USERNAME = System.getenv(ENV_ELASTIC_USERNAME);
        if (ELASTIC_USERNAME == null || ELASTIC_USERNAME.isEmpty()) {
            throw new RuntimeException("Elastic username not set; please set '" + ENV_ELASTIC_USERNAME + "'");
        }

        ELASTIC_PASSWORD = System.getenv(ENV_ELASTIC_PASSWORD);
        if (ELASTIC_PASSWORD == null || ELASTIC_PASSWORD.isEmpty()) {
            throw new RuntimeException("Elastic password not set; please set '" + ENV_ELASTIC_PASSWORD + "'");
        }

        if (System.getenv(ENV_AWS_ACCESS_KEY) != null && System.getenv(ENV_AWS_SECRET_KEY) != null) {
            USE_AWS_CREDENTIALS = true;
        }
    }

    private String trace(Throwable t)
    {
        StringWriter sw = new StringWriter();
        t.printStackTrace(new PrintWriter(sw));
        return t.toString();
    }
}
