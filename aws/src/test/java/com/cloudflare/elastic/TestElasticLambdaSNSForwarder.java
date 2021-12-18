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

import com.amazonaws.services.lambda.runtime.ClientContext;
import com.amazonaws.services.lambda.runtime.CognitoIdentity;
import com.amazonaws.services.lambda.runtime.Context;
import com.amazonaws.services.lambda.runtime.LambdaLogger;
import com.amazonaws.services.lambda.runtime.events.S3Event;
import com.amazonaws.services.lambda.runtime.events.SNSEvent;
import com.amazonaws.services.s3.event.S3EventNotification;
import org.junit.Test;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.List;
import java.util.stream.Collectors;

public class TestElasticLambdaSNSForwarder
{
    @Test
    public void TestLambdaFunction()
    {
        SNSEvent event = new SNSEvent();
        event.setRecords(List.of(SNSRecord()));
        new ElasticLambdaSNSForwarder().handleRequest(event, context());
    }

    private SNSEvent.SNSRecord SNSRecord()
    {
        String json = "";
        try (BufferedReader buffer = new BufferedReader(new InputStreamReader(this.getClass().getResourceAsStream("/sample-event.json")))) {
            json = buffer.lines().collect(Collectors.joining());
        }
        catch (IOException e) {
            e.printStackTrace();
        }
        SNSEvent.SNS snsMessage = new SNSEvent.SNS();
        snsMessage.setMessage(json);
        SNSEvent.SNSRecord record = new SNSEvent.SNSRecord();
        record.setSns(snsMessage);
        return record;
    }

    private Context context()
    {
        return new Context() {
            @Override
            public String getAwsRequestId()
            {
                return null;
            }

            @Override
            public String getLogGroupName()
            {
                return null;
            }

            @Override
            public String getLogStreamName()
            {
                return null;
            }

            @Override
            public String getFunctionName()
            {
                return null;
            }

            @Override
            public String getFunctionVersion()
            {
                return null;
            }

            @Override
            public String getInvokedFunctionArn()
            {
                return null;
            }

            @Override
            public CognitoIdentity getIdentity()
            {
                return null;
            }

            @Override
            public ClientContext getClientContext()
            {
                return null;
            }

            @Override
            public int getRemainingTimeInMillis()
            {
                return 0;
            }

            @Override
            public int getMemoryLimitInMB()
            {
                return 0;
            }

            @Override
            public LambdaLogger getLogger()
            {
                return new LambdaLogger() {
                    @Override
                    public void log(String string)
                    {
                        System.out.println(string);
                    }
                };
            }
        };
    }
}
