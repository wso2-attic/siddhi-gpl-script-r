/*
 * Copyright (c)  2016, WSO2 Inc. (http://www.wso2.org) All Rights Reserved.
 *
 * WSO2 Inc. licenses this file to you under the Apache License,
 * Version 2.0 (the "License"); you may not use this file except
 * in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package io.siddhi.extension.gpl.evalscript.r.r;

import io.siddhi.core.SiddhiAppRuntime;
import io.siddhi.core.exception.SiddhiAppCreationException;
import org.apache.log4j.Logger;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import io.siddhi.core.SiddhiManager;
import io.siddhi.core.event.Event;
import io.siddhi.core.query.output.callback.QueryCallback;
import io.siddhi.core.stream.input.InputHandler;
import io.siddhi.core.util.EventPrinter;

import static org.junit.Assume.assumeTrue;

public class EvalRTestCase {

    private static final Logger log = Logger.getLogger(EvalRTestCase.class);

    private boolean[] isReceived = new boolean[10];
    private Object[] value = new Object[10];

    @Before
    public void assumeEnvironmentVariablesPresent() {
        assumeTrue(System.getenv("JRI_HOME") != null);
        assumeTrue(System.getenv("R_HOME") != null);
    }

    @Test
    public void testEvalRConcat() throws InterruptedException {
        log.info("TestEvalRConcat");

        SiddhiManager siddhiManager = new SiddhiManager();
        siddhiManager.setExtension("script:r", EvalR.class);

        String concatFunc = "define function concatR[R] return string {\n" +
                "return(paste(data, collapse=\"\"));};";

        String cseEventStream =
                "@config(async = 'true')define stream cseEventStream (symbol string, price float, volume long);";
        String query =
                ("@info(name = 'query1') from cseEventStream select price , concatR(symbol,' ',price) as concatStr " +
                        "group by volume insert into mailOutput;");
        SiddhiAppRuntime executionPlanRuntime =
                siddhiManager.createSiddhiAppRuntime(concatFunc + cseEventStream + query);

        executionPlanRuntime.addCallback("query1", new QueryCallback() {
            @Override
            public void receive(long timeStamp, Event[] inEvents, Event[] removeEvents) {
                EventPrinter.print(timeStamp, inEvents, removeEvents);
                isReceived[0] = true;
                value[0] = inEvents[inEvents.length - 1].getData(1);
            }
        });

        isReceived[0] = false;
        value[0] = null;

        InputHandler inputHandler = executionPlanRuntime.getInputHandler("cseEventStream");
        executionPlanRuntime.start();
        inputHandler.send(new Object[]{"IBM", 700f, 100l});
        Thread.sleep(100);

        if (isReceived[0]) {
            Assert.assertEquals("IBM 700", value[0]);
        } else {
            throw new RuntimeException("The event has not been received");
        }

        executionPlanRuntime.shutdown();
    }

    @Test(expected = SiddhiAppCreationException.class)
    public void testRCompilationFailure() throws InterruptedException {
        log.info("testRCompilationFailure");

        SiddhiManager siddhiManager = new SiddhiManager();
        siddhiManager.setExtension("script:r", EvalR.class);

        String concatFunc = "define function concatR[R] return string {\n" +
                "  str1 <- data[1;\n" +
                "  str2 <- data[2];\n" +
                "  str3 <- data[3];\n" +
                "  res <- str1;\n" +
                "  return res;\n" +
                "};";

        SiddhiAppRuntime executionPlanRuntime = siddhiManager.createSiddhiAppRuntime(concatFunc);

        executionPlanRuntime.shutdown();
    }
}
