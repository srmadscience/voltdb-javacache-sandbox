package org.voltdb.jsr107;

/* This file is part of VoltDB.
 * Copyright (C) 2008-2022 VoltDB Inc.
 *
 * Permission is hereby granted, free of charge, to any person obtaining
 * a copy of this software and associated documentation files (the
 * "Software"), to deal in the Software without restriction, including
 * without limitation the rights to use, copy, modify, merge, publish,
 * distribute, sublicense, and/or sell copies of the Software, and to
 * permit persons to whom the Software is furnished to do so, subject to
 * the following conditions:
 *
 * The above copyright notice and this permission notice shall be
 * included in all copies or substantial portions of the Software.
 *
 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND,
 * EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF
 * MERCHANTABILITY, FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT.
 * IN NO EVENT SHALL THE AUTHORS BE LIABLE FOR ANY CLAIM, DAMAGES OR
 * OTHER LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE,
 * ARISING FROM, OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR
 * OTHER DEALINGS IN THE SOFTWARE.
 */

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.CountDownLatch;

import javax.cache.processor.EntryProcessorResult;

import org.voltdb.client.ClientResponse;
import org.voltdb.client.ProcedureCallback;

public class BulkInvocationProcedureCallCallback implements ProcedureCallback {

    CountDownLatch latch;
    @SuppressWarnings("rawtypes")
    Map<String, EntryProcessorResult> results = new HashMap<String, EntryProcessorResult>();

    public BulkInvocationProcedureCallCallback(CountDownLatch latch) {

        super();
        this.latch = latch;

    }

    @Override
    public void clientCallback(ClientResponse arg0) throws Exception {

        results.put(arg0.getAppStatusString(), new VoltDBEntryProcessorResult(arg0));

        latch.countDown();

    }

    /**
     * @return the results
     */
    @SuppressWarnings("rawtypes")
    public Map<String, EntryProcessorResult> getResults() {
        return results;
    }

}
