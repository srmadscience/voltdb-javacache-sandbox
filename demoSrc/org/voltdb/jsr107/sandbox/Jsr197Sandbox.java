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

package org.voltdb.jsr107.sandbox;

import java.io.File;
import java.text.SimpleDateFormat;
import java.util.Arrays;
import java.util.Date;
import java.util.Random;

import org.voltdb.jsr107.VoltDBCache;
import org.voltdb.voltutil.stats.SafeHistogramCache;

public class Jsr197Sandbox {

    public Jsr197Sandbox() {
        msg("Starting Jsr197Sandbox");
    }

    public static void main(String[] args) {

        SafeHistogramCache shc = SafeHistogramCache.getInstance();

        msg("Parameters:" + Arrays.toString(args));

        if (args.length != 8) {
            msg("Usage: hostnames usercount threads durationseconds batchsize lobsize eraseusers1_or_0 enable_events_1_or_0");
            System.exit(1);
        }

        // Comma delimited list of hosts...
        String hostlist = args[0];

        // How many users
        int userCount = Integer.parseInt(args[1]);

        // Target transactions per millisecond.
        int threads = Integer.parseInt(args[2]);

        // Runtime for TRANSACTIONS in seconds.
        int durationSeconds = Integer.parseInt(args[3]);

        // how many users we try and create at once
        int batchSize = Integer.parseInt(args[4]);

        // How big the arbitrary binary payload is.
        int lobSize = Integer.parseInt(args[5]);

        boolean eraseUsers = false;

        if (Integer.parseInt(args[6]) > 0) {
            eraseUsers = true;
        }

        // enable events
        boolean enableEvents = false;

        if (Integer.parseInt(args[7]) > 0) {
            enableEvents = true;
        }

        try {
            // Create an arbitrary binary payload
            byte[] randomLob = new byte[lobSize];
            new Random().nextBytes(randomLob);

            VoltDBCache[] cacheArray = new VoltDBCache[threads];
            CacheSandboxThread[] sbArray = new CacheSandboxThread[threads];
            Thread[] threadArray = new Thread[threads];

            for (int i = 0; i < threads; i++) {
                cacheArray[i] = new VoltDBCache(hostlist, 2, "TestCache",  "jsr107.sandbox",
                        9092);
                sbArray[i] = new CacheSandboxThread(cacheArray[i], userCount, durationSeconds, i, lobSize, threads,
                        batchSize);
            }

            cacheArray[0].setEvents(enableEvents);

            long startMs = System.currentTimeMillis();

            if (eraseUsers) {

                cacheArray[0].loadEntryProcessors();

                msg("Step 1: Remove any old records...");
                cacheArray[0].removeAll();
                shc.reportLatency("RemoveAll", startMs, "", 1000);

                msg("...done");
            } else {
                msg("skipping erase users/load entry processors");
            }

            Thread.sleep(1000);

            msg("Step 2: Creating " + userCount + " new records, each with a binary payload of " + lobSize + " bytes");

            long startPutAllMs = System.currentTimeMillis();
            startMs = System.currentTimeMillis();

            for (int i = 0; i < threads; i++) {
                sbArray[i].setWriteType(CacheSandboxThread.CREATE_USERS);
                threadArray[i] = new Thread(sbArray[i]);
                threadArray[i].start();
            }

            Thread.sleep(1000);
            msg("Waiting for " + threads + " threads to finish");
            for (int i = 0; i < threads; i++) {
                threadArray[i].join();
            }
            msg("...done");

            shc.reportLatency("putAllTotal", startPutAllMs,
                    "time to call put for " + userCount + " records in batches of " + batchSize, 180000);

            msg("Step 3: Starting " + threads + " threads to do simple Get/Put operations. ");
            msg("We'll see how many we can get done in " + durationSeconds + " seconds...");

            for (int i = 0; i < threads; i++) {
                sbArray[i].setWriteType(CacheSandboxThread.DO_SIMPLE_PUTS);
                threadArray[i] = new Thread(sbArray[i]);
                threadArray[i].start();
            }

            Thread.sleep(1000);
            msg("Waiting for " + threads + " threads to finish");
            for (int i = 0; i < threads; i++) {
                threadArray[i].join();
                msg("thread [" + i + "] got " + sbArray[i].getTps() + " Logical TPS");
            }
            msg("...done");

            msg("Step 4: Starting " + threads + " threads to do Get/Optimistic Replace operations. ");
            msg("We'll see how many we can get done in " + durationSeconds + " seconds...");

            msg("Starting " + threads + " threads ");
            for (int i = 0; i < threads; i++) {
                sbArray[i].setWriteType(CacheSandboxThread.DO_OPTIMISTIC_PUTS);
                threadArray[i] = new Thread(sbArray[i]);
                threadArray[i].start();
            }

            Thread.sleep(1000);
            msg("Waiting for " + threads + " threads to finish");
            for (int i = 0; i < threads; i++) {
                threadArray[i].join();
                msg("thread [" + i + "] got " + sbArray[i].getTps() + " Logical TPS");
            }
            msg("...done");

            msg("Step 5: Starting " + threads + " threads to do invoke operations on the server side. ");
            msg("We'll see how many we can get done in " + durationSeconds + " seconds...");

            msg("Starting " + threads + " threads ");
            for (int i = 0; i < threads; i++) {
                sbArray[i].setWriteType(CacheSandboxThread.DO_INVOCATIONS);
                threadArray[i] = new Thread(sbArray[i]);
                threadArray[i].start();
            }

            Thread.sleep(1000);
            msg("Waiting for " + threads + " threads to finish");
            for (int i = 0; i < threads; i++) {
                threadArray[i].join();
                msg("thread [" + i + "] got " + sbArray[i].getTps() + " TPS");
            }
            msg("...done");

            msg(shc.toString());

            msg("Time to create " + userCount + " records in batches of " + batchSize + ": " +

                    +shc.get("putAllTotal").getEventTotal());
            msg("Simple Get/Put operations in " + durationSeconds + " seconds: "
                    + shc.get("putExists").getEventTotal());
            msg("Get/Optimistic Replace in " + durationSeconds + " seconds: " + shc.get("replace_ok").getEventTotal());
            msg("Invocation operations in " + durationSeconds + " seconds: " + shc.get("invoke_ok").getEventTotal());

        } catch (Exception e) {
            msg(e.getMessage());
        }

    }

    /**
     * Print a formatted message.
     *
     * @param message
     */
    public static void msg(String message) {

        SimpleDateFormat sdfDate = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
        Date now = new Date();
        String strDate = sdfDate.format(now);
        System.out.println(strDate + ":" + message);

    }

}
