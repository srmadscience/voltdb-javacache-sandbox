/* This file is part of VoltDB.
 * Copyright (C) 2008-2021 VoltDB Inc.
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

import java.util.Date;
import java.util.HashMap;
import java.util.Random;

import org.voltdb.jsr107.VoltDBCache;
import org.voltdb.jsr107.VoltDBEntryProcessorResult;
import org.voltdb.types.TimestampType;
import org.voltdb.voltutil.stats.SafeHistogramCache;

import com.google.gson.Gson;

import jsr107.AbstractEventTrackingProcedure;
import jsr107.sandbox.AddNewFlightEntryProcessor;
import jsr107.sandbox.AirmilesRecord;

/**
 * A thread that will do as many sync transactions as it can for a predefined
 * period of time.
 */
public class CacheSandboxThread implements Runnable {

    public static final int CREATE_USERS = 0;
    public static final int DO_SIMPLE_PUTS = 1;
    public static final int DO_OPTIMISTIC_PUTS = 2;
    public static final int DO_INVOCATIONS = 3;

    SafeHistogramCache shc = SafeHistogramCache.getInstance();
    Random r = new Random();
    Gson g = new Gson();

    int writeType = DO_SIMPLE_PUTS;

    VoltDBCache voltDBCache;

    int userCount;
    long starttimeMS;
    long endtimeMS;
    long eventCount = 0;
    int threadId;
    int threadCount;
    int durationSeconds;
    byte[] randomLob;
    int batchSize;

    AddNewFlightEntryProcessor addnewFlight = new AddNewFlightEntryProcessor();

    final String[] airports = { "SFO", "DEN", "CDG", "MUN", "DUB", "BCN", "JFK" };

    /**
     * @param voltDBCache     - instance of a cache
     * @param userCount       - How many users are in the system
     * @param durationSeconds - how long to run each pass for. We make three passes.
     * @param threadId        - ID of this thread
     * @param lobSize         - How large the field AirMilesRecord.randomLob is, if
     *                        we need to make one.
     */
    public CacheSandboxThread(VoltDBCache voltDBCache, int userCount, int durationSeconds, int threadId, int lobSize,
            int threadCount, int batchSize) {
        this.voltDBCache = voltDBCache;
        this.userCount = userCount;
        this.threadId = threadId;
        this.threadCount = threadCount;
        this.durationSeconds = durationSeconds;
        this.batchSize = batchSize;
        randomLob = new byte[lobSize];
        new Random().nextBytes(randomLob);
    }

    @Override
    public void run() {

        eventCount = 0;
        starttimeMS = System.currentTimeMillis();
        endtimeMS = System.currentTimeMillis() + (1000 * durationSeconds);

        if (writeType == CREATE_USERS) {
            addUsers();

        } else {
            while (System.currentTimeMillis() <= endtimeMS) {
                // Note that all our calls our sync...
                doSomething();
            }
        }

    }

    /**
     * Add users for this thread
     */
    private void addUsers() {

        HashMap<String, byte[]> ourMap = new HashMap<String, byte[]>();
        int batchCount = 0;
        long startMs = System.currentTimeMillis();
        long lastMessageMs = System.currentTimeMillis();

        for (int i = 0; i < userCount; i++) {

            if (i % threadCount == threadId) {

                AirmilesRecord ar = new AirmilesRecord("FlyAllDay", r.nextInt(userCount), 0, randomLob);
                byte[] arAsByteArray = g.toJson(ar).getBytes();
                ourMap.put("User_" + i, arAsByteArray);

                if (++batchCount >= batchSize) {

                    startMs = System.currentTimeMillis();
                    voltDBCache.putAll(ourMap);
                    shc.reportLatency("putAllBatch", startMs, "time to call put for " + batchSize + " records", 10000);

                    ourMap.clear();

                    batchCount = 0;

                    if (lastMessageMs + 10000 < System.currentTimeMillis()) {
                        Jsr197Sandbox.msg("[" + threadId + "] on user " + i);
                        lastMessageMs = System.currentTimeMillis();
                    }

                }

            }

        }

        startMs = System.currentTimeMillis();
        voltDBCache.putAll(ourMap);
        shc.reportLatency("putAllBatch", startMs, "time to call put for " + batchSize + " records", 10000);

    }

    /**
     * Do an action of some kind. What depends on 'writeType'...
     */
    private void doSomething() {

        if (++eventCount % 100000 == 1) {
            Jsr197Sandbox.msg("Thread [" + threadId + "] on transaction " + eventCount);
        }

        // pick a random user
        String userId = getUserId();

        switch (writeType) {
        case DO_SIMPLE_PUTS:
            doSimplePuts(userId);
            break;
        case DO_OPTIMISTIC_PUTS:
            doOptimisticPuts(userId);
            break;
        case DO_INVOCATIONS:
            doInvocations(userId);
            break;

        }

    }

    /**
     * Use the cache as simply as possible - gets followed by puts.
     * 
     * @param userId
     */
    private void doSimplePuts(String userId) {

        // See if user already exists...
        byte[] payload = voltDBCache.get(userId);

        if (payload == null) {

            // user does not exist; create them

            AirmilesRecord ar = createNewRecord();
            byte[] arAsByteArray = g.toJson(ar).getBytes();

            long startMs = System.currentTimeMillis();
            voltDBCache.put(userId, arAsByteArray);
            shc.reportLatency("putNew", startMs, "time to call put for new record", 1000);

        } else {

            if (r.nextInt(100) == 0) {
                // Delete user

                long startMs = System.currentTimeMillis();
                voltDBCache.remove(userId);
                shc.reportLatency("remove", startMs, "time to call remove", 1000);

            } else {

                // user exists - let's get their record from the stored value
                AirmilesRecord ar = g.fromJson(new String(payload), AirmilesRecord.class);

                // Add another flight.
                addNewFlight(ar);

                // turn back into bytes
                byte[] newPayload = g.toJson(ar).getBytes();

                long startMs = System.currentTimeMillis();
                voltDBCache.put(userId, newPayload);

                shc.reportLatency("putExists", startMs, "time to call put for existing record", 1000);

            }

        }
    }

    /**
     * Use the cache, but make sure the data hasn't changed since we saw it.
     * 
     * @param userId
     */
    private void doOptimisticPuts(String userId) {

        // See if user already exists...
        byte[] payload = voltDBCache.get(userId);

        if (payload == null) {

            // user does not exist; create them

            AirmilesRecord ar = createNewRecord();
            byte[] arAsByteArray = g.toJson(ar).getBytes();

            long startMs = System.currentTimeMillis();
            boolean ok = voltDBCache.putIfAbsent(userId, arAsByteArray);

            if (ok) {
                shc.reportLatency("putIfAbsent_ok", startMs, "time to call putIfAbsent_ok", 1000);
            } else {
                shc.reportLatency("putIfAbsent_clobbered", startMs, "time to call putIfAbsent_clobbered", 1000);
            }

        } else {

            // user exists - let's get their record from the stored value
            AirmilesRecord ar = g.fromJson(new String(payload), AirmilesRecord.class);

            if (r.nextInt(100) == 0) {
                // Delete user

                long startMs = System.currentTimeMillis();

                // Delete, but only if unchanged...
                boolean ok = voltDBCache.remove(userId, g.toJson(ar).getBytes());

                if (ok) {
                    shc.reportLatency("remove_ok", startMs, "", 1000);
                } else {
                    shc.reportLatency("remove_failed", startMs, "", 1000);
                }

            } else {

                // Add a new flight...
                addNewFlight(ar);
                byte[] newPayload = g.toJson(ar).getBytes();

                long startMs = System.currentTimeMillis();
                boolean ok = voltDBCache.replace(userId, payload, newPayload);

                if (ok) {
                    shc.reportLatency("replace_ok", startMs, "time to call replace_ok", 1000);
                } else {
                    // Someone else changed the record before us.
                    shc.reportLatency("replace_clobbered", startMs, "time to call replace_clobbered", 1000);
                }

            }

        }
    }

    /**
     * Use the cache, but do the work using the 'invocation' interface, which makes
     * sure the data hasn't changed since we saw it.
     * 
     * @param userId
     */
    private void doInvocations(String userId) {

        long startMs = System.currentTimeMillis();
        Object preResult = voltDBCache.invoke(userId, addnewFlight, randomAirport(), randomAirport(), new Date(),
                r.nextInt(20));
        VoltDBEntryProcessorResult result = (VoltDBEntryProcessorResult) preResult;

        if (result.get().getAppStatus() == AbstractEventTrackingProcedure.OK) {
            shc.reportLatency("invoke_ok", startMs, "", 1000);
        } else if (result.get().getAppStatus() == AbstractEventTrackingProcedure.OK_BUT_NOT_FOUND) {
            shc.reportLatency("invoke_nothing_to_update", startMs, "", 1000);
        } else {
            shc.reportLatency("invoke_response_" + result.get().getAppStatus(), startMs, "", 1000);
            Jsr197Sandbox.msg(result.get().getResults()[0].toFormattedString());
        }

    }

    /**
     * Add a new pseudo-random flight to the record.
     * 
     * @param ar
     */
    private void addNewFlight(AirmilesRecord ar) {

        ar.addFlight(randomAirport(), randomAirport(), new TimestampType(), r.nextInt(20));
    }

    /**
     * @return A pseudo random user id
     */
    public String getUserId() {
        return "User_" + r.nextInt(userCount);
    }

    /**
     * @return A pseudo random airport
     */
    private String randomAirport() {
        return airports[r.nextInt(airports.length)];
    }

    /**
     * @return A brand bew record for a new user
     */
    public AirmilesRecord createNewRecord() {
        return new AirmilesRecord("FlyAllDay", r.nextInt(userCount), 0, randomLob);
    }

    /**
     * @return TPS, Transactions per second.
     */
    public long getTps() {

        double eventsPerMS = (1000 * eventCount) / (endtimeMS - starttimeMS);
        return (long) eventsPerMS;
    }

    /**
     * @return the writeType
     */
    public int getWriteType() {
        return writeType;
    }

    /**
     * @param writeType the writeType to set
     */
    public void setWriteType(int writeType) {
        this.writeType = writeType;
    }

}
