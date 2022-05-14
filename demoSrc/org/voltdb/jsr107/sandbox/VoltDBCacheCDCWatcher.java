package org.voltdb.jsr107.sandbox;

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

import java.text.SimpleDateFormat;
import java.util.Arrays;
import java.util.Date;

import javax.cache.configuration.Factory;
import javax.cache.configuration.MutableCacheEntryListenerConfiguration;

import org.voltdb.jsr107.VoltDBCache;
import org.voltdb.jsr107.test.MyCacheEntryEventFilter;
import org.voltdb.jsr107.test.MyCacheEntryFilterFactory;

/**
 * Class to watch kv_deltas
 *
 */
class VoltDBCacheCDCWatcher {

    VoltDBCache c = null;
    int durationSeconds;
    String prefix;

    public VoltDBCacheCDCWatcher(String hostnames, String cacheName, int durationSeconds, String prefix) {
        super();
        this.durationSeconds = durationSeconds;
        this.prefix = prefix;
        c = new VoltDBCache(hostnames, 10, cacheName, "", "", 9092);
    }

    /**
     * Watch kv_deltas topic for durationSeconds
     */
    void watch() {

        Factory<VerboseCacheEntryListener<String, byte[]>> theListenerFactory = new VerboseCacheEntryListenerFactory();
        Factory<MyCacheEntryEventFilter<String, byte[]>> theEventFactory = new MyCacheEntryFilterFactory(prefix);

        MutableCacheEntryListenerConfiguration<String, byte[]> cacheEntryListenerConfig = new MutableCacheEntryListenerConfiguration<>(
                theListenerFactory, theEventFactory, false, true);

        c.registerCacheEntryListener(cacheEntryListenerConfig);

        VerboseCacheEntryListener<String, byte[]> cacheEntryListener = ((VerboseCacheEntryListenerFactory) theListenerFactory)
                .getListener();

        try {
            Thread.sleep(3000);
        } catch (InterruptedException e) {

        }

        cacheEntryListener.resetCounters();

        long timeoutMS = System.currentTimeMillis() + (durationSeconds * 1000);

        while (System.currentTimeMillis() < timeoutMS) {

            try {
                msg(cacheEntryListener.toString());
                Thread.sleep(1000);

            } catch (InterruptedException e) {

            }

        }

        msg(cacheEntryListener.toString());

    }

    /**
     * Watch topic kv_deltas. Params hostnames - comma delimited list of hostnames
     * cachename - name of cache to watch durationseconds - how long to run for
     */
    public static void main(String[] args) {

        msg("Parameters:" + Arrays.toString(args));

        if (args.length != 4) {
            msg("Usage: hostnames cachename durationseconds prefix");
            System.exit(1);
        }

        String hostnames = new String(args[0]);
        String cachename = new String(args[1]);
        int durationSeconds = Integer.parseInt(args[2]);
        String prefix = new String(args[3]);

        VoltDBCacheCDCWatcher vw = new VoltDBCacheCDCWatcher(hostnames, cachename, durationSeconds, prefix);

        vw.watch();

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
