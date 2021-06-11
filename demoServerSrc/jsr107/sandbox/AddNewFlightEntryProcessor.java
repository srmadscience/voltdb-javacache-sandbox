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

package jsr107.sandbox;

import java.util.Date;

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

import javax.cache.processor.EntryProcessor;
import javax.cache.processor.EntryProcessorException;
import javax.cache.processor.MutableEntry;

import org.voltdb.VoltTable;
import org.voltdb.autojar.IsNeededByAVoltDBProcedure;

import com.google.gson.Gson;

@IsNeededByAVoltDBProcedure
public class AddNewFlightEntryProcessor implements EntryProcessor<String, byte[], VoltTable[]> {

    Gson g = new Gson();

    @Override
    public VoltTable[] process(MutableEntry<String, byte[]> entry, Object... params) throws EntryProcessorException {

        if (params.length != 4) {
            throw new EntryProcessorException("Only 4 params allowable");
        }

        byte[] existingValue = entry.getValue();

        if (existingValue != null) {

            AirmilesRecord ar = g.fromJson(new String(existingValue), AirmilesRecord.class);

            String fromAirport = (String) params[0];
            String toAirport = (String) params[1];
            Date flightDate = (Date) params[2];
            int points = (Integer) params[3];

            ar.addFlight(fromAirport, toAirport, flightDate, points);

            entry.setValue(g.toJson(ar).getBytes());

        }

        VoltTable[] tableArray = {};
        return tableArray;
    }

}
