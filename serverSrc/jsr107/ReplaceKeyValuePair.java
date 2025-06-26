package jsr107;

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

import org.voltdb.SQLStmt;
import org.voltdb.VoltTable;

public class ReplaceKeyValuePair extends AbstractEventTrackingProcedure {

    // @formatter:off

	public static final SQLStmt getV = new SQLStmt(
			"SELECT v "
			+ "FROM kv "
			+ "WHERE c = ? "
			+ "AND k = ? "
			+ "AND v = ?;");

	   public static final SQLStmt upsertKV = new SQLStmt(
	            "UPSERT INTO kv\n"
	            + "(c,k,v)\n"
	            + "VALUES \n"
	            + "(?,?,?);");

 	// @formatter:on

    public VoltTable[] run(String k, String c, byte[] oldV, byte[] newV) throws VoltAbortException {

        voltQueueSQL(getV, c, k, oldV);
        queueEventCheck(c);

        final VoltTable[] oldValues = voltExecuteSQL();

        if (oldValues[0].advanceRow()) {

            reportEvent(c, k, newV, UPDATED, oldValues);
            voltQueueSQL(upsertKV, c, k, newV);
            return voltExecuteSQL(true);

        }

        return new VoltTable[0];

    }
}
