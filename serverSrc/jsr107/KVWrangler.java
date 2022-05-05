package jsr107;

import org.voltdb.VoltProcedure.VoltAbortException;

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

/**
 * Utility class for java cache
 *
 */
public class KVWrangler {

    /**
     * Working in the assumption that 'varbinaryPayload' is actually a Sting we
     * convert it to one.
     * 
     * @param varbinaryPayload byte[]
     * @return A new String version of varbinaryPayload
     * @throws VoltAbortException
     */
    public String varbinaryToString(byte[] varbinaryPayload) throws VoltAbortException {
        try {
            if (varbinaryPayload == null) {
                return null;
            }

            return new String(varbinaryPayload);
        } catch (Exception e) {
            throw new VoltAbortException(e);
        }

    }

}
