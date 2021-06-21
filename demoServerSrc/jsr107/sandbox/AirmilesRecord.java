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

import java.util.ArrayList;
import java.util.Date;

import org.voltdb.autojar.IsNeededByAVoltDBProcedure;
import org.voltdb.types.TimestampType;

/**
 * Hypothetical payload for our KV store.
 */
@IsNeededByAVoltDBProcedure
public class AirmilesRecord {

    public byte[] randomLob;

    public String loyaltySchemeName;

    public long loyaltySchemeNumber;

    public long loyaltySchemePoints;

    public long loyaltySchemeTier = 0;

    public ArrayList<AirmilesFlight> flightsTaken = new ArrayList<AirmilesFlight>();

    public AirmilesRecord(String loyaltySchemeName, long loyaltySchemeNumber, long loyaltySchemePoints,
            byte[] randomLob) {
        super();

        this.loyaltySchemeName = loyaltySchemeName;
        this.loyaltySchemeNumber = loyaltySchemeNumber;
        this.loyaltySchemePoints = loyaltySchemePoints;
        this.randomLob = randomLob;
    }

    /**
     * 
     * Add a flight to our record.
     * 
     * @param fromAirport
     * @param toAirport
     * @param flightDate
     * @param points
     */
    public void addFlight(String fromAirport, String toAirport, TimestampType flightDate, long points) {

        AirmilesFlight af = new AirmilesFlight(fromAirport, toAirport, flightDate, points);

        if (flightsTaken.size() > 10) {

            flightsTaken.clear();
            loyaltySchemeTier += 1;
        }

        loyaltySchemePoints += af.points;
        flightsTaken.add(af);
    }

    @Override
    public String toString() {
        StringBuilder builder = new StringBuilder();
        builder.append("AirmilesRecord [loyaltySchemeName=");
        builder.append(loyaltySchemeName);
        builder.append(", loyaltySchemeNumber=");
        builder.append(loyaltySchemeNumber);
        builder.append(", loyaltySchemePoints=");
        builder.append(loyaltySchemePoints);
        builder.append(", loyaltySchemeTier=");
        builder.append(loyaltySchemeTier);
        builder.append(", flightsTaken=[");

        for (int i = 0; i < flightsTaken.size(); i++) {
            builder.append(flightsTaken.get(i));
            builder.append(",");
        }

        builder.append("]");
        return builder.toString();
    }

}
