//-----------------------------------------------------------------------
// <copyright company="Microsoft Corporation">
//        Copyright (c) Microsoft Corporation.  All rights reserved.
//        Licensed under the MIT license. See LICENSE file in the project root for full license information.
// </copyright>
//-----------------------------------------------------------------------

import { describe, expect, it } from "vitest";
import { buildIncidentQueue, type IncidentCandidate } from "@/services/incident-queue.service";

const now = new Date("2026-08-27T12:00:00.000Z");
const base: IncidentCandidate = {
    id: "PORTARTHUR-U-01",
    siteId: "PORTARTHUR",
    siteName: "Port Arthur",
    status: "alarm",
    anomalyScore: 0.9,
    unitTempC: 440,
    utilizationPct: 96,
    throughputKbd: 70,
    acknowledged: false,
    hasOpenOrder: false,
    detectedAt: "2026-08-27T11:30:00.000Z",
};

describe("buildIncidentQueue", () => {
    it("ranks a critical alarm before a warning and calculates its age", () => {
        const queue = buildIncidentQueue([
            { ...base, id: "U-WARN", status: "warning", anomalyScore: 0.6 },
            base,
        ], now);
        expect(queue[0].id).toBe(base.id);
        expect(queue[0].severity).toBe("Critical");
        expect(queue[0].ageMinutes).toBe(30);
    });

    it("selects a heat-exchanger work order after acknowledgement", () => {
        const [item] = buildIncidentQueue([{ ...base, acknowledged: true }], now);
        expect(item.probableAsset).toBe("Heat Exchanger");
        expect(item.nextAction).toBe("Create work order");
    });

    it("routes an incident with an open order back to the twin", () => {
        const [item] = buildIncidentQueue([{ ...base, hasOpenOrder: true, acknowledged: true }], now);
        expect(item.nextAction).toBe("Inspect twin");
    });

    it("ignores healthy candidates", () => {
        expect(buildIncidentQueue([{ ...base, status: "healthy" }], now)).toEqual([]);
    });

    it("defaults a non-hot warning to Pump", () => {
        const [item] = buildIncidentQueue([{ ...base, status: "warning", anomalyScore: 0.2, unitTempC: 320 }], now);
        expect(item.severity).toBe("Medium");
        expect(item.probableAsset).toBe("Pump");
    });
});
