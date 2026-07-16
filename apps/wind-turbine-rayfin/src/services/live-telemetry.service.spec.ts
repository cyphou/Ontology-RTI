//-----------------------------------------------------------------------
// <copyright company="Microsoft Corporation">
//        Copyright (c) Microsoft Corporation.  All rights reserved.
//        Licensed under the MIT license. See LICENSE file in the project root for full license information.
// </copyright>
//-----------------------------------------------------------------------

import { describe, it, expect, vi } from "vitest";

// Keep the module offline: the pure helpers under test never touch the client,
// but importing the service pulls in the Fabric client barrel.
vi.mock("@/lib/fabric-client", () => ({
    getFabricClient: () => ({ semanticModel: () => ({ query: vi.fn() }) }),
}));

import {
    isLiveTelemetryConfigured,
    tableToRecords,
    recordsToFarms,
    recordsToTurbines,
    recordsToReadings,
    pivotReadings,
    escapeDaxString,
    daxPowerHistory,
    daxAnomalyScores,
    recordsToHistory,
    recordsToAnomalyScores,
    fetchPowerHistory,
    fetchAnomalyScores,
    DEFAULT_HISTORY_LIMIT,
    DEFAULT_ANOMALY_LIMIT,
} from "@/services/live-telemetry.service";

describe("live-telemetry.service", () => {
    it("is not configured when VITE_LIVE_TELEMETRY_MODEL is unset", () => {
        // The test environment does not define the alias, so the app must fall
        // back to synthetic data rather than attempting a live query.
        expect(isLiveTelemetryConfigured()).toBe(false);
    });

    it("maps SDK query tables to records regardless of column order", () => {
        const table = {
            columns: [{ name: "sensortelemetry[Value]" }, { name: "TurbineId" }],
            rows: [[12.5, "WT-001"]],
        };

        const records = tableToRecords(table as never);

        expect(records).toEqual([{ value: 12.5, turbineid: "WT-001" }]);
    });

    it("projects farm records and drops rows without an id", () => {
        const farms = recordsToFarms([
            { windfarmid: "WF-001", windfarmname: "North Ridge", latitude: 56.4, longitude: 8.8, installedcapacitymw: 5.1 },
            { windfarmid: "", windfarmname: "orphan" },
        ]);

        expect(farms).toEqual([
            { farmId: "WF-001", farmName: "North Ridge", latitude: 56.4, longitude: 8.8, capacityMw: 5.1 },
        ]);
    });

    it("projects turbine → farm records", () => {
        const turbines = recordsToTurbines([
            { turbineid: "WT-001", windfarmid: "WF-001" },
            { turbineid: "", windfarmid: "WF-001" },
        ]);

        expect(turbines).toEqual([{ turbineId: "WT-001", farmId: "WF-001" }]);
    });

    it("pivots latest readings by sensor device type into app signals", () => {
        const readings = recordsToReadings([
            { turbineid: "WT-001", sensortype: "Anemometer", value: 11 },
            { turbineid: "WT-001", sensortype: "Accelerometer", value: 4.2 },
            { turbineid: "WT-001", sensortype: "Temperature", value: 63 },
            { turbineid: "WT-001", sensortype: "CurrentSensor", value: 1000 },
            { turbineid: "WT-001", sensortype: "UnknownDevice", value: 999 },
        ]);

        const metrics = pivotReadings(readings);

        expect(metrics).toHaveLength(1);
        expect(metrics[0]).toEqual({
            turbineId: "WT-001",
            windMs: 11,
            vibrationMmS: 4.2,
            nacelleTempC: 63,
            powerKw: 690, // 1000 * 0.69 conversion, matching the KQL loader
        });
    });

    it("keeps turbines independent when pivoting", () => {
        const metrics = pivotReadings([
            { turbineId: "WT-001", sensorType: "Anemometer", value: 10 },
            { turbineId: "WT-002", sensorType: "Anemometer", value: 20 },
        ]);

        expect(metrics).toHaveLength(2);
        expect(metrics.find((m) => m.turbineId === "WT-001")?.windMs).toBe(10);
        expect(metrics.find((m) => m.turbineId === "WT-002")?.windMs).toBe(20);
    });
});

describe("timeseries history", () => {
    it("escapes double quotes in DAX string literals", () => {
        expect(escapeDaxString('WT-"01"')).toBe('WT-""01""');
        expect(escapeDaxString("WT-001")).toBe("WT-001");
    });

    it("builds a bounded, ordered history query filtered to the turbine and current sensor", () => {
        const dax = daxPowerHistory("WT-001", 25);

        expect(dax).toContain("TOPN(25,");
        expect(dax).toContain('[TurbineId] = "WT-001"');
        expect(dax).toContain('[SensorType] = "CurrentSensor"');
        expect(dax).toContain("ORDER BY [Timestamp] ASC");
    });

    it("defaults the history limit and escapes the id inside the query", () => {
        const dax = daxPowerHistory('WT-"x"');

        expect(dax).toContain(`TOPN(${DEFAULT_HISTORY_LIMIT},`);
        expect(dax).toContain('[TurbineId] = "WT-""x"""');
    });

    it("orders history oldest→newest and converts amps to kW", () => {
        const history = recordsToHistory([
            { timestamp: "2026-01-01T00:02:00Z", value: 1000 },
            { timestamp: "2026-01-01T00:00:00Z", value: 500 },
            { timestamp: "2026-01-01T00:01:00Z", value: 800 },
        ]);

        // 500·0.69=345, 800·0.69=552, 1000·0.69=690 — sorted by timestamp ASC.
        expect(history).toEqual([345, 552, 690]);
    });

    it("drops history rows without a timestamp", () => {
        const history = recordsToHistory([
            { timestamp: "", value: 1000 },
            { timestamp: "2026-01-01T00:00:00Z", value: 1000 },
        ]);

        expect(history).toEqual([690]);
    });

    it("returns null history when live telemetry is not configured", async () => {
        // The alias is unset in the test env, so the fetch must fail safe to null.
        await expect(fetchPowerHistory("WT-001")).resolves.toBeNull();
    });

    it("returns null history for an empty turbine id", async () => {
        await expect(fetchPowerHistory("")).resolves.toBeNull();
    });

    it("builds a bounded anomaly query with temperature and vibration signals", () => {
        const dax = daxAnomalyScores("WT-001", 8);

        expect(dax).toContain("TOPN(8,");
        expect(dax).toContain('sensortelemetry[TurbineId] = "WT-001"');
        expect(dax).toContain('sensortelemetry[SensorType] = "Temperature"');
        expect(dax).toContain('sensortelemetry[SensorType] = "Accelerometer"');
        expect(dax).toContain("ORDER BY [Timestamp] ASC");
    });

    it("defaults anomaly query limit and escapes the turbine id", () => {
        const dax = daxAnomalyScores('WT-"x"');

        expect(dax).toContain(`TOPN(${DEFAULT_ANOMALY_LIMIT},`);
        expect(dax).toContain('sensortelemetry[TurbineId] = "WT-""x"""');
    });

    it("maps anomaly signal records into clamped wind anomaly scores", () => {
        const scores = recordsToAnomalyScores([
            { timestamp: "2026-01-01T00:01:00Z", temperature: 75, accelerometer: 5 },
            { timestamp: "2026-01-01T00:00:00Z", temperature: 60, accelerometer: 4 },
            { timestamp: "2026-01-01T00:02:00Z", temperature: 100, accelerometer: 20 },
            { timestamp: "", temperature: 70, accelerometer: 6 },
        ]);

        expect(scores).toEqual([0, 0.5, 1]);
    });

    it("returns null anomaly history when live telemetry is not configured", async () => {
        await expect(fetchAnomalyScores("WT-001")).resolves.toBeNull();
    });

    it("returns null anomaly history for an empty turbine id", async () => {
        await expect(fetchAnomalyScores("")).resolves.toBeNull();
    });
});
