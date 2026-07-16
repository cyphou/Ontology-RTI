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
    recordsToRefineries,
    recordsToUnits,
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
            columns: [{ name: "sensortelemetry[Value]" }, { name: "ProcessUnitId" }],
            rows: [[212.5, "PU001"]],
        };

        const records = tableToRecords(table as never);

        expect(records).toEqual([{ value: 212.5, processunitid: "PU001" }]);
    });

    it("projects refinery records, converts BPD to kbd, and drops rows without an id", () => {
        const refineries = recordsToRefineries([
            { refineryid: "REF001", refineryname: "Gulf Coast", latitude: 29.76, longitude: -95.36, capacitybpd: 620000 },
            { refineryid: "", refineryname: "orphan" },
        ]);

        expect(refineries).toEqual([
            { refineryId: "REF001", refineryName: "Gulf Coast", latitude: 29.76, longitude: -95.36, capacityKbd: 620 },
        ]);
    });

    it("projects unit → refinery records", () => {
        const units = recordsToUnits([
            { processunitid: "PU001", refineryid: "REF001" },
            { processunitid: "", refineryid: "REF001" },
        ]);

        expect(units).toEqual([{ unitId: "PU001", refineryId: "REF001" }]);
    });

    it("pivots latest readings by sensor type into app signals", () => {
        const readings = recordsToReadings([
            { processunitid: "PU001", sensortype: "Throughput", value: 210 },
            { processunitid: "PU001", sensortype: "FeedRate", value: 240 },
            { processunitid: "PU001", sensortype: "UnitTemp", value: 415 },
            { processunitid: "PU001", sensortype: "Utilization", value: 88 },
            { processunitid: "PU001", sensortype: "UnknownDevice", value: 999 },
        ]);

        const metrics = pivotReadings(readings);

        expect(metrics).toHaveLength(1);
        expect(metrics[0]).toEqual({
            unitId: "PU001",
            powerKw: 210,
            irradianceWm2: 240,
            moduleTempC: 415,
            inverterLoadPct: 88,
        });
    });

    it("keeps process units independent when pivoting", () => {
        const metrics = pivotReadings([
            { unitId: "PU001", sensorType: "Throughput", value: 100 },
            { unitId: "PU002", sensorType: "Throughput", value: 200 },
        ]);

        expect(metrics).toHaveLength(2);
        expect(metrics.find((m) => m.unitId === "PU001")?.powerKw).toBe(100);
        expect(metrics.find((m) => m.unitId === "PU002")?.powerKw).toBe(200);
    });
});

describe("timeseries history", () => {
    it("escapes double quotes for DAX string literals", () => {
        expect(escapeDaxString('PU"001')).toBe('PU""001');
    });

    it("builds a TOPN Throughput-filtered query ordered oldest-to-newest", () => {
        const dax = daxPowerHistory("PU001", 5);
        expect(dax).toContain("EVALUATE TOPN(5,");
        expect(dax).toContain('[ProcessUnitId] = "PU001"');
        expect(dax).toContain('[SensorType] = "Throughput"');
        expect(dax).toContain("ORDER BY [Timestamp] ASC");
    });

    it("defaults the limit and escapes the unit id inside the query", () => {
        const dax = daxPowerHistory('A"B');
        expect(dax).toContain(`EVALUATE TOPN(${DEFAULT_HISTORY_LIMIT},`);
        expect(dax).toContain('[ProcessUnitId] = "A""B"');
    });

    it("orders history oldest-to-newest and rounds throughput values", () => {
        const history = recordsToHistory([
            { timestamp: "2024-01-01T00:02:00Z", value: 800.04 },
            { timestamp: "2024-01-01T00:00:00Z", value: 500.06 },
            { timestamp: "2024-01-01T00:01:00Z", value: 690.02 },
        ]);
        expect(history).toEqual([500.1, 690, 800]);
    });

    it("drops rows without a timestamp", () => {
        const history = recordsToHistory([
            { timestamp: "", value: 500 },
            { timestamp: "2024-01-01T00:00:00Z", value: 690 },
        ]);
        expect(history).toEqual([690]);
    });

    it("returns null when not configured", async () => {
        expect(await fetchPowerHistory("PU001")).toBeNull();
    });

    it("returns null for an empty unit id", async () => {
        expect(await fetchPowerHistory("")).toBeNull();
    });

    it("builds a bounded anomaly query with unit temp and utilization signals", () => {
        const dax = daxAnomalyScores("PU001", 8);
        expect(dax).toContain("TOPN(8,");
        expect(dax).toContain('sensortelemetry[ProcessUnitId] = "PU001"');
        expect(dax).toContain('sensortelemetry[SensorType] = "UnitTemp"');
        expect(dax).toContain('sensortelemetry[SensorType] = "Utilization"');
        expect(dax).toContain("ORDER BY [Timestamp] ASC");
    });

    it("defaults anomaly query limit and escapes the unit id", () => {
        const dax = daxAnomalyScores('A"B');
        expect(dax).toContain(`TOPN(${DEFAULT_ANOMALY_LIMIT},`);
        expect(dax).toContain('sensortelemetry[ProcessUnitId] = "A""B"');
    });

    it("maps anomaly signal records into clamped refinery anomaly scores", () => {
        const scores = recordsToAnomalyScores([
            { timestamp: "2024-01-01T00:01:00Z", unittemp: 395, utilization: 89 },
            { timestamp: "2024-01-01T00:00:00Z", unittemp: 360, utilization: 80 },
            { timestamp: "2024-01-01T00:02:00Z", unittemp: 500, utilization: 120 },
            { timestamp: "", unittemp: 395, utilization: 89 },
        ]);

        expect(scores).toEqual([0, 0.5, 1]);
    });

    it("returns null anomaly history when live telemetry is not configured", async () => {
        expect(await fetchAnomalyScores("PU001")).toBeNull();
    });

    it("returns null anomaly history for an empty unit id", async () => {
        expect(await fetchAnomalyScores("")).toBeNull();
    });
});
