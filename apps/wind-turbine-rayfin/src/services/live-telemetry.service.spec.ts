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
