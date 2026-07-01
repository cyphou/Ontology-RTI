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
    recordsToPlants,
    recordsToArrays,
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
            columns: [{ name: "sensortelemetry[Value]" }, { name: "ArrayId" }],
            rows: [[212.5, "CESTAS-PV-01"]],
        };

        const records = tableToRecords(table as never);

        expect(records).toEqual([{ value: 212.5, arrayid: "CESTAS-PV-01" }]);
    });

    it("projects plant records and drops rows without an id", () => {
        const plants = recordsToPlants([
            { plantid: "CESTAS", plantname: "Cestas", latitude: 44.74, longitude: -0.68, capacitymwc: 300 },
            { plantid: "", plantname: "orphan" },
        ]);

        expect(plants).toEqual([
            { plantId: "CESTAS", plantName: "Cestas", latitude: 44.74, longitude: -0.68, capacityMwc: 300 },
        ]);
    });

    it("projects array → plant records", () => {
        const arrays = recordsToArrays([
            { arrayid: "CESTAS-PV-01", plantid: "CESTAS" },
            { arrayid: "", plantid: "CESTAS" },
        ]);

        expect(arrays).toEqual([{ arrayId: "CESTAS-PV-01", plantId: "CESTAS" }]);
    });

    it("pivots latest readings by sensor type into app signals", () => {
        const readings = recordsToReadings([
            { arrayid: "CESTAS-PV-01", sensortype: "AcPower", value: 210 },
            { arrayid: "CESTAS-PV-01", sensortype: "Irradiance", value: 640 },
            { arrayid: "CESTAS-PV-01", sensortype: "ModuleTemp", value: 48 },
            { arrayid: "CESTAS-PV-01", sensortype: "InverterLoad", value: 72 },
            { arrayid: "CESTAS-PV-01", sensortype: "UnknownDevice", value: 999 },
        ]);

        const metrics = pivotReadings(readings);

        expect(metrics).toHaveLength(1);
        expect(metrics[0]).toEqual({
            arrayId: "CESTAS-PV-01",
            powerKw: 210,
            irradianceWm2: 640,
            moduleTempC: 48,
            inverterLoadPct: 72,
        });
    });

    it("keeps arrays independent when pivoting", () => {
        const metrics = pivotReadings([
            { arrayId: "CESTAS-PV-01", sensorType: "AcPower", value: 100 },
            { arrayId: "CESTAS-PV-02", sensorType: "AcPower", value: 200 },
        ]);

        expect(metrics).toHaveLength(2);
        expect(metrics.find((m) => m.arrayId === "CESTAS-PV-01")?.powerKw).toBe(100);
        expect(metrics.find((m) => m.arrayId === "CESTAS-PV-02")?.powerKw).toBe(200);
    });
});
