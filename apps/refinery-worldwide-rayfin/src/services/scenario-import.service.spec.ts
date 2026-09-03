//-----------------------------------------------------------------------
// <copyright company="Microsoft Corporation">
//        Copyright (c) Microsoft Corporation.  All rights reserved.
//        Licensed under the MIT license. See LICENSE file in the project root for full license information.
// </copyright>
//-----------------------------------------------------------------------

import { describe, it, expect } from "vitest";

import { classifyImport, summarizeActuals } from "@/services/scenario-import.service";

describe("classifyImport", () => {
    it("detects scenario rows from curtailment/downtime/horizon columns (synonyms + %)", () => {
        const result = classifyImport([
            { Scenario: "Turnaround", "Curtailment %": "20", Downtime: 6, Horizon: 24 },
            { Scenario: "Push", Curtail: 0, "Downtime Ticks": 0, Periods: 24 },
        ], "plans.xlsx");
        expect(result.kind).toBe("scenarios");
        if (result.kind === "scenarios") {
            expect(result.scenarios).toHaveLength(2);
            expect(result.scenarios[0].label).toBe("Turnaround");
            expect(result.scenarios[0].curtailmentPct).toBe(20);
            expect(result.scenarios[0].downtimeTicks).toBe(6);
            expect(result.scenarios[0].horizonTicks).toBe(24);
        }
    });

    it("detects an actuals series from actual/forecast columns", () => {
        const result = classifyImport([
            { Period: "Jan", Actual: 1000, Forecast: 1020 },
            { Period: "Feb", Actual: 980, Forecast: 990 },
        ], "actuals.csv");
        expect(result.kind).toBe("actuals");
        if (result.kind === "actuals") {
            expect(result.name).toBe("actuals.csv");
            expect(result.points).toHaveLength(2);
            expect(result.points[0]).toEqual({ label: "Jan", actual: 1000, forecast: 1020 });
        }
    });

    it("clamps out-of-range scenario values", () => {
        const result = classifyImport([{ name: "Bad", curtailment: 150, downtime: -4, horizon: 0 }]);
        expect(result.kind).toBe("scenarios");
        if (result.kind === "scenarios") {
            expect(result.scenarios[0].curtailmentPct).toBe(100);
            expect(result.scenarios[0].downtimeTicks).toBe(0);
            expect(result.scenarios[0].horizonTicks).toBe(1);
        }
    });

    it("reads explicit refinery operating numbers from a scenario row", () => {
        const result = classifyImport([{ Label: "Hot run", Curtailment: 0, Downtime: 1, Horizon: 12, "Throughput kbd": 720, "Feed rate kbd": 800, "Unit temp C": 435, "Utilization %": 98, "Price per barrel USD": 82, "Variable cost per barrel USD": 48, "Maintenance cost USD": 125000, "Energy cost USD": 35000 }]);
        expect(result.kind).toBe("scenarios");
        if (result.kind === "scenarios") {
            expect(result.scenarios[0]).toMatchObject({ throughputKbd: 720, feedRateKbd: 800, unitTempC: 435, utilizationPct: 98, pricePerBarrelUsd: 82, variableCostPerBarrelUsd: 48, maintenanceCostUsd: 125000, energyCostUsd: 35000 });
        }
    });

    it("returns empty with a reason when no recognizable columns exist", () => {
        const result = classifyImport([{ foo: 1, bar: 2 }]);
        expect(result.kind).toBe("empty");
        if (result.kind === "empty") {
            expect(result.reason).toMatch(/could not find/i);
        }
    });

    it("returns empty for no rows", () => {
        expect(classifyImport([]).kind).toBe("empty");
    });
});

describe("summarizeActuals", () => {
    it("computes mean/min/max and a rising trend", () => {
        const s = summarizeActuals([
            { label: "1", actual: 100 },
            { label: "2", actual: 110 },
            { label: "3", actual: 130 },
        ]);
        expect(s.n).toBe(3);
        expect(s.min).toBe(100);
        expect(s.max).toBe(130);
        expect(s.trend).toBe("rising");
    });

    it("guards an empty series", () => {
        const s = summarizeActuals([]);
        expect(s.n).toBe(0);
        expect(s.trend).toBe("flat");
    });
});
