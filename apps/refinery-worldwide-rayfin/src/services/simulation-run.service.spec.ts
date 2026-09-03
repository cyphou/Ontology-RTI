import { describe, expect, it } from "vitest";
import { buildDecisionPackage, createSimulationRun, validateSimulationRun } from "@/services/simulation-run.service";

const draft = {
    refineryId: "REF001",
    processUnitId: "PU001",
    purpose: "maintenance" as const,
    objective: "margin" as const,
    horizon: 24,
    timeUnit: "hour" as const,
    baselineSource: "live" as const,
    baselineAt: "2026-08-28T08:00:00Z",
};

describe("simulation run", () => {
    it("creates a versioned run with a stable simulation id", () => {
        const run = createSimulationRun(draft);
        expect(run.runId).toMatch(/^sim-/);
        expect(run.horizon).toBe(24);
    });

    it("warns when the run uses simulated data or has no candidate", () => {
        const result = validateSimulationRun({ ...createSimulationRun(draft), baselineSource: "simulated" }, false);
        expect(result.valid).toBe(true);
        expect(result.warnings.join(" ")).toMatch(/simulated/i);
        expect(result.warnings.join(" ")).toMatch(/candidate/i);
    });

    it("builds a ready-for-review package with explicit input classifications", () => {
        const run = createSimulationRun(draft);
        const result = buildDecisionPackage(run, { throughputKbd: 100, forecastKbd: 105, feedRateKbd: 110, unitTempC: 380, utilizationPct: 85 }, { throughputKbd: 90, maintenanceCostUsd: 5000 });
        expect(result.schemaVersion).toBe("1.0");
        expect(result.status).toBe("ready-for-review");
        expect(result.classifications.baselineThroughput).toBe("observed");
        expect(result.classifications["candidate.maintenanceCostUsd"]).toBe("assumed");
    });
});
