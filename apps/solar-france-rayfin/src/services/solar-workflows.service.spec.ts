import { describe, expect, it } from "vitest";
import { buildIncidentQueue } from "@/services/incident-queue.service";
import { compareScenarios, summarizeComparison } from "@/services/scenario-lab.service";
import { approveSimulationRun, createSimulationRun, loadSimulationRuns, persistSimulationRun, validateSimulationRun } from "@/services/simulation-run.service";

describe("Solar operational workflows", () => {
    it("prioritizes a hot, overloaded PV array and routes it to repair after acknowledgement", () => {
        const [incident] = buildIncidentQueue([{ id: "CESTAS-PV-01", siteId: "CESTAS", siteName: "Cestas", status: "alarm", anomalyScore: 0.9, moduleTempC: 82, inverterLoadPct: 99, powerKw: 1200, acknowledged: true, hasOpenOrder: false, detectedAt: "2026-09-07T09:30:00Z" }], new Date("2026-09-07T10:00:00Z"));
        expect(incident).toMatchObject({ severity: "Critical", probableAsset: "Inverter", nextAction: "Create repair order", ageMinutes: 30 });
    });

    it("ranks solar scenarios by energy impact and recommends the best plan", () => {
        const comparison = compareScenarios(1000, [
            { id: "baseline", label: "Maintain output", curtailmentPct: 0, downtimeTicks: 0, horizonTicks: 12 },
            { id: "service", label: "Inverter service", curtailmentPct: 0, downtimeTicks: 3, horizonTicks: 12, tariffEurPerKwh: 0.12, maintenanceCostEur: 50 },
        ]);
        expect(comparison.bestId).toBe("baseline");
        expect(comparison.scenarios.find((scenario) => scenario.id === "service")?.energyDeltaKwt).toBe(-3000);
        expect(summarizeComparison(comparison)).toContain("Maintain output");
    });

    it("persists and approves a governed simulation run", () => {
        const values = new Map<string, string>();
        const storage = { getItem: (key: string) => values.get(key) ?? null, setItem: (key: string, value: string) => { values.set(key, value); }, removeItem: () => {}, clear: () => {}, key: () => null, length: 0 } as Storage;
        const run = createSimulationRun({ arrayId: "CESTAS-PV-01", plantId: "CESTAS", purpose: "maintenance", objective: "availability", horizon: 24, timeUnit: "hour", baselineSource: "simulated" });
        expect(validateSimulationRun(run).join(" ")).toMatch(/simulated/i);
        persistSimulationRun(run, storage);
        const approved = approveSimulationRun(run.runId, "approved", "Schedule at noon", storage);
        expect(loadSimulationRuns(storage)).toHaveLength(1);
        expect(approved[0].approval).toMatchObject({ decision: "approved", reason: "Schedule at noon" });
    });
});