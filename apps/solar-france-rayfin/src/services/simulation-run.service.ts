export type SimulationPurpose = "yield" | "maintenance" | "curtailment" | "incident";
export type SimulationObjective = "generation" | "revenue" | "risk" | "availability";

export interface SimulationRun {
    runId: string;
    arrayId: string;
    plantId: string;
    purpose: SimulationPurpose;
    objective: SimulationObjective;
    horizon: number;
    timeUnit: "hour" | "day";
    baselineSource: "live" | "simulated" | "unknown" | "stale";
    createdAt: string;
    approval?: { decision: "approved" | "rejected"; reason: string; decidedAt: string };
}

const STORAGE_KEY = "solar-simulation-runs";

export function createSimulationRun(input: Omit<SimulationRun, "runId" | "createdAt">): SimulationRun {
    return { ...input, runId: `solar-sim-${Date.now().toString(36)}`, horizon: Math.max(1, Math.round(input.horizon)), createdAt: new Date().toISOString() };
}

export function validateSimulationRun(run: SimulationRun): string[] {
    const warnings: string[] = [];
    if (run.baselineSource !== "live") warnings.push(run.baselineSource === "simulated" ? "Baseline is simulated; results are exploratory." : "Baseline freshness must be confirmed before approval.");
    if (run.horizon > (run.timeUnit === "hour" ? 168 : 90)) warnings.push("Long horizons increase irradiance and availability uncertainty.");
    return warnings;
}

export function loadSimulationRuns(storage: Storage = localStorage): SimulationRun[] {
    try { return JSON.parse(storage.getItem(STORAGE_KEY) ?? "[]") as SimulationRun[]; } catch { return []; }
}

export function persistSimulationRun(run: SimulationRun, storage: Storage = localStorage): SimulationRun[] {
    const runs = [run, ...loadSimulationRuns(storage).filter((entry) => entry.runId !== run.runId)].slice(0, 20);
    storage.setItem(STORAGE_KEY, JSON.stringify(runs));
    return runs;
}

export function approveSimulationRun(runId: string, decision: "approved" | "rejected", reason: string, storage: Storage = localStorage): SimulationRun[] {
    const runs = loadSimulationRuns(storage).map((run) => run.runId === runId ? { ...run, approval: { decision, reason: reason.trim(), decidedAt: new Date().toISOString() } } : run);
    storage.setItem(STORAGE_KEY, JSON.stringify(runs));
    return runs;
}