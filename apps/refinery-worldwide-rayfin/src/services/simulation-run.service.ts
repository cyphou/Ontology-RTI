export type SimulationPurpose = "production" | "maintenance" | "feedstock" | "incident";
export type SimulationObjective = "margin" | "throughput" | "risk" | "energy";
export type InputClassification = "observed" | "assumed";

export interface SimulationRunDraft {
    runId: string;
    refineryId: string;
    processUnitId: string;
    purpose: SimulationPurpose;
    objective: SimulationObjective;
    horizon: number;
    timeUnit: "hour" | "day";
    baselineSource: "live" | "simulated" | "unknown" | "stale";
    baselineAt?: string;
}

export interface SimulationValidation {
    valid: boolean;
    warnings: string[];
}

export interface SimulationDecisionPackage {
    schemaVersion: "1.0";
    generatedAt: string;
    run: SimulationRunDraft;
    baseline: {
        throughputKbd: number;
        forecastKbd: number;
        feedRateKbd: number;
        unitTempC: number;
        utilizationPct: number;
    };
    candidate?: Record<string, unknown>;
    classifications: Record<string, InputClassification>;
    status: "draft" | "ready-for-review";
}

export function createSimulationRun(input: Omit<SimulationRunDraft, "runId">): SimulationRunDraft {
    return {
        ...input,
        runId: `sim-${Date.now().toString(36)}`,
        horizon: Math.max(1, Math.round(input.horizon)),
    };
}

// Reject only invalid setup; preserve advisory warnings so analysts can decide whether
// assumptions are appropriate for an exploratory run.
export function validateSimulationRun(run: SimulationRunDraft, hasCandidate: boolean): SimulationValidation {
    const warnings: string[] = [];
    if (run.baselineSource !== "live") {
        warnings.push(run.baselineSource === "simulated" ? "Baseline is simulated; results are exploratory." : "Baseline freshness is not verified; confirm source data before approval.");
    }
    if (!hasCandidate) {
        warnings.push("No imported candidate is attached. The run compares current performance and forecast only.");
    }
    if (run.horizon > (run.timeUnit === "hour" ? 168 : 90)) {
        warnings.push("Long horizons increase forecast and assumption uncertainty.");
    }
    return { valid: run.refineryId !== "" && run.processUnitId !== "" && run.horizon > 0, warnings };
}

export function buildDecisionPackage(
    run: SimulationRunDraft,
    baseline: SimulationDecisionPackage["baseline"],
    candidate?: Record<string, unknown>,
): SimulationDecisionPackage {
    const classifications: Record<string, InputClassification> = {
        baselineThroughput: "observed",
        baselineForecast: "observed",
        baselineFeedRate: "observed",
        baselineUnitTemp: "observed",
        baselineUtilization: "observed",
    };
    if (candidate) {
        Object.keys(candidate).forEach((key) => { classifications[`candidate.${key}`] = "assumed"; });
    }
    const validation = validateSimulationRun(run, Boolean(candidate));
    return {
        schemaVersion: "1.0",
        generatedAt: new Date().toISOString(),
        run,
        baseline,
        candidate,
        classifications,
        status: validation.valid && candidate ? "ready-for-review" : "draft",
    };
}
