export type DataMode = "live" | "simulated" | "stale" | "unknown";

export interface DataTrust {
    mode: DataMode;
    label: string;
    detail: string;
}

export function dataTrust(isLive: boolean, latestAt?: string, now = new Date()): DataTrust {
    if (!isLive) {
        return { mode: "simulated", label: "Simulated data", detail: "Synthetic telemetry is active until a Fabric semantic-model connection is configured." };
    }
    if (!latestAt) {
        return { mode: "unknown", label: "Data status unknown", detail: "Live connection is configured but no timestamp was returned." };
    }
    const timestamp = new Date(latestAt).getTime();
    const ageSeconds = Number.isFinite(timestamp) ? Math.max(0, Math.floor((now.getTime() - timestamp) / 1000)) : Number.POSITIVE_INFINITY;
    if (ageSeconds > 300) {
        return { mode: "stale", label: "Data may be stale", detail: `Latest telemetry is ${Math.floor(ageSeconds / 60)} minutes old.` };
    }
    return { mode: "live", label: "Live Fabric data", detail: `Latest telemetry is ${ageSeconds}s old.` };
}

export interface BlastRadiusInput {
    unitId: string;
    siteId: string;
    siteName: string;
    asset: string;
    severity: "Critical" | "High" | "Medium";
    hasOpenOrder: boolean;
}

export interface BlastRadius {
    upstream: string[];
    downstream: string[];
    constraints: string[];
    action: string;
}

// Deterministic topology preview matching the deployed refinery graph concepts. Replace
// these labels with ontology/Eventhouse lookups when a protected graph data API is wired.
export function buildBlastRadius(input: BlastRadiusInput): BlastRadius {
    const unit = input.unitId;
    const priority = input.severity === "Critical" ? "isolate and verify" : "inspect and monitor";
    return {
        upstream: [`${unit} feed pipeline`, `${input.siteName} crude receipt`],
        downstream: [`${unit} product header`, `${input.siteName} storage tank farm`],
        constraints: [`${input.asset} condition`, "Pipeline flow and pressure", "Tank inventory / overflow risk"],
        action: input.hasOpenOrder ? `Review the open work order, then ${priority} the affected path.` : `Create a work order, then ${priority} the affected path.`,
    };
}
