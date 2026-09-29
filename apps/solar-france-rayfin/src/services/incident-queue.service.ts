export type IncidentStatus = "healthy" | "warning" | "alarm";

export interface IncidentCandidate {
    id: string;
    siteId: string;
    siteName: string;
    status: IncidentStatus;
    anomalyScore: number;
    moduleTempC: number;
    inverterLoadPct: number;
    powerKw: number;
    acknowledged: boolean;
    hasOpenOrder: boolean;
    detectedAt: string;
}

export interface IncidentQueueItem extends IncidentCandidate {
    severity: "Critical" | "High" | "Medium";
    ageMinutes: number;
    probableAsset: "PV module" | "Inverter";
    nextAction: "Create repair order" | "Inspect twin" | "Acknowledge";
    priorityScore: number;
}

export function buildIncidentQueue(candidates: IncidentCandidate[], now = new Date()): IncidentQueueItem[] {
    return candidates
        .filter((candidate) => candidate.status !== "healthy")
        .map((candidate) => {
            const detected = new Date(candidate.detectedAt).getTime();
            const ageMinutes = Number.isFinite(detected) ? Math.max(0, Math.floor((now.getTime() - detected) / 60_000)) : 0;
            const severity = candidate.status === "alarm" && candidate.anomalyScore >= 0.8
                ? "Critical"
                : candidate.status === "alarm" || candidate.anomalyScore >= 0.55 ? "High" : "Medium";
            const nextAction = candidate.hasOpenOrder ? "Inspect twin" : candidate.acknowledged ? "Create repair order" : "Acknowledge";
            return {
                ...candidate,
                severity,
                ageMinutes,
                probableAsset: candidate.inverterLoadPct >= 90 ? "Inverter" : "PV module",
                nextAction,
                priorityScore: (severity === "Critical" ? 300 : severity === "High" ? 200 : 100) + Math.round(candidate.anomalyScore * 100) + Math.min(ageMinutes, 120) + (candidate.hasOpenOrder ? -35 : 0),
            };
        })
        .sort((left, right) => right.priorityScore - left.priorityScore);
}