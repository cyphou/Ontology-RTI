//-----------------------------------------------------------------------
// <copyright company="Microsoft Corporation">
//        Copyright (c) Microsoft Corporation.  All rights reserved.
//        Licensed under the MIT license. See LICENSE file in the project root for full license information.
// </copyright>
//-----------------------------------------------------------------------

export type IncidentStatus = "healthy" | "warning" | "alarm";

export interface IncidentCandidate {
    id: string;
    siteId: string;
    siteName: string;
    status: IncidentStatus;
    anomalyScore: number;
    unitTempC: number;
    utilizationPct: number;
    throughputKbd: number;
    acknowledged: boolean;
    hasOpenOrder: boolean;
    detectedAt: string;
}

export interface IncidentQueueItem extends IncidentCandidate {
    severity: "Critical" | "High" | "Medium";
    ageMinutes: number;
    probableAsset: "Heat Exchanger" | "Pump";
    nextAction: "Create work order" | "Inspect twin" | "Acknowledge";
    priorityScore: number;
}

function ageMinutes(timestamp: string, now: Date): number {
    const detected = new Date(timestamp).getTime();
    return Number.isFinite(detected) ? Math.max(0, Math.floor((now.getTime() - detected) / 60_000)) : 0;
}

function severityFor(candidate: IncidentCandidate): IncidentQueueItem["severity"] {
    if (candidate.status === "alarm" && candidate.anomalyScore >= 0.8) {
        return "Critical";
    }
    if (candidate.status === "alarm" || candidate.anomalyScore >= 0.55) {
        return "High";
    }
    return "Medium";
}

function probableAssetFor(candidate: IncidentCandidate): IncidentQueueItem["probableAsset"] {
    return candidate.unitTempC >= 380 ? "Heat Exchanger" : "Pump";
}

// Build a prioritized, action-oriented queue. Candidates are derived from the app's
// active telemetry today; the shape deliberately matches an EquipmentAlert enrichment
// so a protected Eventhouse query can replace that input without changing the UI.
export function buildIncidentQueue(candidates: IncidentCandidate[], now = new Date()): IncidentQueueItem[] {
    return candidates
        .filter((candidate) => candidate.status !== "healthy")
        .map((candidate) => {
            const age = ageMinutes(candidate.detectedAt, now);
            const severity = severityFor(candidate);
            const severityWeight = severity === "Critical" ? 300 : severity === "High" ? 200 : 100;
            const nextAction: IncidentQueueItem["nextAction"] = candidate.hasOpenOrder
                ? "Inspect twin"
                : candidate.acknowledged
                    ? "Create work order"
                    : "Acknowledge";
            return {
                ...candidate,
                severity,
                ageMinutes: age,
                probableAsset: probableAssetFor(candidate),
                nextAction,
                priorityScore: severityWeight + Math.round(candidate.anomalyScore * 100) + Math.min(age, 120) + (candidate.hasOpenOrder ? -35 : 0),
            };
        })
        .sort((left, right) => right.priorityScore - left.priorityScore);
}
