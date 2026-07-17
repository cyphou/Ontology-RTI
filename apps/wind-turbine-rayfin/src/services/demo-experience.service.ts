//-----------------------------------------------------------------------
// <copyright company="Microsoft Corporation">
//        Copyright (c) Microsoft Corporation.  All rights reserved.
//        Licensed under the MIT license. See LICENSE file in the project root for full license information.
// </copyright>
//-----------------------------------------------------------------------

// Domain-agnostic "demo experience" engine: pure helpers that power the guided
// storytelling flow (mission challenge scoring, exportable run report, SLA urgency,
// and scripted narration). These are intentionally free of React and Wind-specific
// state so Solar and Refinery can reuse them without copy/paste.

export type DemoScriptStepId = "story" | "locate" | "twin" | "graph" | "dispatch" | "support" | "ask";

export type EscalationStage = "none" | "manager" | "regional";

export type EscalationTimelineState = "done" | "current" | "pending";

export interface EscalationTimelineEntry {
    id: "dispatch" | "manager" | "regional";
    label: string;
    state: EscalationTimelineState;
    note: string;
}

// Pure escalation timeline. Turns the current escalation stage (and SLA status) into
// an ordered dispatch → manager → regional sequence with done/current/pending states
// so the UI can render a compact, honest handoff trail.
export function buildEscalationTimeline(stage: EscalationStage, isSlaOverdue: boolean): EscalationTimelineEntry[] {
    const rank: Record<EscalationStage, number> = { none: 0, manager: 1, regional: 2 };
    const reached = rank[stage];
    const entry = (id: EscalationTimelineEntry["id"], order: number, label: string): EscalationTimelineEntry => {
        let state: EscalationTimelineState;
        if (order < reached) {
            state = "done";
        } else if (order === reached) {
            state = "current";
        } else {
            state = "pending";
        }
        const note = state === "done"
            ? "handed off"
            : state === "current"
                ? (order === reached && isSlaOverdue && order < 2 ? "SLA breach — escalate" : "active")
                : "not yet needed";
        return { id, label, state, note };
    };
    return [
        entry("dispatch", 0, "Responder dispatched"),
        entry("manager", 1, "Ops Duty Manager"),
        entry("regional", 2, "Regional Reliability Lead"),
    ];
}

export interface MissionChallengeInput {
    dispatchQualityScore: number;
    hasLeadTechnician: boolean;
    hasEvidence: boolean;
    hasOpenOrder: boolean;
    escalationStage: EscalationStage;
    isSlaOverdue: boolean;
    demoRuns: number;
}

export interface MissionChallengeObjective {
    id: "triage" | "order" | "escalation" | "drill";
    label: string;
    ok: boolean;
    detail: string;
    weight: number;
}

export interface MissionChallengeResult {
    objectives: MissionChallengeObjective[];
    score: number;
    verdict: "ready" | "watch" | "critical";
}

// Weighted readiness score for the guided challenge. Each objective contributes its
// weight when satisfied; the verdict banding keeps the jury-facing summary honest.
export function buildMissionChallenge(input: MissionChallengeInput): MissionChallengeResult {
    const objectives: MissionChallengeObjective[] = [
        {
            id: "triage",
            label: "Triage packet complete",
            ok: input.dispatchQualityScore >= 75 && input.hasLeadTechnician && input.hasEvidence,
            detail: `quality ${input.dispatchQualityScore}%`,
            weight: 35,
        },
        {
            id: "order",
            label: "Repair order in flight",
            ok: input.hasOpenOrder,
            detail: input.hasOpenOrder ? "open order tracked" : "no open order",
            weight: 30,
        },
        {
            id: "escalation",
            label: "Escalation policy respected",
            ok: !input.isSlaOverdue || input.escalationStage === "manager" || input.escalationStage === "regional",
            detail: input.isSlaOverdue ? `SLA overdue · stage ${input.escalationStage}` : "within SLA",
            weight: 20,
        },
        {
            id: "drill",
            label: "Demo drill executed",
            ok: input.demoRuns > 0,
            detail: `${input.demoRuns} run${input.demoRuns === 1 ? "" : "s"}`,
            weight: 15,
        },
    ];

    const achieved = objectives.reduce((sum, objective) => sum + (objective.ok ? objective.weight : 0), 0);
    const score = Math.min(100, Math.max(0, Math.round(achieved)));
    const verdict = score >= 85 ? "ready" : score >= 60 ? "watch" : "critical";
    return { objectives, score, verdict };
}

export interface MissionReportEvent {
    step: string;
    at: string;
    detail?: string;
}

export interface MissionReportInput {
    turbineId: string;
    siteName: string;
    component: string;
    priority: string;
    responder: string | null;
    dispatchQualityScore: number;
    challengeScore: number;
    challengeVerdict: string;
    events: MissionReportEvent[];
    outcome: string;
}

export interface MissionReport {
    generatedAt: string;
    turbineId: string;
    siteName: string;
    component: string;
    priority: string;
    responder: string;
    dispatchQualityScore: number;
    challengeScore: number;
    challengeVerdict: string;
    stepCount: number;
    durationMs: number;
    events: MissionReportEvent[];
    outcome: string;
}

// Pure builder for an exportable demo run report. Computes total duration from the
// first/last event timestamps so a jury can see the scripted flow, timing, and result.
export function buildMissionReport(input: MissionReportInput): MissionReport {
    const times = input.events
        .map((e) => new Date(e.at).getTime())
        .filter((t) => Number.isFinite(t));
    const durationMs = times.length >= 2 ? Math.max(0, Math.max(...times) - Math.min(...times)) : 0;
    return {
        generatedAt: new Date().toISOString(),
        turbineId: input.turbineId,
        siteName: input.siteName,
        component: input.component,
        priority: input.priority,
        responder: input.responder ?? "unassigned",
        dispatchQualityScore: input.dispatchQualityScore,
        challengeScore: input.challengeScore,
        challengeVerdict: input.challengeVerdict,
        stepCount: input.events.length,
        durationMs,
        events: input.events,
        outcome: input.outcome,
    };
}

export interface MissionRunSummary {
    id: string;
    at: string;
    turbineId: string;
    component: string;
    challengeScore: number;
    challengeVerdict: string;
    durationSec: number;
    stepCount: number;
}

// Condense a full report into a list-friendly summary for the run-history panel.
export function summarizeMissionRun(report: MissionReport): MissionRunSummary {
    return {
        id: `${report.turbineId}-${report.generatedAt}`,
        at: report.generatedAt,
        turbineId: report.turbineId,
        component: report.component,
        challengeScore: report.challengeScore,
        challengeVerdict: report.challengeVerdict,
        durationSec: Math.round(report.durationMs / 100) / 10,
        stepCount: report.stepCount,
    };
}

// Prepend a run to history (most-recent-first), de-duplicating by report identity and
// capping the retained count. Pure so it can be unit-tested and reused across domains.
export function pushMissionRun(history: MissionReport[], report: MissionReport, max = 10): MissionReport[] {
    const identity = (r: MissionReport) => `${r.turbineId}-${r.generatedAt}`;
    const deduped = history.filter((r) => identity(r) !== identity(report));
    return [report, ...deduped].slice(0, Math.max(1, max));
}

export type ResponderShift = "day" | "swing" | "night";

export interface ResponderAvailabilityInput {
    shift: ResponderShift;
    onCall: boolean;
    currentLoad: number;
}

export interface ResponderAvailabilitySummary {
    total: number;
    onCall: number;
    free: number;
    byShift: Record<ResponderShift, number>;
    busiestLoad: number;
}

// Pure availability roll-up for a responder roster. "free" counts responders with no
// active tasks; the shift breakdown and busiest load help an operator read capacity
// at a glance. Domain-agnostic so any accelerator app can reuse it.
export function summarizeResponderAvailability(responders: ResponderAvailabilityInput[]): ResponderAvailabilitySummary {
    const byShift: Record<ResponderShift, number> = { day: 0, swing: 0, night: 0 };
    let onCall = 0;
    let free = 0;
    let busiestLoad = 0;
    for (const r of responders) {
        byShift[r.shift] += 1;
        if (r.onCall) {
            onCall += 1;
        }
        if ((r.currentLoad ?? 0) <= 0) {
            free += 1;
        }
        busiestLoad = Math.max(busiestLoad, r.currentLoad ?? 0);
    }
    return { total: responders.length, onCall, free, byShift, busiestLoad };
}

export interface SlaUrgency {
    level: "ok" | "warning" | "critical" | "breached";
    fraction: number;
    color: string;
    label: string;
}

// Pure SLA urgency model. `fraction` is the share of the SLA window elapsed (0..1);
// the level and color escalate as the remaining time shrinks, and switches to
// "breached" once the age exceeds the SLA budget.
export function slaUrgency(ageMin: number, slaMin: number): SlaUrgency {
    const budget = Math.max(1, slaMin);
    const elapsed = Math.max(0, ageMin);
    const fraction = Math.min(1, elapsed / budget);
    if (elapsed > budget) {
        return { level: "breached", fraction: 1, color: "#d85c57", label: "Breached" };
    }
    if (fraction >= 0.8) {
        return { level: "critical", fraction, color: "#e0894a", label: "Critical" };
    }
    if (fraction >= 0.5) {
        return { level: "warning", fraction, color: "#d8c15c", label: "Warning" };
    }
    return { level: "ok", fraction, color: "#5fa27b", label: "On track" };
}

export interface DemoNarration {
    index: number;
    total: number;
    title: string;
    caption: string;
}

export interface DemoManifestStep {
    title: string;
    caption: string;
}

// A domain-agnostic description of the guided demo. Solar and Refinery can supply
// their own manifest (nouns, components, responders, evidence, and step narration)
// so the shared engine drives the walkthrough without any Wind-specific wording.
export interface DomainDemoManifest {
    domainId: string;
    entityNoun: string;
    assetNoun: string;
    components: string[];
    responderRoles: string[];
    evidenceLabels: string[];
    steps: Record<DemoScriptStepId, DemoManifestStep>;
}

export const DEMO_STEP_ORDER: DemoScriptStepId[] = ["story", "locate", "twin", "graph", "dispatch", "support", "ask"];

export const WIND_DEMO_MANIFEST: DomainDemoManifest = {
    domainId: "wind-turbine",
    entityNoun: "turbine",
    assetNoun: "component",
    components: ["Gearbox", "Generator"],
    responderRoles: ["Field Reliability Engineer", "Generator Specialist", "Remote Operations Lead"],
    evidenceLabels: ["Gearbox oil trace", "Vibration spectrum anomaly", "Generator thermal hotspot", "Converter cabinet alarm"],
    steps: {
        story: { title: "Welcome — from detection to resolution", caption: "This guided demo follows one live wind-fleet incident end to end: locate it on the map, inspect the digital twin, trace the ontology graph, dispatch a technician, then ask Fabric IQ. To begin, the ontology has framed the probable component, priority, and lead technician for the affected turbine." },
        locate: { title: "Locate on the fleet map", caption: "The global map centers on the affected turbine and its wind site for situational awareness." },
        twin: { title: "Inspect the digital twin", caption: "Drill into the 3D digital twin to read component- and device-level signals for the turbine." },
        graph: { title: "Analyze the ontology graph", caption: "Trace asset relationships and dependencies in the ontology graph to confirm the probable cause." },
        dispatch: { title: "Dispatch the responder", caption: "A tracked maintenance work order is raised and assigned, with projected energy impact from the what-if plan." },
        support: { title: "Call field support", caption: "On-site field support is contacted and the loop is closed, auto-escalating if the match is below threshold." },
        ask: { title: "Ask Fabric IQ", caption: "A natural-language question is posed to Fabric IQ over the live telemetry and ontology to prioritize next actions." },
    },
};

// Data-driven narration for a scripted step, resolved from a domain manifest.
export function narrateStep(manifest: DomainDemoManifest, step: DemoScriptStepId): DemoNarration {
    const index = DEMO_STEP_ORDER.indexOf(step);
    const entry = manifest.steps[step];
    return {
        index: index >= 0 ? index + 1 : 1,
        total: DEMO_STEP_ORDER.length,
        title: entry.title,
        caption: entry.caption,
    };
}

export const SOLAR_DEMO_MANIFEST: DomainDemoManifest = {
    domainId: "solar-farm",
    entityNoun: "inverter",
    assetNoun: "string",
    components: ["Inverter", "Combiner", "Tracker"],
    responderRoles: ["PV Field Technician", "Inverter Specialist", "Site Operations Lead"],
    evidenceLabels: ["Panel hotspot thermal", "String underperformance", "Inverter fault code", "Combiner box arc trace"],
    steps: {
        story: { title: "Welcome — from detection to resolution", caption: "This guided demo follows one live solar incident end to end: locate the inverter string on the map, inspect the digital twin, trace the ontology graph, dispatch a technician, then ask Fabric IQ. To begin, the ontology has framed the probable component, priority, and lead technician for the affected inverter string." },
        locate: { title: "Locate on the site map", caption: "The map centers on the affected inverter string and its solar plant for situational awareness." },
        twin: { title: "Inspect the digital twin", caption: "Drill into the 3D digital twin to read module- and inverter-level signals for the string." },
        graph: { title: "Analyze the ontology graph", caption: "Trace asset relationships and dependencies in the ontology graph to confirm the probable cause." },
        dispatch: { title: "Dispatch the responder", caption: "A tracked maintenance work order is raised and assigned, with projected yield impact from the what-if plan." },
        support: { title: "Call field support", caption: "On-site field support is contacted and the loop is closed, auto-escalating if the match is below threshold." },
        ask: { title: "Ask Fabric IQ", caption: "A natural-language question is posed to Fabric IQ over the live telemetry and ontology to prioritize next actions." },
    },
};

export const REFINERY_DEMO_MANIFEST: DomainDemoManifest = {
    domainId: "oil-gas-refinery",
    entityNoun: "unit",
    assetNoun: "asset",
    components: ["Pump", "Compressor", "Heat Exchanger"],
    responderRoles: ["Rotating Equipment Engineer", "Process Safety Lead", "Control Room Operator"],
    evidenceLabels: ["Seal leak thermogram", "Vibration trip log", "Exchanger fouling scan", "Flare event snapshot"],
    steps: {
        story: { title: "Welcome — from detection to resolution", caption: "This guided demo follows one live refinery incident end to end: locate the process unit on the map, inspect the digital twin, trace the ontology graph, dispatch an engineer, then ask Fabric IQ. To begin, the ontology has framed the probable asset, priority, and lead engineer for the affected process unit." },
        locate: { title: "Locate on the site map", caption: "The map centers on the affected process unit and its refinery for situational awareness." },
        twin: { title: "Inspect the digital twin", caption: "Drill into the 3D digital twin to read asset- and sensor-level signals for the unit." },
        graph: { title: "Analyze the ontology graph", caption: "Trace asset relationships and dependencies in the ontology graph to confirm the probable cause." },
        dispatch: { title: "Dispatch the responder", caption: "A tracked maintenance work order is raised and assigned, with projected throughput impact from the what-if plan." },
        support: { title: "Call field support", caption: "On-site field support is contacted and the loop is closed, auto-escalating if the match is below threshold." },
        ask: { title: "Ask Fabric IQ", caption: "A natural-language question is posed to Fabric IQ over the live telemetry and ontology to prioritize next actions." },
    },
};

// Registry of the accelerator domains that reuse the shared demo engine. New domains
// register here and select their manifest by id; the guided script/narration then
// runs unchanged on top of domain-specific data.
export const DEMO_MANIFESTS: Record<string, DomainDemoManifest> = {
    [WIND_DEMO_MANIFEST.domainId]: WIND_DEMO_MANIFEST,
    [SOLAR_DEMO_MANIFEST.domainId]: SOLAR_DEMO_MANIFEST,
    [REFINERY_DEMO_MANIFEST.domainId]: REFINERY_DEMO_MANIFEST,
};

// Resolve a manifest by domain id, falling back to the Wind default when unknown.
export function getDemoManifest(domainId: string): DomainDemoManifest {
    return DEMO_MANIFESTS[domainId] ?? WIND_DEMO_MANIFEST;
}

// Backward-compatible Wind narration lookup, now delegating to the shared manifest.
export function demoNarration(step: DemoScriptStepId): DemoNarration {
    return narrateStep(WIND_DEMO_MANIFEST, step);
}
