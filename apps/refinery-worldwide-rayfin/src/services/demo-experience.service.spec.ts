//-----------------------------------------------------------------------
// <copyright company="Microsoft Corporation">
//        Copyright (c) Microsoft Corporation.  All rights reserved.
//        Licensed under the MIT license. See LICENSE file in the project root for full license information.
// </copyright>
//-----------------------------------------------------------------------

import { describe, it, expect } from "vitest";
import { buildMissionChallenge, buildMissionReport, buildEscalationTimeline, pushMissionRun, summarizeMissionRun, summarizeResponderAvailability, slaUrgency, demoNarration, narrateStep, getDemoManifest, WIND_DEMO_MANIFEST, SOLAR_DEMO_MANIFEST, REFINERY_DEMO_MANIFEST, DEMO_MANIFESTS, DEMO_STEP_ORDER, type DomainDemoManifest, type MissionReport } from "@/services/demo-experience.service";

describe("buildMissionChallenge", () => {
    it("returns ready when all mission objectives are met", () => {
        const challenge = buildMissionChallenge({
            dispatchQualityScore: 92,
            hasLeadTechnician: true,
            hasEvidence: true,
            hasOpenOrder: true,
            escalationStage: "none",
            isSlaOverdue: false,
            demoRuns: 2,
        });
        expect(challenge.score).toBe(100);
        expect(challenge.verdict).toBe("ready");
        expect(challenge.objectives.every((objective) => objective.ok)).toBe(true);
    });

    it("requires escalation when SLA is overdue", () => {
        const challenge = buildMissionChallenge({
            dispatchQualityScore: 92,
            hasLeadTechnician: true,
            hasEvidence: true,
            hasOpenOrder: true,
            escalationStage: "none",
            isSlaOverdue: true,
            demoRuns: 1,
        });
        const escalation = challenge.objectives.find((objective) => objective.id === "escalation");
        expect(escalation?.ok).toBe(false);
        expect(challenge.score).toBe(80);
        expect(challenge.verdict).toBe("watch");
    });

    it("drops to critical when triage/order/drill are missing", () => {
        const challenge = buildMissionChallenge({
            dispatchQualityScore: 40,
            hasLeadTechnician: false,
            hasEvidence: false,
            hasOpenOrder: false,
            escalationStage: "none",
            isSlaOverdue: true,
            demoRuns: 0,
        });
        expect(challenge.score).toBe(0);
        expect(challenge.verdict).toBe("critical");
    });
});

describe("buildMissionReport", () => {
    const base = {
        turbineId: "SITE-TX-WT-01",
        siteName: "Panhandle Ridge",
        component: "Generator",
        priority: "P2",
        responder: "Aline Laurent",
        dispatchQualityScore: 100,
        challengeScore: 85,
        challengeVerdict: "ready",
        outcome: "Open order (P2)",
    };

    it("computes duration from the first and last event timestamps", () => {
        const report = buildMissionReport({
            ...base,
            events: [
                { step: "story", at: "2026-07-14T10:00:00.000Z" },
                { step: "dispatch", at: "2026-07-14T10:00:06.000Z" },
            ],
        });
        expect(report.stepCount).toBe(2);
        expect(report.durationMs).toBe(6000);
        expect(report.turbineId).toBe("SITE-TX-WT-01");
        expect(report.responder).toBe("Aline Laurent");
    });

    it("treats a single event as zero duration and defaults a missing responder", () => {
        const report = buildMissionReport({
            ...base,
            responder: null,
            events: [{ step: "manual", at: "2026-07-14T10:00:00.000Z" }],
        });
        expect(report.durationMs).toBe(0);
        expect(report.responder).toBe("unassigned");
        expect(report.stepCount).toBe(1);
    });

    it("carries the challenge score, verdict, and outcome through", () => {
        const report = buildMissionReport({ ...base, events: [] });
        expect(report.challengeScore).toBe(85);
        expect(report.challengeVerdict).toBe("ready");
        expect(report.outcome).toBe("Open order (P2)");
        expect(report.durationMs).toBe(0);
    });
});

describe("slaUrgency", () => {
    it("is on track early in the SLA window", () => {
        const u = slaUrgency(10, 90);
        expect(u.level).toBe("ok");
        expect(u.fraction).toBeCloseTo(10 / 90, 5);
    });

    it("escalates to warning past half the budget", () => {
        expect(slaUrgency(50, 90).level).toBe("warning");
    });

    it("escalates to critical near the deadline", () => {
        expect(slaUrgency(80, 90).level).toBe("critical");
    });

    it("reports breached once the age exceeds the budget", () => {
        const u = slaUrgency(120, 90);
        expect(u.level).toBe("breached");
        expect(u.fraction).toBe(1);
    });

    it("guards against a zero or negative budget", () => {
        const u = slaUrgency(5, 0);
        expect(u.level).toBe("breached");
        expect(u.fraction).toBe(1);
    });
});

describe("demoNarration", () => {
    it("orders the nine scripted steps 1..9 of 9", () => {
        expect(demoNarration("story").index).toBe(1);
        expect(demoNarration("locate").index).toBe(2);
        expect(demoNarration("twin").index).toBe(3);
        expect(demoNarration("schematic").index).toBe(4);
        expect(demoNarration("graph").index).toBe(5);
        expect(demoNarration("dispatch").index).toBe(6);
        expect(demoNarration("support").index).toBe(7);
        expect(demoNarration("simulation").index).toBe(8);
        expect(demoNarration("ask").index).toBe(9);
        expect(demoNarration("story").total).toBe(9);
    });

    it("provides a non-empty title and caption for every step", () => {
        for (const step of ["story", "locate", "twin", "schematic", "graph", "dispatch", "support", "simulation", "ask"] as const) {
            const n = demoNarration(step);
            expect(n.title.length).toBeGreaterThan(0);
            expect(n.caption.length).toBeGreaterThan(0);
        }
    });
});

describe("domain demo manifest", () => {
    it("keeps demoNarration in sync with the Wind manifest", () => {
        for (const step of DEMO_STEP_ORDER) {
            const fromManifest = narrateStep(WIND_DEMO_MANIFEST, step);
            const legacy = demoNarration(step);
            expect(legacy.title).toBe(fromManifest.title);
            expect(legacy.caption).toBe(fromManifest.caption);
            expect(legacy.index).toBe(fromManifest.index);
        }
    });

    it("orders narration steps from the manifest step order", () => {
        expect(narrateStep(WIND_DEMO_MANIFEST, "story").index).toBe(1);
        expect(narrateStep(WIND_DEMO_MANIFEST, "ask").index).toBe(DEMO_STEP_ORDER.length);
        expect(narrateStep(WIND_DEMO_MANIFEST, "dispatch").total).toBe(9);
    });

    it("drives narration from an arbitrary domain manifest", () => {
        const solar: DomainDemoManifest = {
            domainId: "solar-farm",
            entityNoun: "inverter",
            assetNoun: "string",
            components: ["Inverter", "Combiner"],
            responderRoles: ["PV Field Tech"],
            evidenceLabels: ["Hotspot thermal"],
            steps: {
                story: { title: "Solar story", caption: "Inverter incident framed." },
                locate: { title: "Solar locate", caption: "Map centered on string." },
                twin: { title: "Solar twin", caption: "Digital twin inspected." },
                graph: { title: "Solar graph", caption: "Graph relationships traced." },
                dispatch: { title: "Solar dispatch", caption: "PV tech dispatched." },
                support: { title: "Solar support", caption: "Field support called." },
                ask: { title: "Solar ask", caption: "Fabric IQ queried." },
                analytics: { title: "Solar analytics", caption: "Analytics visualized." },
            },
        };
        expect(narrateStep(solar, "story").title).toBe("Solar story");
        expect(narrateStep(solar, "dispatch").index).toBe(6);
    });
});

describe("buildEscalationTimeline", () => {
    it("marks dispatch as current and the rest pending when stage is none", () => {
        const t = buildEscalationTimeline("none", false);
        expect(t.map((e) => e.state)).toEqual(["current", "pending", "pending"]);
    });

    it("advances dispatch to done and manager to current at the manager stage", () => {
        const t = buildEscalationTimeline("manager", false);
        expect(t.map((e) => e.state)).toEqual(["done", "current", "pending"]);
    });

    it("marks all handoffs reached at the regional stage", () => {
        const t = buildEscalationTimeline("regional", true);
        expect(t.map((e) => e.state)).toEqual(["done", "done", "current"]);
    });

    it("flags an SLA breach note on the current pre-regional stage", () => {
        const t = buildEscalationTimeline("manager", true);
        const manager = t.find((e) => e.id === "manager");
        expect(manager?.note).toMatch(/SLA breach/i);
    });
});

describe("run history helpers", () => {
    const makeReport = (turbineId: string, at: string, durationMs = 6000): MissionReport => ({
        generatedAt: at,
        turbineId,
        siteName: "Panhandle Ridge",
        component: "Generator",
        priority: "P2",
        responder: "Aline Laurent",
        dispatchQualityScore: 100,
        challengeScore: 85,
        challengeVerdict: "ready",
        stepCount: 4,
        durationMs,
        events: [],
        outcome: "Open order (P2)",
    });

    it("prepends most-recent-first and caps the retained count", () => {
        let history: MissionReport[] = [];
        for (let i = 0; i < 12; i += 1) {
            history = pushMissionRun(history, makeReport(`WT-${i}`, `2026-07-14T10:00:${String(i).padStart(2, "0")}.000Z`), 10);
        }
        expect(history).toHaveLength(10);
        expect(history[0].turbineId).toBe("WT-11");
    });

    it("de-duplicates a report with the same identity", () => {
        const r = makeReport("WT-1", "2026-07-14T10:00:00.000Z");
        const history = pushMissionRun(pushMissionRun([], r), r);
        expect(history).toHaveLength(1);
    });

    it("summarizes a report into a list-friendly row", () => {
        const s = summarizeMissionRun(makeReport("WT-1", "2026-07-14T10:00:00.000Z", 6400));
        expect(s.turbineId).toBe("WT-1");
        expect(s.durationSec).toBe(6.4);
        expect(s.challengeVerdict).toBe("ready");
    });
});

describe("mission report parity", () => {
    const input = {
        turbineId: "SITE-TX-WT-01",
        siteName: "Panhandle Ridge",
        component: "Generator",
        priority: "P2",
        responder: "Aline Laurent",
        dispatchQualityScore: 100,
        challengeScore: 85,
        challengeVerdict: "ready",
        events: [
            { step: "story", at: "2026-07-14T10:00:00.000Z", detail: "Prepared incident story" },
            { step: "heal", at: "2026-07-14T10:00:06.000Z", detail: "AutoHeal complete" },
        ],
        outcome: "Open order (P2)",
    };

    it("locks the exported field set (shape stability)", () => {
        const report = buildMissionReport(input);
        expect(Object.keys(report).sort()).toEqual([
            "challengeScore",
            "challengeVerdict",
            "component",
            "dispatchQualityScore",
            "durationMs",
            "events",
            "generatedAt",
            "outcome",
            "priority",
            "responder",
            "siteName",
            "stepCount",
            "turbineId",
        ]);
    });

    it("maps stable fields deterministically (ignoring the timestamp)", () => {
        const report = buildMissionReport(input);
        const { generatedAt, ...stable } = report;
        expect(typeof generatedAt).toBe("string");
        expect(stable).toEqual({
            turbineId: "SITE-TX-WT-01",
            siteName: "Panhandle Ridge",
            component: "Generator",
            priority: "P2",
            responder: "Aline Laurent",
            dispatchQualityScore: 100,
            challengeScore: 85,
            challengeVerdict: "ready",
            stepCount: 2,
            durationMs: 6000,
            events: input.events,
            outcome: "Open order (P2)",
        });
    });

    it("keeps the run summary shape stable", () => {
        const summary = summarizeMissionRun(buildMissionReport(input));
        expect(Object.keys(summary).sort()).toEqual([
            "at",
            "challengeScore",
            "challengeVerdict",
            "component",
            "durationSec",
            "id",
            "stepCount",
            "turbineId",
        ]);
    });
});

describe("summarizeResponderAvailability", () => {
    const roster = [
        { shift: "day" as const, onCall: true, currentLoad: 0 },
        { shift: "day" as const, onCall: false, currentLoad: 2 },
        { shift: "swing" as const, onCall: true, currentLoad: 0 },
        { shift: "night" as const, onCall: true, currentLoad: 5 },
    ];

    it("counts on-call, free, and shift distribution", () => {
        const s = summarizeResponderAvailability(roster);
        expect(s.total).toBe(4);
        expect(s.onCall).toBe(3);
        expect(s.free).toBe(2);
        expect(s.byShift).toEqual({ day: 2, swing: 1, night: 1 });
        expect(s.busiestLoad).toBe(5);
    });

    it("returns a zeroed summary for an empty roster", () => {
        const s = summarizeResponderAvailability([]);
        expect(s.total).toBe(0);
        expect(s.onCall).toBe(0);
        expect(s.free).toBe(0);
        expect(s.byShift).toEqual({ day: 0, swing: 0, night: 0 });
        expect(s.busiestLoad).toBe(0);
    });
});

describe("demo manifest registry", () => {
    it("registers wind, solar, and refinery manifests", () => {
        expect(Object.keys(DEMO_MANIFESTS).sort()).toEqual(["oil-gas-refinery", "solar-farm", "wind-turbine"]);
        expect(DEMO_MANIFESTS["solar-farm"]).toBe(SOLAR_DEMO_MANIFEST);
        expect(DEMO_MANIFESTS["oil-gas-refinery"]).toBe(REFINERY_DEMO_MANIFEST);
    });

    it("resolves a manifest by id and falls back to Wind for unknown domains", () => {
        expect(getDemoManifest("solar-farm")).toBe(SOLAR_DEMO_MANIFEST);
        expect(getDemoManifest("does-not-exist")).toBe(WIND_DEMO_MANIFEST);
    });

    it("keeps every manifest complete across the ten scripted steps", () => {
        for (const manifest of Object.values(DEMO_MANIFESTS) as DomainDemoManifest[]) {
            for (const step of DEMO_STEP_ORDER) {
                const n = narrateStep(manifest, step);
                expect(n.title.length).toBeGreaterThan(0);
                expect(n.caption.length).toBeGreaterThan(0);
            }
            expect(manifest.components.length).toBeGreaterThan(0);
            expect(manifest.responderRoles.length).toBeGreaterThan(0);
            expect(manifest.evidenceLabels.length).toBeGreaterThan(0);
        }
    });
});
