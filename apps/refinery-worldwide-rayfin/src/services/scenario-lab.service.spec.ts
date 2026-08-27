//-----------------------------------------------------------------------
// <copyright company="Microsoft Corporation">
//        Copyright (c) Microsoft Corporation.  All rights reserved.
//        Licensed under the MIT license. See LICENSE file in the project root for full license information.
// </copyright>
//-----------------------------------------------------------------------

import { describe, it, expect } from "vitest";

import {
    compareScenarios,
    forecastVsRealised,
    buildScenarioPrompt,
    summarizeComparison,
    computeInsights,
    answerScenarioQuestion,
    type ScenarioSpec,
} from "@/services/scenario-lab.service";

const specs: ScenarioSpec[] = [
    { id: "base", label: "Run as-is", curtailmentPct: 0, downtimeTicks: 0, horizonTicks: 12 },
    { id: "trim", label: "Trim 15%", curtailmentPct: 15, downtimeTicks: 0, horizonTicks: 12 },
    { id: "maint", label: "Maintenance window", curtailmentPct: 0, downtimeTicks: 4, horizonTicks: 12 },
];

describe("compareScenarios", () => {
    it("ranks the least-disruptive plan first and flags best/worst", () => {
        const c = compareScenarios(1000, specs);
        const best = c.scenarios.find((s) => s.isBest);
        expect(best?.id).toBe("base");
        expect(c.bestId).toBe("base");
        expect(c.worstId).toBe("maint");
        expect(best?.rank).toBe(1);
    });

    it("computes projected throughput and volume delta vs baseline", () => {
        const c = compareScenarios(1000, specs);
        const trim = c.scenarios.find((s) => s.id === "trim")!;
        expect(trim.projectedKbd).toBe(850);
        // 850 kbd x 12 ticks - 1000 x 12 = -1800 kbd-t
        expect(trim.volumeDelta).toBe(-1800);
        expect(trim.deltaPct).toBeCloseTo(-15, 5);
    });

    it("keeps a positive spread between best and worst", () => {
        const c = compareScenarios(1000, specs);
        expect(c.spread).toBeGreaterThan(0);
    });

    it("handles an empty scenario list", () => {
        const c = compareScenarios(1000, []);
        expect(c.scenarios).toHaveLength(0);
        expect(c.bestId).toBeNull();
        expect(c.spread).toBe(0);
    });
});

describe("forecastVsRealised", () => {
    it("reports over-forecast bias when the forecast exceeds realised", () => {
        const v = forecastVsRealised([100, 100, 100], 120);
        expect(v.bias).toBe("over");
        expect(v.realisedMean).toBe(100);
        expect(v.forecast).toBe(120);
        expect(v.absErrorPct).toBeCloseTo(20, 5);
        expect(v.accuracyPct).toBeCloseTo(80, 5);
    });

    it("reports under-forecast bias when the forecast trails realised", () => {
        expect(forecastVsRealised([100, 100, 100], 80).bias).toBe("under");
    });

    it("reports on-track within the tolerance band", () => {
        expect(forecastVsRealised([100, 100, 100], 101).bias).toBe("on-track");
    });

    it("guards an empty realised history", () => {
        const v = forecastVsRealised([], 50);
        expect(v.realisedMean).toBe(0);
        expect(v.forecast).toBe(50);
    });
});

describe("narrative helpers", () => {
    it("builds a prompt that names every scenario and the variance", () => {
        const comparison = compareScenarios(1000, specs);
        const variance = forecastVsRealised([980, 1010, 1000], 1005);
        const prompt = buildScenarioPrompt(comparison, variance, "process unit");
        expect(prompt).toContain("Run as-is");
        expect(prompt).toContain("Trim 15%");
        expect(prompt).toContain("Maintenance window");
        expect(prompt).toContain("realised mean");
        expect(prompt).toContain("process unit");
    });

    it("summarizes the recommended plan deterministically offline", () => {
        const comparison = compareScenarios(1000, specs);
        const variance = forecastVsRealised([980, 1010, 1000], 1005);
        const summary = summarizeComparison(comparison, variance, "process unit");
        expect(summary).toContain("Run as-is");
        expect(summary.length).toBeGreaterThan(0);
    });

    it("prompts to add a scenario when the list is empty", () => {
        const comparison = compareScenarios(1000, []);
        const variance = forecastVsRealised([100], 100);
        expect(summarizeComparison(comparison, variance)).toMatch(/add a scenario/i);
    });
});

describe("computeInsights", () => {
    it("summarizes leaders, spread, dispersion and downtime risk", () => {
        const comparison = compareScenarios(1000, specs);
        const insights = computeInsights(comparison);
        expect(insights.planCount).toBe(3);
        expect(insights.bestLabel).toBe("Run as-is");
        expect(insights.worstLabel).toBe("Maintenance window");
        expect(insights.riskiestLabel).toBe("Maintenance window");
        expect(insights.maxDowntime).toBe(4);
        expect(insights.spread).toBeGreaterThan(0);
    });

    it("guards an empty comparison", () => {
        const insights = computeInsights(compareScenarios(1000, []));
        expect(insights.planCount).toBe(0);
        expect(insights.bestLabel).toBeNull();
    });
});

describe("answerScenarioQuestion", () => {
    const comparison = compareScenarios(1000, specs);
    const variance = forecastVsRealised([980, 1010, 1000], 1005);
    const insights = computeInsights(comparison);
    const ctx = { comparison, variance, insights };

    it("routes risk questions to downtime exposure", () => {
        const a = answerScenarioQuestion("what is the risk?", ctx, "process unit");
        expect(a).toMatch(/downtime/i);
        expect(a).toContain("Maintenance window");
    });

    it("routes accuracy questions to forecast variance", () => {
        expect(answerScenarioQuestion("how accurate is the forecast?", ctx)).toMatch(/accuracy|track|%/i);
    });

    it("lists all plans on a full-comparison question", () => {
        const a = answerScenarioQuestion("show me the full comparison of all plans", ctx);
        expect(a).toContain("Run as-is");
        expect(a).toContain("Maintenance window");
    });

    it("falls back to a recommendation for a generic question", () => {
        expect(answerScenarioQuestion("what should I do?", ctx).length).toBeGreaterThan(0);
    });

    it("handles an empty comparison", () => {
        const empty = compareScenarios(1000, []);
        const emptyCtx = { comparison: empty, variance, insights: computeInsights(empty) };
        expect(answerScenarioQuestion("anything", emptyCtx)).toMatch(/add or import/i);
    });
});
