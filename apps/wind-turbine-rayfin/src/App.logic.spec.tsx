//-----------------------------------------------------------------------
// <copyright company="Microsoft Corporation">
//        Copyright (c) Microsoft Corporation.  All rights reserved.
//        Licensed under the MIT license. See LICENSE file in the project root for full license information.
// </copyright>
//-----------------------------------------------------------------------

import { describe, it, expect, afterEach } from "vitest";
import { forecastDetail, anomalyScore, parseHash, newlyAlarmed, sourceLabel, signalState, deriveTurbineStatus, thresholdRows, formatBand, donutSegments, applyThresholdOverrides, clearThresholdOverrides, summarizeSites, forecastEscalation } from "@/App";
import { classifyAskIntent, normalizeAskQuestion } from "@/services/ask-routing.service";

type TurbineLike = Parameters<typeof anomalyScore>[0];

function turbine(nacelleTempC: number, vibrationMmS: number): TurbineLike {
    return {
        id: "SITE-TX-WT-01",
        siteId: "SITE-TX",
        siteName: "Panhandle Ridge",
        latitude: 0,
        longitude: 0,
        powerKw: 1000,
        windMs: 12,
        nacelleTempC,
        vibrationMmS,
        status: "healthy",
    } as TurbineLike;
}

describe("forecastDetail", () => {
    it("returns the last value with zero confidence when history is too short", () => {
        const r = forecastDetail([500, 520], 5);
        expect(r.value).toBe(520);
        expect(r.confidence).toBe(0);
        expect(r.lo).toBe(520);
        expect(r.hi).toBe(520);
    });

    it("extrapolates an upward linear trend", () => {
        const r = forecastDetail([100, 200, 300, 400, 500], 1);
        expect(r.value).toBe(600);
        expect(r.confidence).toBe(100);
    });

    it("keeps lo <= value <= hi and widens the band with horizon", () => {
        const history = [100, 180, 260, 300, 420, 470];
        const near = forecastDetail(history, 3);
        const far = forecastDetail(history, 12);
        expect(near.lo).toBeLessThanOrEqual(near.value);
        expect(near.value).toBeLessThanOrEqual(near.hi);
        expect(far.hi - far.lo).toBeGreaterThanOrEqual(near.hi - near.lo);
    });

    it("never forecasts a negative value", () => {
        const r = forecastDetail([500, 400, 300, 200, 100], 20);
        expect(r.value).toBeGreaterThanOrEqual(0);
        expect(r.lo).toBeGreaterThanOrEqual(0);
    });
});

describe("anomalyScore", () => {
    it("is zero for nominal temperature and vibration", () => {
        expect(anomalyScore(turbine(55, 3))).toBe(0);
    });

    it("is clamped to 1 at extreme readings", () => {
        expect(anomalyScore(turbine(120, 20))).toBe(1);
    });

    it("rises as readings approach thresholds", () => {
        expect(anomalyScore(turbine(75, 4))).toBeGreaterThan(anomalyScore(turbine(65, 4)));
    });
});

describe("newlyAlarmed", () => {
    it("flags only turbines that crossed from non-alarm into alarm", () => {
        const prev = { A: "healthy" as const, B: "alarm" as const, C: "warning" as const };
        const ids = newlyAlarmed(prev, [
            { id: "A", status: "alarm" },
            { id: "B", status: "alarm" },
            { id: "C", status: "warning" },
        ]);
        expect(ids).toEqual(["A"]);
    });

    it("treats a turbine with no prior status as a new alarm", () => {
        expect(newlyAlarmed({}, [{ id: "Z", status: "alarm" }])).toEqual(["Z"]);
    });

    it("returns nothing when no turbine is alarming", () => {
        expect(newlyAlarmed({ A: "alarm" }, [{ id: "A", status: "healthy" }])).toEqual([]);
    });
});

describe("parseHash", () => {
    it("returns the view and decoded turbine id from a valid hash", () => {
        window.location.hash = "#/twin/SITE-TX-WT-01";
        expect(parseHash()).toEqual({ view: "twin", selectedId: "SITE-TX-WT-01" });
    });

    it("ignores unknown views", () => {
        window.location.hash = "#/bogus/SITE-TX-WT-01";
        expect(parseHash().view).toBeUndefined();
    });

    it("returns undefined fields for an empty hash", () => {
        window.location.hash = "";
        expect(parseHash()).toEqual({ view: undefined, selectedId: undefined });
    });
});

describe("sourceLabel", () => {
    it("labels a live Data Agent answer", () => {
        expect(sourceLabel("fabriciq")).toMatch(/Data Agent/i);
    });

    it("labels an ontology-grounded answer", () => {
        expect(sourceLabel("ontology")).toMatch(/Ontology/i);
    });

    it("labels an offline local answer", () => {
        expect(sourceLabel("local")).toMatch(/offline/i);
    });
});

describe("normalizeAskQuestion", () => {
    it("trims and collapses whitespace", () => {
        expect(normalizeAskQuestion("  Which   site has  alarms?  ")).toBe("which site has alarms?");
    });
});

describe("classifyAskIntent", () => {
    it("routes operational triage prompts to fastpath", () => {
        expect(classifyAskIntent("Any turbines in alarm right now?")).toBe("ops-fastpath");
        expect(classifyAskIntent("What is the hottest nacelle?")).toBe("ops-fastpath");
    });

    it("routes analytical prompts to analytics", () => {
        expect(classifyAskIntent("Show output trend over time by site")).toBe("analytics");
        expect(classifyAskIntent("Compare last 24 hour average by country")).toBe("analytics");
    });

    it("keeps ambiguous prompts in hybrid mode", () => {
        expect(classifyAskIntent("Why is SITE-TX-WT-01 underperforming?")).toBe("hybrid");
    });
});

describe("forecastEscalation", () => {
    it("treats short histories as stable", () => {
        expect(forecastEscalation([])).toMatchObject({ direction: "stable", etaToAlarmTicks: null });
        expect(forecastEscalation([0.4, 0.5])).toMatchObject({ direction: "stable", etaToAlarmTicks: null });
    });

    it("flags a rising trend and projects ticks to alarm", () => {
        const f = forecastEscalation([0.2, 0.4, 0.6, 0.8]);
        expect(f.direction).toBe("rising");
        expect(f.slopePerTick).toBeCloseTo(0.2, 3);
        expect(f.etaToAlarmTicks).toBe(1);
    });

    it("flags a falling trend with no ETA", () => {
        const f = forecastEscalation([0.9, 0.7, 0.5, 0.3]);
        expect(f.direction).toBe("falling");
        expect(f.etaToAlarmTicks).toBeNull();
    });

    it("treats a flat trend as stable", () => {
        expect(forecastEscalation([0.5, 0.5, 0.5, 0.5]).direction).toBe("stable");
    });
});

describe("summarizeSites", () => {
    const turbines = [
        { siteId: "A", status: "healthy" as const, powerKw: 1000, windMs: 10, nacelleTempC: 50, vibrationMmS: 3 },
        { siteId: "A", status: "alarm" as const, powerKw: 500, windMs: 12, nacelleTempC: 90, vibrationMmS: 9 },
        { siteId: "B", status: "warning" as const, powerKw: 800, windMs: 8, nacelleTempC: 70, vibrationMmS: 5 },
    ];
    const sites = [
        { id: "A", name: "Alpha", capacityMw: 1 },
        { id: "B", name: "Bravo", capacityMw: 2 },
        { id: "C", name: "Charlie", capacityMw: 3 },
    ];

    it("rolls turbines into per-site totals and health counts", () => {
        const [a, b, c] = summarizeSites(turbines, sites);
        expect(a).toMatchObject({ id: "A", turbineCount: 2, totalKw: 1500, alarms: 1, warnings: 0, healthy: 1 });
        expect(b).toMatchObject({ id: "B", turbineCount: 1, totalKw: 800, alarms: 0, warnings: 1, healthy: 0 });
        expect(c).toMatchObject({ id: "C", turbineCount: 0, totalKw: 0, alarms: 0, warnings: 0, healthy: 0 });
    });

    it("computes capacity factor against rated MW and zero-safe empty sites", () => {
        const [a, , c] = summarizeSites(turbines, sites);
        // rated = 1 MW * 2 turbines = 2 MW; output 1.5 MW => 75%.
        expect(a.ratedMw).toBe(2);
        expect(a.capacityFactor).toBeCloseTo(75, 5);
        expect(c.capacityFactor).toBe(0);
    });

    it("averages signal readings per site", () => {
        const [a] = summarizeSites(turbines, sites);
        expect(a.avgWindMs).toBeCloseTo(11, 5);
        expect(a.avgNacelleTempC).toBeCloseTo(70, 5);
        expect(a.avgVibrationMmS).toBeCloseTo(6, 5);
    });
});

describe("signalState", () => {
    it("bands nacelle temperature into healthy/warning/alarm", () => {
        expect(signalState("temp", 50)).toBe("healthy");
        expect(signalState("temp", 70)).toBe("warning");
        expect(signalState("temp", 85)).toBe("alarm");
    });

    it("bands vibration into healthy/warning/alarm", () => {
        expect(signalState("vibration", 3)).toBe("healthy");
        expect(signalState("vibration", 6)).toBe("warning");
        expect(signalState("vibration", 8)).toBe("alarm");
    });

    it("treats power as informational (always healthy) but bands wind by speed", () => {
        expect(signalState("power", 9999)).toBe("healthy");
        expect(signalState("wind", 10)).toBe("healthy");
        expect(signalState("wind", 18)).toBe("warning");
        expect(signalState("wind", 25)).toBe("alarm");
    });
});

describe("deriveTurbineStatus", () => {
    it("is healthy when temp and vibration are nominal", () => {
        expect(deriveTurbineStatus(50, 3)).toBe("healthy");
    });

    it("escalates to the worst band across health signals", () => {
        expect(deriveTurbineStatus(70, 3)).toBe("warning");
        expect(deriveTurbineStatus(85, 3)).toBe("alarm");
        expect(deriveTurbineStatus(50, 9)).toBe("alarm");
    });
});

describe("thresholdRows", () => {
    it("flattens every signal into an ontology-grounded threshold row", () => {
        const rows = thresholdRows();
        expect(rows.map((r) => r.signalKey)).toEqual(["power", "wind", "temp", "vibration"]);
    });

    it("carries the ontology property, unit and bands for a health signal", () => {
        const temp = thresholdRows().find((r) => r.signalKey === "temp");
        expect(temp).toMatchObject({
            ontologyProperty: "GeneratorTempC",
            unit: "\u00b0C",
            warn: 67,
            alarm: 79,
            governsHealth: true,
        });
    });

    it("marks power as informational (unbanded) and not health-governing", () => {
        const power = thresholdRows().find((r) => r.signalKey === "power");
        expect(power?.governsHealth).toBe(false);
        expect(Number.isFinite(power?.alarm ?? Infinity)).toBe(false);
    });
});

describe("ontology-driven threshold overrides", () => {
    afterEach(() => {
        clearThresholdOverrides();
    });

    it("adopts a valid backend band and drives classification at runtime", () => {
        // Default nacelle-temp band is warn 67 / alarm 79 — 55°C is nominal by default.
        expect(signalState("temp", 55)).toBe("healthy");
        const applied = applyThresholdOverrides([
            { signalKey: "temp", ontologyProperty: "GeneratorTempC", unit: "\u00b0C", warn: 40, alarm: 50, governsHealth: true },
        ]);
        expect(applied).toBe(1);
        expect(signalState("temp", 55)).toBe("alarm");
        expect(signalState("temp", 45)).toBe("warning");
    });

    it("surfaces overridden bands through thresholdRows and derived status", () => {
        applyThresholdOverrides([
            { signalKey: "vibration", ontologyProperty: "VibrationMmS", unit: "mm/s", warn: 2, alarm: 3, governsHealth: true },
        ]);
        const vib = thresholdRows().find((r) => r.signalKey === "vibration");
        expect(vib).toMatchObject({ warn: 2, alarm: 3 });
        // 4 mm/s was healthy by default (warn 4), now alarms under the tighter band.
        expect(deriveTurbineStatus(50, 4)).toBe("alarm");
    });

    it("ignores malformed rows (unknown key, inverted or negative bands)", () => {
        const applied = applyThresholdOverrides([
            { signalKey: "bogus", ontologyProperty: "X", unit: "", warn: 1, alarm: 2, governsHealth: false },
            { signalKey: "temp", ontologyProperty: "GeneratorTempC", unit: "\u00b0C", warn: 90, alarm: 70, governsHealth: true },
            { signalKey: "vibration", ontologyProperty: "VibrationMmS", unit: "mm/s", warn: -1, alarm: 5, governsHealth: true },
        ]);
        expect(applied).toBe(0);
        // Defaults intact: temp alarm stays 79, vibration warn stays 4.
        expect(signalState("temp", 85)).toBe("alarm");
        expect(signalState("temp", 55)).toBe("healthy");
        expect(signalState("vibration", 3)).toBe("healthy");
    });

    it("clearThresholdOverrides restores the compiled-in defaults", () => {
        applyThresholdOverrides([
            { signalKey: "temp", ontologyProperty: "GeneratorTempC", unit: "\u00b0C", warn: 40, alarm: 50, governsHealth: true },
        ]);
        expect(signalState("temp", 55)).toBe("alarm");
        clearThresholdOverrides();
        expect(signalState("temp", 55)).toBe("healthy");
        expect(thresholdRows().find((r) => r.signalKey === "temp")).toMatchObject({ warn: 67, alarm: 79 });
    });
});

describe("formatBand", () => {
    it("renders the warn and alarm limits for a banded signal", () => {
        expect(formatBand({ warn: 67, alarm: 79, unit: "\u00b0C" })).toBe("warn \u2265 67\u00b0C \u00b7 alarm \u2265 79\u00b0C");
    });

    it("labels an unbanded signal as informational", () => {
        expect(formatBand({ warn: Infinity, alarm: Infinity, unit: "kW" })).toBe("informational");
    });
});

describe("donutSegments", () => {
    it("returns cumulative fractions that sum to 1", () => {
        const segs = donutSegments([
            { label: "A", value: 30, color: "#1" },
            { label: "B", value: 10, color: "#2" },
            { label: "C", value: 10, color: "#3" },
        ]);
        expect(segs.map((s) => s.fraction)).toEqual([0.6, 0.2, 0.2]);
        expect(segs.map((s) => s.offset)).toEqual([0, 0.6, 0.8]);
        expect(segs.reduce((sum, s) => sum + s.fraction, 0)).toBeCloseTo(1, 10);
    });

    it("returns no segments when every value is zero or negative", () => {
        expect(donutSegments([{ label: "A", value: 0, color: "#1" }, { label: "B", value: -5, color: "#2" }])).toEqual([]);
    });

    it("ignores negative values when computing fractions", () => {
        const segs = donutSegments([
            { label: "A", value: 75, color: "#1" },
            { label: "B", value: 25, color: "#2" },
            { label: "C", value: -100, color: "#3" },
        ]);
        expect(segs[0].fraction).toBeCloseTo(0.75, 10);
        expect(segs[2].fraction).toBe(0);
    });
});
