//-----------------------------------------------------------------------
// <copyright company="Microsoft Corporation">
//        Copyright (c) Microsoft Corporation.  All rights reserved.
//        Licensed under the MIT license. See LICENSE file in the project root for full license information.
// </copyright>
//-----------------------------------------------------------------------

import { describe, it, expect, afterEach } from "vitest";
import { forecastDetail, anomalyScore, parseHash, newlyAlarmed, sourceLabel, signalState, derivePlantStatus, thresholdRows, formatBand, donutSegments, applyThresholdOverrides, clearThresholdOverrides, classifyAskIntent, normalizeAskQuestion } from "@/App";

type TurbineLike = Parameters<typeof anomalyScore>[0];

function plant(moduleTempC: number, inverterLoadPct: number): TurbineLike {
    return {
        id: "JAMNAGAR-U-01",
        siteId: "JAMNAGAR",
        siteName: "Jamnagar",
        latitude: 0,
        longitude: 0,
        powerKw: 120,
        irradianceWm2: 150,
        moduleTempC,
        inverterLoadPct,
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
    it("is zero for nominal unit temperature and utilization", () => {
        expect(anomalyScore(plant(350, 70))).toBe(0);
    });

    it("is clamped to 1 at extreme readings", () => {
        expect(anomalyScore(plant(500, 100))).toBe(1);
    });

    it("rises as readings approach thresholds", () => {
        expect(anomalyScore(plant(410, 70))).toBeGreaterThan(anomalyScore(plant(390, 70)));
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
        expect(classifyAskIntent("Any units in alarm right now?")).toBe("ops-fastpath");
        expect(classifyAskIntent("What is the hottest unit temperature?")).toBe("ops-fastpath");
    });

    it("routes analytical prompts to analytics", () => {
        expect(classifyAskIntent("Show output trend over time by site")).toBe("analytics");
        expect(classifyAskIntent("Compare last 24 hour average by region")).toBe("analytics");
    });

    it("keeps ambiguous prompts in hybrid mode", () => {
        expect(classifyAskIntent("Why is JAMNAGAR-U-01 underperforming?")).toBe("hybrid");
    });
});

describe("signalState", () => {
    it("bands unit temperature into healthy/warning/alarm", () => {
        expect(signalState("moduleTemp", 350)).toBe("healthy");
        expect(signalState("moduleTemp", 400)).toBe("warning");
        expect(signalState("moduleTemp", 440)).toBe("alarm");
    });

    it("bands utilization into healthy/warning/alarm", () => {
        expect(signalState("inverterLoad", 80)).toBe("healthy");
        expect(signalState("inverterLoad", 92)).toBe("warning");
        expect(signalState("inverterLoad", 99)).toBe("alarm");
    });

    it("treats throughput and feed rate as informational (always healthy)", () => {
        expect(signalState("power", 9999)).toBe("healthy");
        expect(signalState("irradiance", 9999)).toBe("healthy");
    });
});

describe("derivePlantStatus", () => {
    it("is healthy when unit temp and utilization are nominal", () => {
        expect(derivePlantStatus(350, 70)).toBe("healthy");
    });

    it("escalates to the worst band across health signals", () => {
        expect(derivePlantStatus(400, 70)).toBe("warning");
        expect(derivePlantStatus(440, 70)).toBe("alarm");
        expect(derivePlantStatus(350, 99)).toBe("alarm");
    });
});

describe("thresholdRows", () => {
    it("flattens every signal into an ontology-grounded threshold row", () => {
        const rows = thresholdRows();
        expect(rows.map((r) => r.signalKey)).toEqual(["power", "irradiance", "moduleTemp", "inverterLoad"]);
    });

    it("carries the ontology property, unit and bands for a health signal", () => {
        const temp = thresholdRows().find((r) => r.signalKey === "moduleTemp");
        expect(temp).toMatchObject({
            ontologyProperty: "UnitTempC",
            unit: "\u00b0C",
            warn: 380,
            alarm: 430,
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
        expect(signalState("moduleTemp", 300)).toBe("healthy");
        const applied = applyThresholdOverrides([
            { signalKey: "moduleTemp", ontologyProperty: "UnitTempC", unit: "\u00b0C", warn: 200, alarm: 250, governsHealth: true },
        ]);
        expect(applied).toBe(1);
        expect(signalState("moduleTemp", 300)).toBe("alarm");
        expect(signalState("moduleTemp", 220)).toBe("warning");
    });

    it("surfaces overridden bands through thresholdRows and derived status", () => {
        applyThresholdOverrides([
            { signalKey: "inverterLoad", ontologyProperty: "UtilizationPct", unit: "%", warn: 70, alarm: 80, governsHealth: true },
        ]);
        const load = thresholdRows().find((r) => r.signalKey === "inverterLoad");
        expect(load).toMatchObject({ warn: 70, alarm: 80 });
        expect(derivePlantStatus(300, 82)).toBe("alarm");
    });

    it("ignores malformed rows (unknown key, inverted or negative bands)", () => {
        const applied = applyThresholdOverrides([
            { signalKey: "bogus", ontologyProperty: "X", unit: "", warn: 1, alarm: 2, governsHealth: true },
            { signalKey: "moduleTemp", ontologyProperty: "UnitTempC", unit: "\u00b0C", warn: 450, alarm: 400, governsHealth: true },
            { signalKey: "inverterLoad", ontologyProperty: "UtilizationPct", unit: "%", warn: -1, alarm: 50, governsHealth: true },
        ]);
        expect(applied).toBe(0);
        expect(signalState("moduleTemp", 450)).toBe("alarm");
        expect(signalState("moduleTemp", 300)).toBe("healthy");
        expect(signalState("inverterLoad", 80)).toBe("healthy");
    });

    it("clearThresholdOverrides restores the compiled-in defaults", () => {
        applyThresholdOverrides([
            { signalKey: "moduleTemp", ontologyProperty: "UnitTempC", unit: "\u00b0C", warn: 200, alarm: 250, governsHealth: true },
        ]);
        expect(signalState("moduleTemp", 300)).toBe("alarm");
        clearThresholdOverrides();
        expect(signalState("moduleTemp", 300)).toBe("healthy");
        expect(thresholdRows().find((r) => r.signalKey === "moduleTemp")).toMatchObject({ warn: 380, alarm: 430 });
    });
});

describe("formatBand", () => {
    it("renders the warn and alarm limits for a banded signal", () => {
        expect(formatBand({ warn: 380, alarm: 430, unit: "\u00b0C" })).toBe("warn \u2265 380\u00b0C \u00b7 alarm \u2265 430\u00b0C");
    });

    it("labels an unbanded signal as informational", () => {
        expect(formatBand({ warn: Infinity, alarm: Infinity, unit: "kbd" })).toBe("informational");
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
