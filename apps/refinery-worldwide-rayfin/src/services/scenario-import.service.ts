//-----------------------------------------------------------------------
// <copyright company="Microsoft Corporation">
//        Copyright (c) Microsoft Corporation.  All rights reserved.
//        Licensed under the MIT license. See LICENSE file in the project root for full license information.
// </copyright>
//-----------------------------------------------------------------------

// Pure parser for imported Excel/CSV rows. The App turns a file into an array of
// header-keyed rows (via a lazily-loaded SheetJS), then this classifies them into
// either editable what-if scenarios or an actuals/forecast series to compare against.
// Free of React and of the file-reading library so it is fully unit-testable.

import type { ScenarioSpec } from "./scenario-lab.service";

export interface ImportedScenariosResult {
    kind: "scenarios";
    scenarios: ScenarioSpec[];
}

export interface ImportedActualsPoint {
    label: string;
    actual: number;
    forecast?: number;
}

export interface ImportedActualsResult {
    kind: "actuals";
    name: string;
    points: ImportedActualsPoint[];
}

export interface ImportEmptyResult {
    kind: "empty";
    reason: string;
}

export type ImportResult = ImportedScenariosResult | ImportedActualsResult | ImportEmptyResult;

type Row = Record<string, unknown>;

const SYNONYMS = {
    label: ["label", "name", "scenario", "plan", "case"],
    curtailment: ["curtailmentpct", "curtailment", "curtail", "reduction", "reductionpct", "throttlepct", "throttle"],
    downtime: ["downtimeticks", "downtime", "outage", "outageticks", "maintenance", "maintticks"],
    horizon: ["horizonticks", "horizon", "periods", "ticks", "duration", "window"],
    throughput: ["throughputkbd", "projectedthroughputkbd", "scenariothroughputkbd"],
    feedRate: ["feedratekbd", "feedkbd", "scenariosfeedratekbd"],
    unitTemp: ["unittempc", "temperaturec", "scenariounittempc"],
    utilization: ["utilizationpct", "utilization", "scenarioutilizationpct"],
    price: ["priceperbarrelusd", "priceusdperbarrel", "productpriceusd"],
    variableCost: ["variablecostperbarrelusd", "variablecostusdperbarrel", "operatingcostperbarrelusd"],
    maintenanceCost: ["maintenancecostusd", "maintenanceusd", "interventioncostusd"],
    energyCost: ["energycostusd", "energyusd"],
    actual: ["actual", "actuals", "realised", "realized", "throughput", "value", "kbd", "output", "produced"],
    forecast: ["forecast", "forecasted", "predicted", "projection", "projected", "plan", "planned", "expected"],
    period: ["period", "timestamp", "time", "date", "hour", "tick", "day", "month", "label", "name"],
} as const;

function normalizeKeyMap(row: Row): Map<string, unknown> {
    const map = new Map<string, unknown>();
    for (const [k, v] of Object.entries(row)) {
        map.set(k.toLowerCase().replace(/[\s_%()./-]/g, ""), v);
    }
    return map;
}

function pick(map: Map<string, unknown>, synonyms: readonly string[]): unknown {
    for (const key of synonyms) {
        if (map.has(key)) {
            return map.get(key);
        }
    }
    return undefined;
}

function toNumber(value: unknown): number | null {
    if (typeof value === "number" && Number.isFinite(value)) {
        return value;
    }
    if (typeof value === "string") {
        const cleaned = value.replace(/[, %]/g, "").trim();
        if (cleaned === "") {
            return null;
        }
        const n = Number(cleaned);
        return Number.isFinite(n) ? n : null;
    }
    return null;
}

function hasAny(map: Map<string, unknown>, synonyms: readonly string[]): boolean {
    return synonyms.some((k) => map.has(k));
}

// Classify parsed rows into scenarios (curtailment/downtime/horizon columns present) or
// an actuals series (actual/throughput column present), else empty with a reason.
export function classifyImport(rows: Row[], sourceName = "import"): ImportResult {
    const clean = rows.filter((r) => r && Object.keys(r).length > 0);
    if (clean.length === 0) {
        return { kind: "empty", reason: "No rows found in the file." };
    }
    const maps = clean.map(normalizeKeyMap);
    const first = maps[0];

    const looksLikeScenarios = hasAny(first, SYNONYMS.curtailment) || hasAny(first, SYNONYMS.downtime);
    if (looksLikeScenarios) {
        const scenarios: ScenarioSpec[] = maps.map((m, i) => {
            const horizon = toNumber(pick(m, SYNONYMS.horizon)) ?? 12;
            const label = (pick(m, SYNONYMS.label) as string | undefined)?.toString().trim() || `Imported ${i + 1}`;
            return {
                id: `imp-${i}-${label.replace(/\s+/g, "-").toLowerCase()}`.slice(0, 40),
                label,
                curtailmentPct: Math.min(100, Math.max(0, toNumber(pick(m, SYNONYMS.curtailment)) ?? 0)),
                downtimeTicks: Math.max(0, toNumber(pick(m, SYNONYMS.downtime)) ?? 0),
                horizonTicks: Math.max(1, Math.round(horizon)),
                throughputKbd: toNumber(pick(m, SYNONYMS.throughput)) ?? undefined,
                feedRateKbd: toNumber(pick(m, SYNONYMS.feedRate)) ?? undefined,
                unitTempC: toNumber(pick(m, SYNONYMS.unitTemp)) ?? undefined,
                utilizationPct: toNumber(pick(m, SYNONYMS.utilization)) ?? undefined,
                pricePerBarrelUsd: toNumber(pick(m, SYNONYMS.price)) ?? undefined,
                variableCostPerBarrelUsd: toNumber(pick(m, SYNONYMS.variableCost)) ?? undefined,
                maintenanceCostUsd: toNumber(pick(m, SYNONYMS.maintenanceCost)) ?? undefined,
                energyCostUsd: toNumber(pick(m, SYNONYMS.energyCost)) ?? undefined,
            };
        });
        return { kind: "scenarios", scenarios };
    }

    const hasActual = hasAny(first, SYNONYMS.actual) || hasAny(first, SYNONYMS.forecast);
    if (hasActual) {
        const points: ImportedActualsPoint[] = [];
        maps.forEach((m, i) => {
            const actual = toNumber(pick(m, SYNONYMS.actual));
            const forecast = toNumber(pick(m, SYNONYMS.forecast));
            if (actual === null && forecast === null) {
                return;
            }
            const label = (pick(m, SYNONYMS.period) as string | number | undefined)?.toString().trim() || `t${i + 1}`;
            points.push({ label, actual: actual ?? forecast ?? 0, forecast: forecast ?? undefined });
        });
        if (points.length === 0) {
            return { kind: "empty", reason: "Found an actuals column but no numeric values." };
        }
        return { kind: "actuals", name: sourceName, points };
    }

    return { kind: "empty", reason: "Could not find scenario (curtailment/downtime) or actuals (throughput) columns." };
}

export interface ActualsSummary {
    n: number;
    mean: number;
    min: number;
    max: number;
    trend: "rising" | "falling" | "flat";
}

// Descriptive stats + a coarse trend for an imported actuals series.
export function summarizeActuals(points: ImportedActualsPoint[]): ActualsSummary {
    const values = points.map((p) => p.actual).filter((v) => Number.isFinite(v));
    if (values.length === 0) {
        return { n: 0, mean: 0, min: 0, max: 0, trend: "flat" };
    }
    const mean = values.reduce((s, v) => s + v, 0) / values.length;
    const first = values.slice(0, Math.max(1, Math.floor(values.length / 3)));
    const last = values.slice(-Math.max(1, Math.floor(values.length / 3)));
    const firstMean = first.reduce((s, v) => s + v, 0) / first.length;
    const lastMean = last.reduce((s, v) => s + v, 0) / last.length;
    const delta = lastMean - firstMean;
    const trend = delta > mean * 0.02 ? "rising" : delta < -mean * 0.02 ? "falling" : "flat";
    return {
        n: values.length,
        mean: +mean.toFixed(0),
        min: +Math.min(...values).toFixed(0),
        max: +Math.max(...values).toFixed(0),
        trend,
    };
}
