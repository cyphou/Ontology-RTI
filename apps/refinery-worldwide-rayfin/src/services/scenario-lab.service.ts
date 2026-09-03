//-----------------------------------------------------------------------
// <copyright company="Microsoft Corporation">
//        Copyright (c) Microsoft Corporation.  All rights reserved.
//        Licensed under the MIT license. See LICENSE file in the project root for full license information.
// </copyright>
//-----------------------------------------------------------------------

// Pure engine for the Scenario Lab: rank multiple what-if scenarios against a
// baseline, compare forecast against realised throughput, and produce a deterministic
// offline narrative (the GenAI path in App.tsx reuses buildScenarioPrompt when a Data
// Agent is configured). Free of React and domain wording so any accelerator app can
// reuse it.

export interface ScenarioSpec {
    id: string;
    label: string;
    // % throughput reduction while the unit runs (0..100).
    curtailmentPct: number;
    // ticks the unit is taken offline for maintenance within the horizon.
    downtimeTicks: number;
    // planning horizon in ticks.
    horizonTicks: number;
    // Optional imported operating readings. When projected throughput is supplied,
    // it takes precedence over the curtailment-derived value for a real comparison.
    throughputKbd?: number;
    feedRateKbd?: number;
    unitTempC?: number;
    utilizationPct?: number;
    pricePerBarrelUsd?: number;
    variableCostPerBarrelUsd?: number;
    maintenanceCostUsd?: number;
    energyCostUsd?: number;
}

export interface ScenarioComputed extends ScenarioSpec {
    projectedKbd: number;
    runningTicks: number;
    volumeScenario: number;
    volumeBaseline: number;
    volumeDelta: number;
    deltaPct: number;
    rank: number;
    isBest: boolean;
    revenueUsd?: number;
    operatingCostUsd?: number;
    grossMarginUsd?: number;
    marginDeltaUsd?: number;
    costPerBarrelUsd?: number;
    roiPct?: number;
}

export interface ScenarioComparison {
    baselineKbd: number;
    scenarios: ScenarioComputed[];
    bestId: string | null;
    worstId: string | null;
    spread: number;
}

// Deterministic what-if: curtailment reduces running throughput; downtime removes
// running ticks. Volume is throughput x running ticks. Kept identical in spirit to the
// App's simulateScenario so the lab and the single-unit panel agree.
function computeScenario(baselineKbd: number, spec: ScenarioSpec): { projectedKbd: number; runningTicks: number; volumeScenario: number; volumeBaseline: number } {
    const baseline = Math.max(0, baselineKbd);
    const curtail = Math.min(100, Math.max(0, spec.curtailmentPct));
    const horizon = Math.max(0, Math.round(spec.horizonTicks));
    const downtime = Math.min(horizon, Math.max(0, Math.round(spec.downtimeTicks)));
    const projectedKbd = Number.isFinite(spec.throughputKbd)
        ? Math.max(0, Math.round(spec.throughputKbd!))
        : Math.round(baseline * (1 - curtail / 100));
    const runningTicks = horizon - downtime;
    return {
        projectedKbd,
        runningTicks,
        volumeScenario: projectedKbd * runningTicks,
        volumeBaseline: baseline * horizon,
    };
}

// Rank scenarios by volume delta vs baseline (highest = best). Adds delta %, rank, and
// a best/worst flag so the UI can highlight the recommended plan honestly.
export function compareScenarios(baselineKbd: number, specs: ScenarioSpec[]): ScenarioComparison {
    const computed: ScenarioComputed[] = specs.map((spec) => {
        const c = computeScenario(baselineKbd, spec);
        const volumeDelta = c.volumeScenario - c.volumeBaseline;
        const deltaPct = c.volumeBaseline > 0 ? (volumeDelta / c.volumeBaseline) * 100 : 0;
        const hasEconomics = [spec.pricePerBarrelUsd, spec.variableCostPerBarrelUsd, spec.maintenanceCostUsd, spec.energyCostUsd].some((value) => Number.isFinite(value));
        const price = Math.max(0, spec.pricePerBarrelUsd ?? 0);
        const variableCost = Math.max(0, spec.variableCostPerBarrelUsd ?? 0);
        const maintenanceCost = Math.max(0, spec.maintenanceCostUsd ?? 0);
        const energyCost = Math.max(0, spec.energyCostUsd ?? 0);
        const revenueUsd = c.volumeScenario * 1000 * price;
        const operatingCostUsd = c.volumeScenario * 1000 * variableCost + maintenanceCost + energyCost;
        const grossMarginUsd = revenueUsd - operatingCostUsd;
        const baselineMarginUsd = c.volumeBaseline * 1000 * (price - variableCost);
        const interventionCostUsd = maintenanceCost + energyCost;
        const marginDeltaUsd = grossMarginUsd - baselineMarginUsd;
        return {
            ...spec,
            projectedKbd: c.projectedKbd,
            runningTicks: c.runningTicks,
            volumeScenario: c.volumeScenario,
            volumeBaseline: c.volumeBaseline,
            volumeDelta,
            deltaPct: +deltaPct.toFixed(1),
            rank: 0,
            isBest: false,
            ...(hasEconomics ? {
                revenueUsd,
                operatingCostUsd,
                grossMarginUsd,
                marginDeltaUsd,
                costPerBarrelUsd: c.volumeScenario > 0 ? operatingCostUsd / (c.volumeScenario * 1000) : 0,
                roiPct: interventionCostUsd > 0 ? (marginDeltaUsd / interventionCostUsd) * 100 : undefined,
            } : {}),
        };
    });

    const ordered = [...computed].sort((a, b) => b.volumeDelta - a.volumeDelta);
    ordered.forEach((s, i) => {
        s.rank = i + 1;
        s.isBest = i === 0;
    });

    const best = ordered[0] ?? null;
    const worst = ordered[ordered.length - 1] ?? null;
    return {
        baselineKbd: Math.max(0, baselineKbd),
        scenarios: computed,
        bestId: best?.id ?? null,
        worstId: ordered.length > 1 ? (worst?.id ?? null) : null,
        spread: best && worst ? best.volumeDelta - worst.volumeDelta : 0,
    };
}

export interface ForecastVsRealised {
    realisedMean: number;
    realisedLast: number;
    forecast: number;
    absErrorPct: number;
    accuracyPct: number;
    bias: "over" | "under" | "on-track";
}

// Compare a forecast against the realised recent history. accuracyPct is 100 - MAPE-ish
// error of the forecast vs the realised mean; bias says whether the forecast ran over or
// under what actually happened.
export function forecastVsRealised(realised: number[], forecast: number): ForecastVsRealised {
    const clean = realised.filter((v) => Number.isFinite(v));
    const realisedMean = clean.length > 0 ? clean.reduce((s, v) => s + v, 0) / clean.length : 0;
    const realisedLast = clean.length > 0 ? clean[clean.length - 1] : 0;
    const denom = Math.max(1, Math.abs(realisedMean));
    const absErrorPct = +(Math.abs(forecast - realisedMean) / denom * 100).toFixed(1);
    const accuracyPct = +Math.max(0, 100 - absErrorPct).toFixed(1);
    let bias: ForecastVsRealised["bias"] = "on-track";
    if (forecast > realisedMean * 1.02) {
        bias = "over";
    } else if (forecast < realisedMean * 0.98) {
        bias = "under";
    }
    return { realisedMean: +realisedMean.toFixed(0), realisedLast: +realisedLast.toFixed(0), forecast: +forecast.toFixed(0), absErrorPct, accuracyPct, bias };
}

// Compact structured prompt for the Data Agent / GenAI path.
export function buildScenarioPrompt(
    comparison: ScenarioComparison,
    variance: ForecastVsRealised,
    assetNoun = "unit",
    options?: { insights?: ScenarioInsights; importedNote?: string; question?: string },
): string {
    const lines = comparison.scenarios.map(
        (s) => `- ${s.label}: curtail ${s.curtailmentPct}%, downtime ${s.downtimeTicks}t, horizon ${s.horizonTicks}t -> projected ${s.projectedKbd} kbd, volume delta ${s.volumeDelta >= 0 ? "+" : ""}${s.volumeDelta} kbd-t (${s.deltaPct >= 0 ? "+" : ""}${s.deltaPct}% vs baseline)`,
    );
    const out = [
        `Compare these ${assetNoun} operating scenarios against a baseline of ${comparison.baselineKbd} kbd${options?.question ? " and answer the question below" : " and recommend one"}.`,
        ...lines,
        `Forecast vs realised: forecast ${variance.forecast} kbd, realised mean ${variance.realisedMean} kbd, accuracy ${variance.accuracyPct}% (${variance.bias}).`,
    ];
    if (options?.insights) {
        const ins = options.insights;
        out.push(`Insights: best "${ins.bestLabel}", worst "${ins.worstLabel}", spread ${ins.spread} kbd-t, avg delta ${ins.avgDelta} kbd-t, dispersion ${ins.deltaStdDev}, riskiest "${ins.riskiestLabel}" (${ins.maxDowntime}t downtime).`);
    }
    if (options?.importedNote) {
        out.push(`Imported data: ${options.importedNote}`);
    }
    out.push(options?.question
        ? `Question: ${options.question}\nAnswer in one concise, grounded paragraph.`
        : `Give a one-paragraph recommendation naming the best scenario and the trade-off.`);
    return out.join("\n");
}

// Deterministic offline narrative used when no Data Agent is configured.
export function summarizeComparison(comparison: ScenarioComparison, variance: ForecastVsRealised, assetNoun = "unit"): string {
    if (comparison.scenarios.length === 0) {
        return "Add a scenario to compare operating plans.";
    }
    const best = comparison.scenarios.find((s) => s.id === comparison.bestId) ?? comparison.scenarios[0];
    const parts: string[] = [];
    parts.push(
        `Recommended plan: "${best.label}" — projected ${best.projectedKbd} kbd with a volume delta of ${best.volumeDelta >= 0 ? "+" : ""}${best.volumeDelta.toLocaleString()} kbd·t (${best.deltaPct >= 0 ? "+" : ""}${best.deltaPct}% vs baseline) over ${best.horizonTicks} ticks.`,
    );
    if (comparison.scenarios.length > 1) {
        parts.push(`It leads the field by ${Math.abs(comparison.spread).toLocaleString()} kbd·t against the weakest option.`);
    }
    const biasPhrase = variance.bias === "over"
        ? `the forecast is running ${variance.absErrorPct}% above realised throughput, so treat the upside cautiously`
        : variance.bias === "under"
            ? `the forecast is running ${variance.absErrorPct}% below realised throughput, so there may be headroom`
            : `the forecast tracks realised throughput within ${variance.absErrorPct}% (${variance.accuracyPct}% accuracy)`;
    parts.push(`On confidence: ${biasPhrase}.`);
    return parts.join(" ");
}

export interface ScenarioInsights {
    planCount: number;
    bestLabel: string | null;
    worstLabel: string | null;
    spread: number;
    avgDelta: number;
    deltaStdDev: number;
    riskiestLabel: string | null;
    maxDowntime: number;
}

// Roll a comparison up into headline insights: leaders, spread, average impact,
// dispersion (how much the plans disagree), and the riskiest plan by downtime exposure.
export function computeInsights(comparison: ScenarioComparison): ScenarioInsights {
    const s = comparison.scenarios;
    if (s.length === 0) {
        return { planCount: 0, bestLabel: null, worstLabel: null, spread: 0, avgDelta: 0, deltaStdDev: 0, riskiestLabel: null, maxDowntime: 0 };
    }
    const deltas = s.map((x) => x.volumeDelta);
    const avgDelta = deltas.reduce((a, b) => a + b, 0) / deltas.length;
    const variance = deltas.reduce((a, b) => a + (b - avgDelta) ** 2, 0) / deltas.length;
    const best = s.find((x) => x.id === comparison.bestId) ?? null;
    const worst = comparison.worstId ? (s.find((x) => x.id === comparison.worstId) ?? null) : null;
    const riskiest = [...s].sort((a, b) => b.downtimeTicks - a.downtimeTicks)[0] ?? null;
    return {
        planCount: s.length,
        bestLabel: best?.label ?? null,
        worstLabel: worst?.label ?? null,
        spread: +comparison.spread.toFixed(0),
        avgDelta: +avgDelta.toFixed(0),
        deltaStdDev: +Math.sqrt(variance).toFixed(0),
        riskiestLabel: riskiest?.label ?? null,
        maxDowntime: riskiest?.downtimeTicks ?? 0,
    };
}

// Offline free-text engine for the broader "ask about these scenarios" box. Keyword
// routing over the computed comparison/variance/insights so the app answers grounded
// questions with no Data Agent configured; falls back to the recommendation summary.
export function answerScenarioQuestion(
    question: string,
    ctx: { comparison: ScenarioComparison; variance: ForecastVsRealised; insights: ScenarioInsights; importedNote?: string },
    assetNoun = "unit",
): string {
    const q = question.toLowerCase();
    const { comparison, variance, insights, importedNote } = ctx;
    if (comparison.scenarios.length === 0) {
        return "Add or import a scenario to analyze.";
    }
    if (/(risk|down\s?time|outage|maintenance|safe)/.test(q)) {
        return `Highest exposure is "${insights.riskiestLabel}" with ${insights.maxDowntime} ticks of downtime. Plans disagree by a dispersion of ${insights.deltaStdDev.toLocaleString()} kbd·t, so the spread between best and worst is ${insights.spread.toLocaleString()} kbd·t — weigh the volume upside against that outage risk.`;
    }
    if (/(accuracy|variance|forecast|realis|confiden|trust)/.test(q)) {
        return `The forecast (${variance.forecast.toLocaleString()} kbd) is ${variance.bias === "on-track" ? "tracking" : `${variance.absErrorPct}% ${variance.bias}`} realised throughput (${variance.realisedMean.toLocaleString()} kbd), i.e. ${variance.accuracyPct}% accuracy${importedNote ? `. ${importedNote}` : ""}.`;
    }
    if (/(worst|avoid|least|bad)/.test(q) && insights.worstLabel) {
        return `The weakest plan is "${insights.worstLabel}" — it trails the best by ${insights.spread.toLocaleString()} kbd·t of volume.`;
    }
    if (/(all|each|every|table|full|compare|list)/.test(q)) {
        return comparison.scenarios
            .map((s) => `${s.label}: ${s.projectedKbd.toLocaleString()} kbd, ${s.volumeDelta >= 0 ? "+" : ""}${s.volumeDelta.toLocaleString()} kbd·t (${s.deltaPct >= 0 ? "+" : ""}${s.deltaPct}%), #${s.rank}`)
            .join("  •  ");
    }
    // Default: best-plan recommendation.
    return summarizeComparison(comparison, variance, assetNoun) + (importedNote ? ` ${importedNote}` : "");
}

