export interface ScenarioSpec {
    id: string;
    label: string;
    curtailmentPct: number;
    downtimeTicks: number;
    horizonTicks: number;
    projectedPowerKw?: number;
    irradianceWm2?: number;
    moduleTempC?: number;
    inverterLoadPct?: number;
    tariffEurPerKwh?: number;
    maintenanceCostEur?: number;
}

export interface ScenarioComputed extends ScenarioSpec {
    projectedPowerKw: number;
    runningTicks: number;
    energyKwt: number;
    baselineEnergyKwt: number;
    energyDeltaKwt: number;
    deltaPct: number;
    rank: number;
    isBest: boolean;
    revenueEur?: number;
    netDeltaEur?: number;
}

export interface ScenarioComparison {
    baselineKw: number;
    scenarios: ScenarioComputed[];
    bestId: string | null;
    worstId: string | null;
    spread: number;
}

export function compareScenarios(baselineKw: number, specs: ScenarioSpec[]): ScenarioComparison {
    const baseline = Math.max(0, baselineKw);
    const scenarios = specs.map((spec) => {
        const horizon = Math.max(1, Math.round(spec.horizonTicks));
        const downtime = Math.min(horizon, Math.max(0, Math.round(spec.downtimeTicks)));
        const projectedPowerKw = Number.isFinite(spec.projectedPowerKw)
            ? Math.max(0, Math.round(spec.projectedPowerKw!))
            : Math.round(baseline * (1 - Math.min(100, Math.max(0, spec.curtailmentPct)) / 100));
        const energyKwt = projectedPowerKw * (horizon - downtime);
        const baselineEnergyKwt = baseline * horizon;
        const energyDeltaKwt = energyKwt - baselineEnergyKwt;
        const tariff = Math.max(0, spec.tariffEurPerKwh ?? 0);
        const maintenance = Math.max(0, spec.maintenanceCostEur ?? 0);
        return {
            ...spec, horizonTicks: horizon, downtimeTicks: downtime, projectedPowerKw, runningTicks: horizon - downtime,
            energyKwt, baselineEnergyKwt, energyDeltaKwt, deltaPct: baselineEnergyKwt === 0 ? 0 : +(energyDeltaKwt / baselineEnergyKwt * 100).toFixed(1),
            rank: 0, isBest: false,
            ...(Number.isFinite(spec.tariffEurPerKwh) || Number.isFinite(spec.maintenanceCostEur)
                ? { revenueEur: energyKwt * tariff, netDeltaEur: energyDeltaKwt * tariff - maintenance }
                : {}),
        };
    });
    const ordered = [...scenarios].sort((left, right) => right.energyDeltaKwt - left.energyDeltaKwt);
    ordered.forEach((scenario, index) => { scenario.rank = index + 1; scenario.isBest = index === 0; });
    const best = ordered[0];
    const worst = ordered.at(-1);
    return { baselineKw: baseline, scenarios, bestId: best?.id ?? null, worstId: ordered.length > 1 ? worst?.id ?? null : null, spread: best && worst ? best.energyDeltaKwt - worst.energyDeltaKwt : 0 };
}

export function summarizeComparison(comparison: ScenarioComparison): string {
    if (comparison.scenarios.length === 0) return "Add a solar operating plan to compare projected generation.";
    const best = comparison.scenarios.find((scenario) => scenario.id === comparison.bestId) ?? comparison.scenarios[0];
    return `Recommended plan: ${best.label}, with ${best.energyDeltaKwt >= 0 ? "+" : ""}${best.energyDeltaKwt.toLocaleString()} kW-t against baseline over ${best.horizonTicks} ticks.${comparison.scenarios.length > 1 ? ` It leads the comparison by ${comparison.spread.toLocaleString()} kW-t.` : ""}`;
}