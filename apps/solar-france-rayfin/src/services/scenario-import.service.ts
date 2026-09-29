import type { ScenarioSpec } from "./scenario-lab.service";

function numberAt(row: Record<string, string>, key: string): number | undefined {
    const raw = String(row[key] ?? "").replace(/[,%]/g, "").trim();
    if (raw === "") return undefined;
    const parsed = Number(raw);
    return Number.isFinite(parsed) ? parsed : undefined;
}

function toScenario(row: Record<string, string>, index: number): ScenarioSpec {
    return {
        id: row.id?.trim() || `imported-plan-${index + 1}`,
        label: row.label?.trim() || row.name?.trim() || `Imported plan ${index + 1}`,
        curtailmentPct: numberAt(row, "curtailmentPct") ?? numberAt(row, "curtailment") ?? 0,
        downtimeTicks: numberAt(row, "downtimeTicks") ?? numberAt(row, "downtime") ?? 0,
        horizonTicks: numberAt(row, "horizonTicks") ?? numberAt(row, "horizon") ?? 12,
        projectedPowerKw: numberAt(row, "projectedPowerKw") ?? numberAt(row, "powerKw"),
        tariffEurPerKwh: numberAt(row, "tariffEurPerKwh"),
        maintenanceCostEur: numberAt(row, "maintenanceCostEur"),
    };
}

export function importScenarios(text: string, fileName = "scenario"): ScenarioSpec[] {
    const trimmed = text.trim();
    if (!trimmed) return [];
    if (fileName.toLowerCase().endsWith(".json")) {
        const rows = JSON.parse(trimmed) as Record<string, string>[];
        return Array.isArray(rows) ? rows.map(toScenario) : [];
    }
    const [header, ...lines] = trimmed.split(/\r?\n/).filter(Boolean);
    const keys = header.split(",").map((key) => key.trim());
    return lines.map((line) => Object.fromEntries(keys.map((key, index) => [key, line.split(",")[index] ?? ""]))).map(toScenario);
}