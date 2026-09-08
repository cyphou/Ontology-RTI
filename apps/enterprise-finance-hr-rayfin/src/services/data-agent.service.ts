import type { PlanningMetrics } from "./planning-metrics.service";

export interface PlanningAnswer {
    text: string;
    source: "agent" | "fallback";
}

export function isDataAgentConfigured(url = import.meta.env.VITE_DATA_AGENT_URL): boolean {
    return Boolean(url?.trim());
}

export function localPlanningAnswer(question: string, metrics: PlanningMetrics): PlanningAnswer {
    const varianceDirection = metrics.variance > 0 ? "above" : "below";
    return {
        source: "fallback",
        text: `Local planning answer: actual spend is ${Math.abs(metrics.variance).toLocaleString("en-US", { style: "currency", currency: "USD", maximumFractionDigits: 0 })} ${varianceDirection} budget. Review aggregate capacity assumptions before changing the forecast.`,
    };
}

export async function askDataAgent(question: string, metrics: PlanningMetrics, url = import.meta.env.VITE_DATA_AGENT_URL): Promise<PlanningAnswer> {
    if (!url?.trim()) return localPlanningAnswer(question, metrics);
    try {
        const response = await fetch(url, {
            method: "POST",
            headers: { "Content-Type": "application/json" },
            credentials: "include",
            body: JSON.stringify({ question, context: { metrics, privacy: "aggregate workforce only; no automated HR decisions" } }),
        });
        if (!response.ok) throw new Error(`Data Agent responded ${response.status}`);
        const payload = await response.json() as { answer?: unknown; summary?: unknown; text?: unknown };
        const text = [payload.answer, payload.summary, payload.text].find((value): value is string => typeof value === "string" && Boolean(value.trim()));
        if (!text) throw new Error("Data Agent returned no answer.");
        return { source: "agent", text: text.trim() };
    } catch {
        return localPlanningAnswer(question, metrics);
    }
}