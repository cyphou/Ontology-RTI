export type PlanningDataState = "live" | "fallback" | "error";

export interface PlanningMetrics {
    budget: number;
    actual: number;
    forecast: number;
    headcount: number;
    fte: number;
    variance: number;
    variancePercent: number;
}

export interface PlanningMetricsResult {
    metrics: PlanningMetrics;
    state: PlanningDataState;
    message: string;
}

export interface SemanticModelClient {
    semanticModel(connection: string): { query(query: string): Promise<unknown> };
}

export const enterprisePlanningMetricsDax = `EVALUATE
ROW(
    "Budget", [Budget],
    "Actual", [Actual],
    "Forecast", [Forecast],
    "Headcount", [Headcount],
    "FTE", [FTE],
    "Variance", [Variance],
    "VariancePercent", [Variance %]
)`;

export const syntheticPlanningMetrics: PlanningMetrics = {
    budget: 1_295_000,
    actual: 1_292_500,
    forecast: 1_319_000,
    headcount: 145,
    fte: 145.7,
    variance: -2_500,
    variancePercent: -0.19,
};

function numeric(value: unknown): number {
    const parsed = typeof value === "number" ? value : Number(value);
    return Number.isFinite(parsed) ? parsed : 0;
}

function readFirstRow(payload: unknown): Record<string, unknown> | undefined {
    if (!payload || typeof payload !== "object") return undefined;
    const table = (payload as { table?: unknown }).table;
    if (!table || typeof table !== "object") return undefined;
    const rows = (table as { rows?: unknown }).rows;
    if (!Array.isArray(rows) || rows.length === 0) return undefined;
    const first = rows[0];
    if (first && typeof first === "object" && !Array.isArray(first)) return first as Record<string, unknown>;
    const columns = (table as { columns?: unknown }).columns;
    if (!Array.isArray(first) || !Array.isArray(columns)) return undefined;
    return Object.fromEntries(columns.map((column, index) => [
        typeof column === "string" ? column : (column as { name?: string }).name ?? String(index),
        first[index],
    ]));
}

export function parsePlanningMetrics(payload: unknown): PlanningMetrics | undefined {
    const row = readFirstRow(payload);
    if (!row) return undefined;
    const budget = numeric(row.Budget);
    const actual = numeric(row.Actual);
    const forecast = numeric(row.Forecast);
    const headcount = numeric(row.Headcount);
    const fte = numeric(row.FTE);
    if (budget === 0 && actual === 0 && forecast === 0 && headcount === 0 && fte === 0) return undefined;
    return {
        budget,
        actual,
        forecast,
        headcount,
        fte,
        variance: numeric(row.Variance),
        variancePercent: numeric(row.VariancePercent),
    };
}

export function configuredSemanticModel(
    connection = import.meta.env.VITE_RAYFIN_LIVE_TELEMETRY_MODEL ?? import.meta.env.VITE_LIVE_TELEMETRY_MODEL,
): string | undefined {
    return connection?.trim() || undefined;
}

async function configuredClient(): Promise<SemanticModelClient> {
    const { getFabricClient } = await import("../lib/fabric-client");
    return getFabricClient() as unknown as SemanticModelClient;
}

export async function loadPlanningMetrics(
    client: SemanticModelClient | undefined = undefined,
    connection = configuredSemanticModel(),
): Promise<PlanningMetricsResult> {
    if (!connection) {
        return { metrics: syntheticPlanningMetrics, state: "fallback", message: "Synthetic aggregate planning data" };
    }
    try {
        const payload = await (client ?? await configuredClient()).semanticModel(connection).query(enterprisePlanningMetricsDax);
        const metrics = parsePlanningMetrics(payload);
        if (!metrics) throw new Error("Semantic model returned no aggregate planning metrics.");
        return { metrics, state: "live", message: `Live Fabric semantic model: ${connection}` };
    } catch (error) {
        const detail = error instanceof Error ? error.message : "Unable to query the semantic model.";
        return { metrics: syntheticPlanningMetrics, state: "error", message: `${detail} Showing synthetic aggregate data.` };
    }
}