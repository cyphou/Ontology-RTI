import { describe, expect, it } from "vitest";
import { enterprisePlanningMetricsDax, loadPlanningMetrics, parsePlanningMetrics, syntheticPlanningMetrics } from "./planning-metrics.service";

const response = {
    table: {
        columns: ["Budget", "Actual", "Forecast", "Headcount", "FTE", "Variance", "VariancePercent"],
        rows: [[1000, 1100, 1200, 20, 19.5, 100, 10]],
    },
};

describe("planning metrics service", () => {
    it("uses the defined Enterprise Finance + HR measures and parses a live query", async () => {
        const client = { semanticModel: (connection: string) => ({ query: async (dax: string) => {
            expect(connection).toBe("enterpriseFinanceHrModel");
            expect(dax).toBe(enterprisePlanningMetricsDax);
            return response;
        } }) };
        const result = await loadPlanningMetrics(client, "enterpriseFinanceHrModel");
        expect(result.state).toBe("live");
        expect(result.metrics).toMatchObject({ actual: 1100, fte: 19.5, variancePercent: 10 });
    });

    it("uses synthetic aggregate metrics when no named connection is configured", async () => {
        const result = await loadPlanningMetrics(undefined, "");
        expect(result).toMatchObject({ state: "fallback", metrics: syntheticPlanningMetrics });
    });

    it("keeps the dashboard usable when a live query fails", async () => {
        const client = { semanticModel: () => ({ query: async () => { throw new Error("401 Unauthorized"); } }) };
        const result = await loadPlanningMetrics(client, "enterpriseFinanceHrModel");
        expect(result).toMatchObject({ state: "error", metrics: syntheticPlanningMetrics });
        expect(result.message).toContain("401 Unauthorized");
    });

    it("rejects an empty semantic-model row", () => {
        expect(parsePlanningMetrics({ table: { rows: [[]], columns: [] } })).toBeUndefined();
    });
});