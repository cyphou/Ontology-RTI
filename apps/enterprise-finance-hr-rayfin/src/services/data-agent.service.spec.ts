import { describe, expect, it, vi } from "vitest";
import { askDataAgent, localPlanningAnswer } from "./data-agent.service";
import { syntheticPlanningMetrics } from "./planning-metrics.service";

describe("Data Agent planning seam", () => {
    it("returns a deterministic local planning answer when no endpoint is configured", async () => {
        const answer = await askDataAgent("What changed?", syntheticPlanningMetrics, undefined);
        expect(answer.source).toBe("fallback");
        expect(answer.text).toContain("Local planning answer");
    });

    it("falls back when the configured agent gateway is unavailable", async () => {
        vi.stubGlobal("fetch", vi.fn().mockRejectedValue(new Error("offline")));
        const answer = await askDataAgent("What changed?", syntheticPlanningMetrics, "https://agent.example.test/ask");
        expect(answer).toEqual(localPlanningAnswer("What changed?", syntheticPlanningMetrics));
        vi.unstubAllGlobals();
    });

    it("uses a successful agent answer without an embedded credential", async () => {
        vi.stubGlobal("fetch", vi.fn().mockResolvedValue({ ok: true, json: async () => ({ answer: "Live aggregate answer" }) }));
        const answer = await askDataAgent("What changed?", syntheticPlanningMetrics, "https://agent.example.test/ask");
        expect(answer).toEqual({ source: "agent", text: "Live aggregate answer" });
        vi.unstubAllGlobals();
    });
});