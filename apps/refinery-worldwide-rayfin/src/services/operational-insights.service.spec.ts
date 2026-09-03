import { describe, expect, it } from "vitest";
import { buildBlastRadius, dataTrust } from "@/services/operational-insights.service";

const now = new Date("2026-08-27T12:00:00.000Z");

describe("dataTrust", () => {
    it("marks an unconfigured source as simulated", () => {
        expect(dataTrust(false).mode).toBe("simulated");
    });

    it("marks a recent live timestamp as live", () => {
        expect(dataTrust(true, "2026-08-27T11:59:20.000Z", now).mode).toBe("live");
    });

    it("marks an old timestamp as stale", () => {
        expect(dataTrust(true, "2026-08-27T11:50:00.000Z", now).mode).toBe("stale");
    });
});

describe("buildBlastRadius", () => {
    it("connects an incident to refinery-native upstream/downstream constraints", () => {
        const radius = buildBlastRadius({ unitId: "PORTARTHUR-U-01", siteId: "PORTARTHUR", siteName: "Port Arthur", asset: "Heat Exchanger", severity: "Critical", hasOpenOrder: false });
        expect(radius.upstream.join(" ")).toMatch(/pipeline/i);
        expect(radius.downstream.join(" ")).toMatch(/tank/i);
        expect(radius.constraints).toContain("Tank inventory / overflow risk");
        expect(radius.action).toMatch(/Create a work order/i);
    });
});
