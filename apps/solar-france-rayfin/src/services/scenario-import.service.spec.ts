import { describe, expect, it } from "vitest";
import { importScenarios } from "@/services/scenario-import.service";

describe("importScenarios", () => {
    it("imports photovoltaic plans from CSV", () => {
        const [plan] = importScenarios("label,curtailmentPct,downtimeTicks,horizonTicks,projectedPowerKw\nInverter service,5,2,24,950", "plans.csv");
        expect(plan).toMatchObject({ label: "Inverter service", curtailmentPct: 5, downtimeTicks: 2, horizonTicks: 24, projectedPowerKw: 950 });
    });

    it("imports JSON plans and treats empty input as no plans", () => {
        expect(importScenarios('[{"name":"Tracker check","horizon":8}]', "plans.json")[0]).toMatchObject({ label: "Tracker check", horizonTicks: 8 });
        expect(importScenarios("", "plans.csv")).toEqual([]);
    });
});