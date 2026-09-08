import { describe, expect, it } from 'vitest';
import { evaluatePlanningScenario, planningDemoScenarios } from './planning-demo.service';

describe('planning demo', () => {
  it('compares aggregate finance and capacity impacts without making a workforce decision', () => {
    const scenario = planningDemoScenarios.find((item) => item.id === 'capacity');
    expect(scenario).toBeDefined();
    const outcome = evaluatePlanningScenario({ budget: 1_000, actual: 980, forecast: 1_050, headcount: 20, fte: 19.5, variance: -20, variancePercent: -2 }, scenario!);
    expect(outcome).toMatchObject({ projectedSpend: 1_076, projectedVariance: 76, projectedFte: 23.5, requiresHumanReview: true });
  });

  it('does not allow a negative aggregate FTE result', () => {
    const outcome = evaluatePlanningScenario({ budget: 1_000, actual: 980, forecast: 1_050, headcount: 20, fte: 0.5, variance: -20, variancePercent: -2 }, planningDemoScenarios[2]);
    expect(outcome.projectedFte).toBe(0);
  });
});