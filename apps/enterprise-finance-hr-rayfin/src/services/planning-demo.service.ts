import type { PlanningMetrics } from './planning-metrics.service';

export type PlanningDemoStep = 'baseline' | 'scenario' | 'review';

export interface PlanningDemoScenario {
  id: string;
  label: string;
  spendChangePercent: number;
  fteChange: number;
  rationale: string;
}

export interface PlanningDemoOutcome {
  projectedSpend: number;
  projectedVariance: number;
  projectedFte: number;
  requiresHumanReview: true;
}

export const planningDemoSteps: Record<PlanningDemoStep, { title: string; detail: string }> = {
  baseline: { title: '1. Establish baseline', detail: 'Review aggregate budget, actuals, forecast, and capacity before proposing a planning adjustment.' },
  scenario: { title: '2. Compare scenario', detail: 'Apply a synthetic planning assumption and compare its financial and workforce impact.' },
  review: { title: '3. Record human review', detail: 'Capture a finance and HR planning review. This demo never makes or recommends an employment decision.' },
};

export const planningDemoScenarios: PlanningDemoScenario[] = [
  { id: 'base', label: 'Maintain current plan', spendChangePercent: 0, fteChange: 0, rationale: 'Retain the approved aggregate cost and capacity plan.' },
  { id: 'capacity', label: 'Protect delivery capacity', spendChangePercent: 2.5, fteChange: 4, rationale: 'Fund a small, aggregate capacity buffer in delivery teams.' },
  { id: 'efficiency', label: 'Efficiency scenario', spendChangePercent: -1.5, fteChange: -1.5, rationale: 'Model a non-person-specific efficiency assumption for planning review.' },
];

export function evaluatePlanningScenario(metrics: PlanningMetrics, scenario: PlanningDemoScenario): PlanningDemoOutcome {
  const projectedSpend = Math.round(metrics.forecast * (1 + scenario.spendChangePercent / 100));
  return {
    projectedSpend,
    projectedVariance: projectedSpend - metrics.budget,
    projectedFte: Math.max(0, metrics.fte + scenario.fteChange),
    requiresHumanReview: true,
  };
}