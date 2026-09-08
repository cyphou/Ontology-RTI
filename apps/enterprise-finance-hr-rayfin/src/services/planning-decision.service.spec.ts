import { describe, expect, it } from 'vitest';
import { buildPlanningDecisions, transitionDecisionStatus } from './planning-decision.service';

const input = {
  planning: { budget: 1000, actual: 1120, forecast: 1150, headcount: 20, fte: 18, variance: 120, variancePercent: 12 },
  compensation: { total: 1100, base: 900, fte: 18, planned: 1000 },
  attendance: { scheduled: 800, worked: 820, approved: 790, overtime: 40 },
  absence: { absenceHours: 32, scheduledHours: 800, populationCount: 20 },
  recruitment: { requisitions: 2, openings: 4, activeCandidates: 5, offers: 1, acceptedOffers: 1 },
};

describe('planning decision queue', () => {
  it('creates aggregate review tasks without individual-level fields', () => {
    const decisions = buildPlanningDecisions(input);
    expect(decisions.length).toBeGreaterThan(1);
    expect(decisions.every((decision) => decision.requiresHumanReview && decision.populationCount >= 5)).toBe(true);
    expect(JSON.stringify(decisions)).not.toMatch(/employee|candidateId|salary|absenceReason/i);
  });

  it('suppresses absence review below the minimum group size', () => {
    const decisions = buildPlanningDecisions({ ...input, absence: { ...input.absence, populationCount: 4 } });
    expect(decisions.some((decision) => decision.id === 'absence')).toBe(false);
  });

  it('allows review transitions only', () => {
    expect(transitionDecisionStatus('new', 'send-for-review')).toBe('in-review');
    expect(transitionDecisionStatus('in-review', 'record-review')).toBe('reviewed');
    expect(transitionDecisionStatus('new', 'record-review')).toBe('new');
  });
});