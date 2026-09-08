import { describe, expect, it } from 'vitest';
import { attendanceSummary } from './attendance.service';
import { absenceSummary, suppressSmallGroup } from './absence.service';
import { compensationSummary } from './compensation.service';
import { recruitmentSummary, requiresAttention } from './recruitment.service';

describe('aggregate HR services', () => {
  it('calculates compensation and time metrics', () => { expect(compensationSummary({ total: 100, base: 80, fte: 2, planned: 90 }).payrollCostPerFte).toBe(50); expect(attendanceSummary({ scheduled: 100, worked: 110, approved: 95, overtime: 10 }).attendanceRate).toBe(.95); });
  it('calculates absence and recruitment rates', () => { expect(absenceSummary({ absenceHours: 16, scheduledHours: 160 }).absenceRate).toBe(.1); expect(recruitmentSummary({ requisitions: 2, openings: 3, activeCandidates: 8, offers: 4, acceptedOffers: 3 }).acceptanceRate).toBe(.75); });
  it('suppresses small groups and flags thin recruitment coverage', () => { expect(suppressSmallGroup({ count: 4, label: 'x' })).toBeUndefined(); expect(suppressSmallGroup({ count: 5, label: 'x' })).toBeDefined(); expect(requiresAttention({ requisitions: 1, openings: 4, activeCandidates: 7, offers: 0, acceptedOffers: 0 })).toBe(true); });
});