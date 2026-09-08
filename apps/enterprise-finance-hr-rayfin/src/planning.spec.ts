import { describe, expect, it } from 'vitest';
import { canViewPseudonymizedWorkforce, variance, variancePercent } from './planning';

describe('planning metrics', () => {
  it('calculates variance', () => { expect(variance(100, 125)).toBe(25); expect(variancePercent(100, 125)).toBe(25); });
  it('handles zero budget', () => { expect(variancePercent(0, 125)).toBe(0); });
  it('guards workforce detail', () => { expect(canViewPseudonymizedWorkforce('finance')).toBe(false); expect(canViewPseudonymizedWorkforce('leader')).toBe(false); expect(canViewPseudonymizedWorkforce('hr-analyst')).toBe(true); });
});