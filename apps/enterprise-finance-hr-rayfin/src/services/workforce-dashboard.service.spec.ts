import { describe, expect, it } from 'vitest';
import { breakdownMax, workforceDashboard } from './workforce-dashboard.service';

describe('workforce dashboard', () => {
    it('changes the trend window and department scope', () => {
        const scoped = workforceDashboard(6, 'IT');
        expect(scoped.trend).toHaveLength(6);
        expect(scoped.departments.map((item) => item.label)).toEqual(['IT']);
        expect(scoped.leaving).toBeGreaterThan(0);
    });

    it('provides a safe scale for breakdown charts', () => {
        expect(breakdownMax(workforceDashboard(3).departments)).toBe(60);
    });
});
