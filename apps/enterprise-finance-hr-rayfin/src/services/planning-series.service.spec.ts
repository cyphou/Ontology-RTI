import { describe, expect, it } from 'vitest';
import { buildPlanningSeries, seriesMax } from './planning-series.service';
import { syntheticPlanningMetrics } from './planning-metrics.service';

describe('planning series', () => {
    it('builds the selected rolling window from planning measures', () => {
        const series = buildPlanningSeries(syntheticPlanningMetrics, 6);
        expect(series).toHaveLength(6);
        expect(series[0].label).toBe('Jul');
        expect(series.at(-1)?.label).toBe('Dec');
        expect(series.every((point) => point.budget > 0 && point.actual > 0 && point.forecast > 0)).toBe(true);
    });

    it('returns a usable scale for chart bars', () => {
        const series = buildPlanningSeries(syntheticPlanningMetrics, 3);
        expect(seriesMax(series)).toBeGreaterThan(series[0].actual);
    });
});
