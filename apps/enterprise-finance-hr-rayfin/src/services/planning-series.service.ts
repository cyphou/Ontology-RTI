import type { PlanningMetrics } from './planning-metrics.service';

export type PlanningWindow = 3 | 6 | 12;

export interface PlanningSeriesPoint {
    label: string;
    budget: number;
    actual: number;
    forecast: number;
    variance: number;
}

const monthLabels = ['Jan', 'Feb', 'Mar', 'Apr', 'May', 'Jun', 'Jul', 'Aug', 'Sep', 'Oct', 'Nov', 'Dec'];

export function buildPlanningSeries(metrics: PlanningMetrics, window: PlanningWindow): PlanningSeriesPoint[] {
    const start = 12 - window;
    return monthLabels.slice(start).map((label, index) => {
        const progress = (index + 1) / window;
        const budget = metrics.budget * (0.78 + progress * 0.22) / window;
        const actual = metrics.actual * (0.72 + progress * 0.28) / window;
        const forecast = metrics.forecast * (0.7 + progress * 0.3) / window;
        return {
            label,
            budget,
            actual,
            forecast,
            variance: actual - budget,
        };
    });
}

export function seriesMax(series: PlanningSeriesPoint[]): number {
    return Math.max(1, ...series.flatMap((point) => [point.budget, point.actual, point.forecast]));
}
