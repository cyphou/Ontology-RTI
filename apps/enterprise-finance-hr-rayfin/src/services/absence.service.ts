export interface AbsenceMetrics { absenceHours: number; scheduledHours: number; }
export function absenceSummary(metrics: AbsenceMetrics) { return { absenceDays: metrics.absenceHours / 8, absenceRate: metrics.scheduledHours ? metrics.absenceHours / metrics.scheduledHours : 0 }; }
export function suppressSmallGroup<T extends { count: number }>(group: T, minimum = 5): T | undefined { return group.count >= minimum ? group : undefined; }
export const syntheticAbsence: AbsenceMetrics = { absenceHours: 240, scheduledHours: 5_824 };