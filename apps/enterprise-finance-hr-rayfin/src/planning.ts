export type WorkforceRole = 'finance' | 'leader' | 'hr-analyst';

export function variance(budget: number, actual: number): number {
	return actual - budget;
}

export function variancePercent(budget: number, actual: number): number {
	return budget === 0 ? 0 : variance(budget, actual) / budget * 100;
}

export function canViewPseudonymizedWorkforce(role: WorkforceRole): boolean {
	return role === 'hr-analyst';
}