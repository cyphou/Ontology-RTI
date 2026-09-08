import type { AbsenceMetrics } from './absence.service';
import type { AttendanceMetrics } from './attendance.service';
import type { CompensationMetrics } from './compensation.service';
import type { PlanningMetrics } from './planning-metrics.service';
import type { RecruitmentMetrics } from './recruitment.service';

export type DecisionStatus = 'new' | 'in-review' | 'reviewed';

export interface PlanningDecision {
  id: 'spend' | 'capacity' | 'absence' | 'recruitment';
  scopeLabel: string;
  populationCount: number;
  status: DecisionStatus;
  evidence: string[];
  recommendedNextStep: string;
  requiresHumanReview: true;
}

export interface AggregateDecisionInputs {
  planning: PlanningMetrics;
  compensation: CompensationMetrics;
  attendance: AttendanceMetrics;
  absence: AbsenceMetrics & { populationCount: number };
  recruitment: RecruitmentMetrics;
}

const MINIMUM_GROUP_SIZE = 5;

export function buildPlanningDecisions(inputs: AggregateDecisionInputs): PlanningDecision[] {
  const decisions: PlanningDecision[] = [];
  const financePopulation = Math.max(inputs.planning.headcount, MINIMUM_GROUP_SIZE);

  if (inputs.planning.variance > 0 || inputs.compensation.total > inputs.compensation.planned) {
    decisions.push({
      id: 'spend',
      scopeLabel: 'Enterprise cost portfolio',
      populationCount: financePopulation,
      status: 'new',
      evidence: [`Spend variance ${inputs.planning.variance.toFixed(0)}`, `Compensation variance ${(inputs.compensation.total - inputs.compensation.planned).toFixed(0)}`],
      recommendedNextStep: 'Send the aggregate cost position to Finance review.',
      requiresHumanReview: true,
    });
  }

  if (inputs.planning.fte < inputs.planning.headcount || inputs.attendance.overtime > inputs.attendance.worked * 0.025) {
    decisions.push({
      id: 'capacity',
      scopeLabel: 'Delivery capacity portfolio',
      populationCount: financePopulation,
      status: 'new',
      evidence: [`FTE ${inputs.planning.fte.toFixed(1)}`, `Overtime ${(inputs.attendance.overtime / Math.max(1, inputs.attendance.worked) * 100).toFixed(1)}%`],
      recommendedNextStep: 'Review aggregate capacity assumptions with Finance and HR.',
      requiresHumanReview: true,
    });
  }

  if (inputs.absence.populationCount >= MINIMUM_GROUP_SIZE && inputs.absence.absenceHours / Math.max(1, inputs.absence.scheduledHours) >= 0.03) {
    decisions.push({
      id: 'absence',
      scopeLabel: 'Workforce availability portfolio',
      populationCount: inputs.absence.populationCount,
      status: 'new',
      evidence: [`Absence ${(inputs.absence.absenceHours / inputs.absence.scheduledHours * 100).toFixed(1)}%`, `Scheduled hours ${inputs.absence.scheduledHours.toFixed(0)}`],
      recommendedNextStep: 'Review availability impact in the aggregate workforce plan.',
      requiresHumanReview: true,
    });
  }

  if (inputs.recruitment.openings > 0 && inputs.recruitment.activeCandidates < inputs.recruitment.openings * 2) {
    decisions.push({
      id: 'recruitment',
      scopeLabel: 'Open demand portfolio',
      populationCount: financePopulation,
      status: 'new',
      evidence: [`Openings ${inputs.recruitment.openings}`, `Active pipeline ${inputs.recruitment.activeCandidates}`],
      recommendedNextStep: 'Review demand coverage and approved workforce-plan assumptions.',
      requiresHumanReview: true,
    });
  }

  return decisions;
}

export function transitionDecisionStatus(status: DecisionStatus, action: 'send-for-review' | 'record-review'): DecisionStatus {
  if (action === 'send-for-review' && status === 'new') return 'in-review';
  if (action === 'record-review' && status === 'in-review') return 'reviewed';
  return status;
}