export type WorkforceWindow = 3 | 6 | 12;

export interface WorkforceTrendPoint {
    label: string;
    joining: number;
    leaving: number;
}

export interface WorkforceBreakdown {
    label: string;
    male: number;
    female: number;
    other: number;
}

export interface WorkforceDashboardData {
    employees: number;
    joining: number;
    leaving: number;
    averageTenure: number;
    trend: WorkforceTrendPoint[];
    departments: WorkforceBreakdown[];
    reasons: WorkforceBreakdown[];
    tenureSalary: Array<{ tenure: string; values: number[] }>;
}

const baseDashboard: WorkforceDashboardData = {
    employees: 1260,
    joining: 40,
    leaving: 45,
    averageTenure: 2.3,
    trend: [
        { label: 'Jan', joining: 30, leaving: 48 },
        { label: 'Feb', joining: 40, leaving: 25 },
        { label: 'Mar', joining: 15, leaving: 40 },
        { label: 'Apr', joining: 60, leaving: 35 },
        { label: 'May', joining: 40, leaving: 48 },
        { label: 'Jun', joining: 20, leaving: 52 },
        { label: 'Jul', joining: 36, leaving: 43 },
        { label: 'Aug', joining: 42, leaving: 31 },
        { label: 'Sep', joining: 28, leaving: 39 },
        { label: 'Oct', joining: 47, leaving: 29 },
        { label: 'Nov', joining: 35, leaving: 44 },
        { label: 'Dec', joining: 51, leaving: 33 },
    ],
    departments: [
        { label: 'Marketing', male: 30, female: 50, other: 4 },
        { label: 'HR', male: 40, female: 25, other: 3 },
        { label: 'IT', male: 15, female: 40, other: 4 },
        { label: 'Operations', male: 60, female: 35, other: 5 },
        { label: 'Engineering', male: 40, female: 55, other: 3 },
    ],
    reasons: [
        { label: 'Career move', male: 30, female: 23, other: 2 },
        { label: 'Compensation', male: 40, female: 32, other: 7 },
        { label: 'Role fit', male: 25, female: 15, other: 3 },
        { label: 'Manager change', male: 60, female: 40, other: 9 },
        { label: 'Relocation', male: 45, female: 40, other: 4 },
        { label: 'Other', male: 35, female: 20, other: 2 },
    ],
    tenureSalary: [
        { tenure: '<$50K', values: [4, 11, 14, 30, 23] },
        { tenure: '$51K-$100K', values: [6, 12, 12, 20, 20] },
        { tenure: '$101K-$150K', values: [7, 23, 32, 21, 44] },
        { tenure: '$151K-$300K', values: [8, 43, 34, 19, 15] },
        { tenure: '$301K+', values: [7, 21, 29, 17, 8] },
    ],
};

export const workforceDepartments = ['All', ...baseDashboard.departments.map((item) => item.label)];

export function workforceDashboard(window: WorkforceWindow, department = 'All'): WorkforceDashboardData {
    const trendStart = 12 - window;
    const departmentFactor = department === 'All' ? 1 : 0.22 + (department.length % 5) * 0.08;
    return {
        ...baseDashboard,
        joining: Math.round(baseDashboard.joining * departmentFactor),
        leaving: Math.round(baseDashboard.leaving * departmentFactor),
        trend: baseDashboard.trend.slice(trendStart),
        departments: department === 'All' ? baseDashboard.departments : baseDashboard.departments.filter((item) => item.label === department),
    };
}

export function breakdownMax(items: WorkforceBreakdown[]): number {
    return Math.max(1, ...items.flatMap((item) => [item.male, item.female, item.other]));
}
