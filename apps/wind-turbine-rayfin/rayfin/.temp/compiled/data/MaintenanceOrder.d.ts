/**
 * Writeback target — a predictive maintenance work order raised against an
 * ontology turbine. This is the durable store behind the "Raise work order"
 * action, closing the loop from the anomaly escalation forecast + what-if
 * simulator to a tracked, structured intervention (component, priority,
 * planned curtailment/downtime and its projected energy impact).
 */
export declare class MaintenanceOrder {
    id: string;
    turbineId: string;
    siteId: string;
    component: string;
    priority: string;
    status: string;
    curtailPct: number;
    downtimeTicks: number;
    projectedDeltaKwt: number;
    assignee: string;
    note: string;
    createdAt: Date;
}
