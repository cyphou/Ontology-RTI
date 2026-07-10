import { entity, authenticated, uuid, text, int, date } from "@microsoft/rayfin-core";

/**
 * Writeback target — a predictive maintenance work order raised against an
 * ontology turbine. This is the durable store behind the "Raise work order"
 * action, closing the loop from the anomaly escalation forecast + what-if
 * simulator to a tracked, structured intervention (component, priority,
 * planned curtailment/downtime and its projected energy impact).
 */
@entity()
@authenticated("*")
export class MaintenanceOrder {
    @uuid() id!: string;
    @text({ max: 48 }) turbineId!: string;
    @text({ max: 32 }) siteId!: string;
    @text({ max: 16 }) component!: string;
    @text({ max: 4 }) priority!: string;
    @text({ max: 16 }) status!: string;
    @int() curtailPct!: number;
    @int() downtimeTicks!: number;
    @int() projectedDeltaKwt!: number;
    @text({ max: 120 }) assignee!: string;
    @text({ max: 500 }) note!: string;
    @date() createdAt!: Date;
}
