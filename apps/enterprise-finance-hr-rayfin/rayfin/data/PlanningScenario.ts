import { authenticated, date, entity, text, uuid } from "@microsoft/rayfin-core";

/** Aggregate planning proposal only; employee detail and automated HR decisions are prohibited. */
@entity()
@authenticated("*")
export class PlanningScenario {
    @uuid() id!: string;
    @text({ max: 96, unique: true }) scenarioId!: string;
    @text({ max: 120 }) name!: string;
    @text({ max: 24 }) status!: string;
    @text({ max: 64 }) baselineSource!: string;
    @text({ max: 120 }) createdBy!: string;
    @date() createdAt!: Date;
    @text({ max: 4000 }) aggregateAssumptions!: string;
}