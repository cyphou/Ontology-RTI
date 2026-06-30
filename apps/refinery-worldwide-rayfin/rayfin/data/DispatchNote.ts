import { entity, authenticated, uuid, text, int, date } from "@microsoft/rayfin-core";

/**
 * Writeback target — an operator dispatch note attached to an ontology
 * turbine. This is the durable store behind the "Writeback This Turbine"
 * action. Records are keyed to ontology entity ids (turbineId / siteId).
 */
@entity()
@authenticated("*")
export class DispatchNote {
    @uuid() id!: string;
    @text({ max: 48 }) turbineId!: string;
    @text({ max: 32 }) siteId!: string;
    @text({ max: 16 }) status!: string;
    @int() powerKw!: number;
    @text({ max: 500 }) note!: string;
    @text({ max: 120 }) author!: string;
    @date() createdAt!: Date;
}
