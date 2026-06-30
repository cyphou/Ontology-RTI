/**
 * Writeback target — an operator dispatch note attached to an ontology
 * turbine. This is the durable store behind the "Writeback This Turbine"
 * action. Records are keyed to ontology entity ids (turbineId / siteId).
 */
export declare class DispatchNote {
    id: string;
    turbineId: string;
    siteId: string;
    status: string;
    powerKw: number;
    note: string;
    author: string;
    createdAt: Date;
}
