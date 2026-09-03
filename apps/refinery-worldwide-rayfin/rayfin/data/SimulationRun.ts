import { entity, authenticated, uuid, text, int, date } from "@microsoft/rayfin-core";

/** Versioned simulation submission: baseline provenance, inputs, and decision package. */
@entity()
@authenticated("*")
export class SimulationRun {
    @uuid() id!: string;
    @text({ max: 64, unique: true }) runId!: string;
    @text({ max: 32 }) refineryId!: string;
    @text({ max: 48 }) processUnitId!: string;
    @text({ max: 24 }) purpose!: string;
    @text({ max: 24 }) objective!: string;
    @int() horizon!: number;
    @text({ max: 12 }) timeUnit!: string;
    @text({ max: 24 }) status!: string;
    @text({ max: 16 }) baselineSource!: string;
    @date() baselineAt?: Date;
    @text({ max: 120 }) createdBy!: string;
    @date() createdAt!: Date;
    @text() decisionPackage!: string;
}
