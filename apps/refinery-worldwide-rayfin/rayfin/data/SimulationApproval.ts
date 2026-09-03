import { entity, authenticated, uuid, text, date } from "@microsoft/rayfin-core";

/** Append-only approval decision attached to a submitted simulation run. */
@entity()
@authenticated("*")
export class SimulationApproval {
    @uuid() id!: string;
    @text({ max: 64 }) runId!: string;
    @text({ max: 24 }) decision!: string;
    @text({ max: 1000 }) reason!: string;
    @text({ max: 120 }) decidedBy!: string;
    @date() decidedAt!: Date;
}
