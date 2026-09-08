import { authenticated, date, entity, text, uuid } from "@microsoft/rayfin-core";

/** Human review record for an aggregate scenario; it is not an HR decision record. */
@entity()
@authenticated("*")
export class PlanningReview {
    @uuid() id!: string;
    @text({ max: 96 }) scenarioId!: string;
    @text({ max: 24 }) decision!: string;
    @text({ max: 1000 }) rationale!: string;
    @text({ max: 120 }) reviewedBy!: string;
    @date() reviewedAt!: Date;
}