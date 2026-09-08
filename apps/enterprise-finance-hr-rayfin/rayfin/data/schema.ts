import { PlanningReview } from "./PlanningReview.js";
import { PlanningScenario } from "./PlanningScenario.js";

export type DataAppSchema = {
    PlanningScenario: PlanningScenario;
    PlanningReview: PlanningReview;
};

export const schema = [PlanningScenario, PlanningReview];