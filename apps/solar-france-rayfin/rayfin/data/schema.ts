import { SolarPlant } from "./SolarPlant.js";
import { DispatchNote } from "./DispatchNote.js";
import { SimulationRun } from "./SimulationRun.js";
import { SimulationApproval } from "./SimulationApproval.js";

/** Type map consumed by RayfinClient for typed GraphQL proxies. */
export type DataAppSchema = {
    SolarPlant: SolarPlant;
    DispatchNote: DispatchNote;
    SimulationRun: SimulationRun;
    SimulationApproval: SimulationApproval;
};

/** Runtime entity registry applied to the database by `rayfin up`. */
export const schema = [SolarPlant, DispatchNote, SimulationRun, SimulationApproval];
