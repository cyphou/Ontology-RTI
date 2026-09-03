import { RefineryUnit } from "./RefineryUnit.js";
import { DispatchNote } from "./DispatchNote.js";
import { SensorThreshold } from "./SensorThreshold.js";
import { SimulationRun } from "./SimulationRun.js";
import { SimulationApproval } from "./SimulationApproval.js";

/** Type map consumed by RayfinClient for typed GraphQL proxies. */
export type DataAppSchema = {
    RefineryUnit: RefineryUnit;
    DispatchNote: DispatchNote;
    SensorThreshold: SensorThreshold;
    SimulationRun: SimulationRun;
    SimulationApproval: SimulationApproval;
};

/** Runtime entity registry applied to the database by `rayfin up`. */
export const schema = [RefineryUnit, DispatchNote, SensorThreshold, SimulationRun, SimulationApproval];
