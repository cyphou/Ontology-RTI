import { SolarPlant } from "./SolarPlant.js";
import { DispatchNote } from "./DispatchNote.js";

/** Type map consumed by RayfinClient for typed GraphQL proxies. */
export type DataAppSchema = {
    SolarPlant: SolarPlant;
    DispatchNote: DispatchNote;
};

/** Runtime entity registry applied to the database by `rayfin up`. */
export const schema = [SolarPlant, DispatchNote];
