import { SolarPlant } from "./SolarPlant.js";
import { DispatchNote } from "./DispatchNote.js";
/** Runtime entity registry applied to the database by `rayfin up`. */
export const schema = [SolarPlant, DispatchNote];
