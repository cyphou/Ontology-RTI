import { WindSite } from "./WindSite.js";
import { DispatchNote } from "./DispatchNote.js";
/** Type map consumed by RayfinClient for typed GraphQL proxies. */
export type DataAppSchema = {
    WindSite: WindSite;
    DispatchNote: DispatchNote;
};
/** Runtime entity registry applied to the database by `rayfin up`. */
export declare const schema: (typeof DispatchNote | typeof WindSite)[];
