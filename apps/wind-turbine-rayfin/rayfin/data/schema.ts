import { WindSite } from "./WindSite.js";
import { DispatchNote } from "./DispatchNote.js";
import { SensorThreshold } from "./SensorThreshold.js";
import { MaintenanceOrder } from "./MaintenanceOrder.js";
import { TurbineDevice } from "./TurbineDevice.js";

/** Type map consumed by RayfinClient for typed GraphQL proxies. */
export type DataAppSchema = {
    WindSite: WindSite;
    DispatchNote: DispatchNote;
    SensorThreshold: SensorThreshold;
    MaintenanceOrder: MaintenanceOrder;
    TurbineDevice: TurbineDevice;
};

/** Runtime entity registry applied to the database by `rayfin up`. */
export const schema = [WindSite, DispatchNote, SensorThreshold, MaintenanceOrder, TurbineDevice];
