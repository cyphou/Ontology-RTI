import { WindSite } from "./WindSite.js";
import { DispatchNote } from "./DispatchNote.js";
import { SensorThreshold } from "./SensorThreshold.js";
import { MaintenanceOrder } from "./MaintenanceOrder.js";
import { TurbineDevice } from "./TurbineDevice.js";
/** Runtime entity registry applied to the database by `rayfin up`. */
export const schema = [WindSite, DispatchNote, SensorThreshold, MaintenanceOrder, TurbineDevice];
