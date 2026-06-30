import { entity, authenticated, uuid, text, decimal, boolean } from "@microsoft/rayfin-core";

/**
 * Ontology-grounded signal threshold band — mirrors the WindTurbine ontology
 * `Sensor` metadata (Unit, MinThreshold/MaxThreshold) for each measured signal.
 * Seeded from the app's single source of truth (`SIGNAL_METADATA`) so the warn /
 * alarm bands that drive turbine status are published to, and auditable from,
 * the Fabric ontology backend rather than living only in the client bundle.
 *
 * `warn` / `alarm` use a `-1` sentinel to represent "no band" (an informational
 * signal such as raw power output) because JSON cannot round-trip `Infinity`.
 */
@entity()
@authenticated("*")
export class SensorThreshold {
    @uuid() id!: string;
    @text({ max: 16, unique: true }) signalKey!: string;
    @text({ max: 48 }) ontologyProperty!: string;
    @text({ max: 16 }) unit!: string;
    @decimal() warn!: number;
    @decimal() alarm!: number;
    @boolean() governsHealth!: boolean;
}
