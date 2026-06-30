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
export declare class SensorThreshold {
    id: string;
    signalKey: string;
    ontologyProperty: string;
    unit: string;
    warn: number;
    alarm: number;
    governsHealth: boolean;
}
