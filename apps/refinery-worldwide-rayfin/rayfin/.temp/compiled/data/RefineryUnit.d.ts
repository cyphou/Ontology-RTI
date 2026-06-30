/**
 * Ontology reference entity — mirrors the Refinery ontology `Site`
 * entity type. Seeded from the app's geospatial refinery catalog so Fabric IQ
 * answers can be grounded in the registered ontology, not just live telemetry.
 */
export declare class RefineryUnit {
    id: string;
    siteId: string;
    name: string;
    region: string;
    latitude: number;
    longitude: number;
    capacityMw: number;
}
