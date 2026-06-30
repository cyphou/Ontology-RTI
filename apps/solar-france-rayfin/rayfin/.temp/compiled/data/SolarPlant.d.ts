/**
 * Ontology reference entity — mirrors the SolarFarm ontology `Site`
 * entity type. Seeded from the app's geospatial plant catalog so Fabric IQ
 * answers can be grounded in the registered ontology, not just live telemetry.
 */
export declare class SolarPlant {
    id: string;
    siteId: string;
    name: string;
    region: string;
    latitude: number;
    longitude: number;
    capacityMw: number;
}
