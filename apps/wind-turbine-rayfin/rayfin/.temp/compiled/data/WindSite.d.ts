/**
 * Ontology reference entity — mirrors the WindTurbine ontology `Site`
 * entity type. Seeded from the app's geospatial site catalog so Fabric IQ
 * answers can be grounded in the registered ontology, not just live telemetry.
 */
export declare class WindSite {
    id: string;
    siteId: string;
    name: string;
    country: string;
    latitude: number;
    longitude: number;
    capacityMw: number;
}
