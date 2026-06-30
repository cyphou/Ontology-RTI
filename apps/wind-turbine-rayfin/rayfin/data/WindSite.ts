import { entity, authenticated, uuid, text, decimal } from "@microsoft/rayfin-core";

/**
 * Ontology reference entity — mirrors the WindTurbine ontology `Site`
 * entity type. Seeded from the app's geospatial site catalog so Fabric IQ
 * answers can be grounded in the registered ontology, not just live telemetry.
 */
@entity()
@authenticated("*")
export class WindSite {
    @uuid() id!: string;
    @text({ max: 32, unique: true }) siteId!: string;
    @text({ max: 120 }) name!: string;
    @text({ max: 8 }) country!: string;
    @decimal() latitude!: number;
    @decimal() longitude!: number;
    @decimal() capacityMw!: number;
}
