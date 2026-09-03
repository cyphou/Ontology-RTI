import * as THREE from "three";
import countries110m from "world-atlas/countries-110m.json";
import { feature } from "topojson-client";

const MAP_WIDTH = 2048;
const MAP_HEIGHT = 1024;

type Position = [number, number];
type Ring = Position[];
type Polygon = Ring[];
type Geometry = { type: string; coordinates?: Position[] | Ring[] | Polygon[] | Polygon[][] };
type Feature = { geometry?: Geometry | null };

function toPixel([longitude, latitude]: Position): [number, number] {
    return [((longitude + 180) / 360) * MAP_WIDTH, ((90 - latitude) / 180) * MAP_HEIGHT];
}

function drawRing(ctx: CanvasRenderingContext2D, ring: Ring): void {
    if (ring.length === 0) {
        return;
    }
    const [startX, startY] = toPixel(ring[0]);
    ctx.moveTo(startX, startY);
    for (let index = 1; index < ring.length; index += 1) {
        const [x, y] = toPixel(ring[index]);
        ctx.lineTo(x, y);
    }
    ctx.closePath();
}

function drawGeometry(ctx: CanvasRenderingContext2D, geometry: Geometry): void {
    ctx.beginPath();
    if (geometry.type === "Polygon") {
        (geometry.coordinates as Polygon).forEach((ring) => drawRing(ctx, ring));
    } else if (geometry.type === "MultiPolygon") {
        (geometry.coordinates as Polygon[][]).forEach((polygon) => polygon.forEach((ring) => drawRing(ctx, ring)));
    }
}

// Real Natural Earth 1:110m country geometry, packaged locally by world-atlas.
// It is rasterized to an equirectangular canvas that maps exactly onto the Three.js
// globe, avoiding online-tile CORS/token dependencies while keeping real coastlines.
export function createWorldMapTexture(): THREE.CanvasTexture {
    const canvas = document.createElement("canvas");
    canvas.width = MAP_WIDTH;
    canvas.height = MAP_HEIGHT;
    const ctx = canvas.getContext("2d");
    const texture = new THREE.CanvasTexture(canvas);
    texture.colorSpace = THREE.SRGBColorSpace;
    texture.anisotropy = 4;
    if (!ctx) {
        return texture;
    }

    const ocean = ctx.createLinearGradient(0, 0, 0, MAP_HEIGHT);
    ocean.addColorStop(0, "#071b31");
    ocean.addColorStop(0.48, "#0a3857");
    ocean.addColorStop(1, "#051727");
    ctx.fillStyle = ocean;
    ctx.fillRect(0, 0, MAP_WIDTH, MAP_HEIGHT);

    // Geographic graticule provides orientation without distracting from sites.
    ctx.strokeStyle = "rgba(155, 215, 242, 0.13)";
    ctx.lineWidth = 1;
    for (let longitude = -150; longitude <= 150; longitude += 30) {
        const [x] = toPixel([longitude, 0]);
        ctx.beginPath(); ctx.moveTo(x, 0); ctx.lineTo(x, MAP_HEIGHT); ctx.stroke();
    }
    for (let latitude = -60; latitude <= 60; latitude += 30) {
        const [, y] = toPixel([0, latitude]);
        ctx.beginPath(); ctx.moveTo(0, y); ctx.lineTo(MAP_WIDTH, y); ctx.stroke();
    }

    const topology = countries110m as unknown as { objects: { countries: unknown } };
    const collection = feature(topology as never, topology.objects.countries as never) as unknown as { features: Feature[] };
    for (const country of collection.features) {
        if (!country.geometry) {
            continue;
        }
        drawGeometry(ctx, country.geometry);
        const land = ctx.createLinearGradient(0, 0, 0, MAP_HEIGHT);
        land.addColorStop(0, "#76a67a");
        land.addColorStop(0.55, "#477e61");
        land.addColorStop(1, "#305841");
        ctx.fillStyle = land;
        ctx.fill("evenodd");
        ctx.strokeStyle = "rgba(207, 246, 217, 0.62)";
        ctx.lineWidth = 0.8;
        ctx.stroke();
    }

    // A light coastal halo makes the land legible against the dark operational globe.
    ctx.globalCompositeOperation = "destination-over";
    ctx.shadowColor = "rgba(83, 218, 187, 0.45)";
    ctx.shadowBlur = 10;
    ctx.strokeStyle = "rgba(83, 218, 187, 0.2)";
    ctx.lineWidth = 3;
    for (const country of collection.features) {
        if (country.geometry) {
            drawGeometry(ctx, country.geometry);
            ctx.stroke();
        }
    }
    ctx.globalCompositeOperation = "source-over";
    ctx.shadowBlur = 0;
    texture.needsUpdate = true;
    return texture;
}
