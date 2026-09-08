import { FabricClient, type FabricClientConfig } from "@microsoft/fabric-app-data";
import { SemanticModelMessageClient } from "@microsoft/fabric-app-data-embed-client";
import { EmbedFabricApiProxy } from "@microsoft/fabric-app-data-proxy";
import { fabricConfig } from "../fabric.generated";

let client: FabricClient | undefined;
let messageClient: SemanticModelMessageClient | undefined;

/** Uses Fabric host postMessage authentication; no browser token is stored by this app. */
export function getFabricClient(): FabricClient {
    messageClient ??= new SemanticModelMessageClient();
    client ??= new FabricClient({
        proxy: new EmbedFabricApiProxy(messageClient),
        ...fabricConfig,
    } as FabricClientConfig);
    return client;
}