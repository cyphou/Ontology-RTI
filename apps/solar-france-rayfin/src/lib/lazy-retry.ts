//-----------------------------------------------------------------------
// <copyright company="Microsoft Corporation">
//        Copyright (c) Microsoft Corporation.  All rights reserved.
//        Licensed under the MIT license. See LICENSE file in the project root for full license information.
// </copyright>
//-----------------------------------------------------------------------

import { lazy, type ComponentType, type LazyExoticComponent } from "react";

// Wrap a dynamic import in a bounded retry. Embedded hosts (the Fabric portal iframe)
// can fail the first chunk fetch (transient network, cold CDN edge, or a briefly
// unresolved module URL); retrying a few times recovers the lazy 3D scene instead of
// tripping the scene error boundary on a one-off failure.
// eslint-disable-next-line @typescript-eslint/no-explicit-any -- mirrors React.lazy's own `ComponentType<any>` constraint
export function lazyRetry<T extends ComponentType<any>>(
    factory: () => Promise<{ default: T }>,
    retries = 3,
    delayMs = 400,
): LazyExoticComponent<T> {
    return lazy(async () => {
        let lastError: unknown;
        for (let attempt = 0; attempt <= retries; attempt += 1) {
            try {
                return await factory();
            } catch (error) {
                lastError = error;
                await new Promise((resolve) => setTimeout(resolve, delayMs * (attempt + 1)));
            }
        }
        throw lastError;
    });
}
