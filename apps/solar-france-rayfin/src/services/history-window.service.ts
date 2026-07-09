//-----------------------------------------------------------------------
// <copyright company="Microsoft Corporation">
//        Copyright (c) Microsoft Corporation.  All rights reserved.
//        Licensed under the MIT license. See LICENSE file in the project root for full license information.
// </copyright>
//-----------------------------------------------------------------------

export type HistoryWindow = "1h" | "6h" | "24h";

export const HISTORY_WINDOWS: HistoryWindow[] = ["1h", "6h", "24h"];

export function historyPointLimit(window: HistoryWindow): number {
    switch (window) {
        case "1h":
            return 24;
        case "6h":
            return 72;
        case "24h":
            return 120;
    }
}
