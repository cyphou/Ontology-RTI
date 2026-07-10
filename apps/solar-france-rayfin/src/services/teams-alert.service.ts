//-----------------------------------------------------------------------
// <copyright company="Microsoft Corporation">
//        Copyright (c) Microsoft Corporation.  All rights reserved.
//        Licensed under the MIT license. See LICENSE file in the project root for full license information.
// </copyright>
//-----------------------------------------------------------------------

// Proactive Teams alerting seam. When VITE_TEAMS_WEBHOOK_URL is set to an
// incoming-webhook / Workflows URL, a compact MessageCard is posted the first
// time an entity crosses into alarm (mirroring the auto-DispatchNote seam).
// Fully fallback-safe: unset or failing calls are silent no-ops.

export interface AlarmAlert {
    id: string;
    siteName: string;
    status: string;
    powerKw: number;
    detail: string;
}

/** True when a Teams incoming webhook has been wired up via env. */
export function isTeamsAlertConfigured(): boolean {
    return Boolean(import.meta.env.VITE_TEAMS_WEBHOOK_URL);
}

// Build a Teams-compatible MessageCard for an alarm onset. Pure so it can be
// unit-tested without a network call.
export function buildAlarmCard(alert: AlarmAlert): Record<string, unknown> {
    return {
        "@type": "MessageCard",
        "@context": "http://schema.org/extensions",
        themeColor: "EF476F",
        summary: `Alarm: ${alert.id}`,
        sections: [
            {
                activityTitle: `\u{1F6A8} Alarm \u2014 ${alert.id}`,
                activitySubtitle: alert.siteName,
                facts: [
                    { name: "Status", value: alert.status },
                    { name: "Output", value: alert.powerKw.toLocaleString() },
                    { name: "Detail", value: alert.detail },
                ],
                markdown: true,
            },
        ],
    };
}

// Post a proactive alarm alert to the configured Teams webhook. Returns false
// (no-op) when unconfigured or on any failure, so it never blocks auto-dispatch.
export async function postTeamsAlert(alert: AlarmAlert): Promise<boolean> {
    const url = import.meta.env.VITE_TEAMS_WEBHOOK_URL;
    if (!url) {
        return false;
    }
    try {
        const res = await fetch(url, {
            method: "POST",
            headers: { "Content-Type": "application/json" },
            body: JSON.stringify(buildAlarmCard(alert)),
        });
        return res.ok;
    } catch {
        return false;
    }
}
