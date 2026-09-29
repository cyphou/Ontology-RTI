# Enterprise Finance + HR Rayfin App

Production-oriented Rayfin dashboard for aggregate finance and workforce planning. It includes Enterprise Overview, Finance, Workforce, Planning, Exceptions, Graph, and Ask Fabric IQ views. Finance, compensation, attendance, absence, and recruitment are represented only through grouped measures; the app never renders individual salary, absence, attendance, or candidate records. All data is synthetic and contains no PII. The app must not support automated HR decisions.

The intended Fabric workspace is `<workspace-id>`. Its Lakehouse is documented for deployment coordination, not stored in browser configuration. Configure an approved semantic-model connection alias in `.env.local` only after the Enterprise Finance + HR semantic model is deployed; the app uses synthetic aggregate planning data until then.

The semantic model query uses its actual `Budget`, `Actual`, `Forecast`, `Headcount`, `FTE`, `Variance`, and `Variance %` measures. Local synthetic fallbacks demonstrate total compensation, payroll cost per FTE, scheduled/worked/approved/overtime rates, absence rates, requisitions, openings, candidate pipeline, offers, acceptance, and hires without revealing detail rows. The optional Data Agent endpoint must be an authenticated gateway or Fabric-hosted service. RTI is optional. No token, secret, employee data, or personal identifier belongs in a `VITE_` variable.

## Planning demo

The dashboard retains Enterprise Overview, Finance, Workforce, Planning, Exceptions, Graph, and Ask Fabric IQ views. Compensation is presented in Finance; Time & Attendance (GTA) is presented in Workforce; and Absence, Recruitment, and Data Quality are presented together in Exceptions. Each view uses grouped measures only, with small absence groups suppressed.

The **Planning** view provides a guided, aggregate-only demonstration: establish the baseline, compare a synthetic budget/capacity assumption against the forecast, then submit it for a recorded human review. It is the primary showcase flow for this domain. The Graph view is a semantic relationship map of the governed planning context, not a people or compensation graph. The RTI dashboard and Eventhouse remain optional operational extensions for freshness and exception monitoring; they are not required for the Finance + HR planning story.

Run `npm install`, then `npm run dev`, `npm test -- --run`, or `npm run build`.