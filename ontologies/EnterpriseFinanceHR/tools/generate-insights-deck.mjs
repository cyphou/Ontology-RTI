// Generates a shiny "Enterprise Finance + HR" insights deck from artifacts/cowork-insights.json.
// Usage: node generate-insights-deck.mjs [insightsJsonPath] [outPptxPath]
import pptxgen from 'pptxgenjs';
import fs from 'node:fs';
import path from 'node:path';
import { fileURLToPath } from 'node:url';

const __dirname = path.dirname(fileURLToPath(import.meta.url));
const repoRoot = path.resolve(__dirname, '..', '..', '..');
const insightsPath = process.argv[2] || path.join(repoRoot, 'artifacts', 'cowork-insights.json');
const outPath = process.argv[3] || path.join(repoRoot, 'artifacts', 'EnterpriseFinanceHR-Insights.pptx');

const insights = JSON.parse(fs.readFileSync(insightsPath, 'utf8'));

// Brand palette (matches the EnterpriseFinanceHRTheme Power BI theme)
const INK = '0F172A';
const INDIGO = '4F46E5';
const CYAN = '06B6D4';
const AMBER = 'F59E0B';
const SLATE = '64748B';
const CARD = 'FFFFFF';
const BG = 'F4F6FB';

const pres = new pptxgen();
pres.layout = 'LAYOUT_WIDE';
pres.author = 'Enterprise Finance + HR Cowork Pipeline';
pres.title = 'Enterprise Finance + HR - AI Insights Briefing';

const W = 13.3, H = 7.5;

function shadow() {
  return { type: 'outer', color: '1E293B', blur: 12, offset: 3, angle: 135, opacity: 0.18 };
}

// ---------- Slide 1: Title ----------
{
  const s = pres.addSlide();
  s.background = { color: INK };
  s.addShape(pres.shapes.RECTANGLE, { x: 0, y: 0, w: 4.6, h: H, fill: { color: INDIGO } });
  s.addShape(pres.shapes.OVAL, { x: 3.6, y: -1.2, w: 3, h: 3, fill: { color: CYAN, transparency: 70 }, line: { type: 'none' } });
  s.addShape(pres.shapes.OVAL, { x: -0.8, y: 5.2, w: 3.5, h: 3.5, fill: { color: AMBER, transparency: 75 }, line: { type: 'none' } });
  s.addText('EF+HR', { x: 0.6, y: 0.5, w: 3.4, h: 0.6, fontSize: 16, bold: true, color: 'FFFFFF', charSpacing: 4 });
  s.addText('AI-Powered\nInsights Briefing', { x: 0.6, y: 2.3, w: 3.6, h: 2.4, fontSize: 30, bold: true, color: 'FFFFFF', fontFace: 'Georgia' });
  s.addText('Enterprise Finance + HR', { x: 0.6, y: H - 1.0, w: 3.6, h: 0.5, fontSize: 13, color: 'CADCFC' });

  s.addText('Ask. Forecast. Decide.', { x: 5.1, y: 1.0, w: 7.6, h: 0.6, fontSize: 15, italic: true, color: CYAN });
  s.addText('Six business questions answered directly from live model data, plus a linear-trend spend forecast, generated end-to-end for this review.', {
    x: 5.1, y: 1.7, w: 7.2, h: 1.2, fontSize: 15, color: 'E2E8F0'
  });

  const genDate = new Date(insights.generatedAt);
  const stats = [
    { label: 'Questions answered', value: String(Object.keys(insights.questions).length) },
    { label: 'Periods forecast', value: String(insights.forecast.forecastPeriods) },
    { label: 'Generated', value: genDate.toLocaleDateString('en-US', { month: 'short', day: 'numeric', year: 'numeric' }) }
  ];
  stats.forEach((st, i) => {
    const x = 5.1 + i * 2.55;
    s.addShape(pres.shapes.ROUNDED_RECTANGLE, { x, y: 3.3, w: 2.3, h: 1.5, rectRadius: 0.12, fill: { color: '1E2761' }, line: { color: '334155', width: 1 } });
    s.addText(st.value, { x, y: 3.5, w: 2.3, h: 0.8, align: 'center', fontSize: 26, bold: true, color: CYAN, fontFace: 'Segoe UI Semibold' });
    s.addText(st.label, { x, y: 4.25, w: 2.3, h: 0.5, align: 'center', fontSize: 11, color: '94A3B8' });
  });

  s.addText('Enterprise Finance + HR Ontology  |  Microsoft Fabric  |  Power BI Direct Lake', { x: 5.1, y: H - 0.7, w: 7.6, h: 0.4, fontSize: 10, color: '64748B' });
}

// ---------- Slide 2: Q&A grid (business questions answered from data) ----------
{
  const s = pres.addSlide();
  s.background = { color: BG };
  s.addShape(pres.shapes.RECTANGLE, { x: 0, y: 0, w: W, h: 1.1, fill: { color: INK } });
  s.addText('Ask the Data: 5 Questions, Answered', { x: 0.6, y: 0.18, w: 10, h: 0.7, fontSize: 26, bold: true, color: 'FFFFFF', fontFace: 'Georgia' });
  s.addText('Natural-language questions resolved directly against the live semantic model', { x: 0.6, y: 0.7, w: 10, h: 0.35, fontSize: 12, color: CYAN });

  const entries = Object.entries(insights.questions).filter(([q]) => !q.toLowerCase().includes('trending over the next quarter'));
  const labels = ['$', 'H', 'R', 'C', 'A'];
  const cols = 1, rows = entries.length;
  const cardH = 1.02, gap = 0.12, startY = 1.35;
  entries.forEach(([q, a], i) => {
    const y = startY + i * (cardH + gap);
    s.addShape(pres.shapes.ROUNDED_RECTANGLE, { x: 0.6, y, w: W - 1.2, h: cardH, rectRadius: 0.08, fill: { color: CARD }, shadow: shadow(), line: { color: 'E9EDF5', width: 1 } });
    s.addShape(pres.shapes.OVAL, { x: 0.85, y: y + (cardH - 0.55) / 2, w: 0.55, h: 0.55, fill: { color: INDIGO } });
    s.addText(labels[i] || '?', { x: 0.85, y: y + (cardH - 0.55) / 2, w: 0.55, h: 0.55, align: 'center', valign: 'middle', fontSize: 20, bold: true, color: 'FFFFFF', fontFace: 'Segoe UI Semibold' });
    s.addText(q, { x: 1.65, y: y + 0.08, w: W - 2.5, h: 0.4, fontSize: 14, bold: true, color: INK, fontFace: 'Segoe UI Semibold' });
    s.addText(a, { x: 1.65, y: y + 0.48, w: W - 2.5, h: 0.48, fontSize: 12, color: SLATE });
  });
}

// ---------- Slide 3: Forecast ----------
{
  const s = pres.addSlide();
  s.background = { color: BG };
  s.addShape(pres.shapes.RECTANGLE, { x: 0, y: 0, w: W, h: 1.1, fill: { color: INK } });
  s.addText('Spend Forecast: Next ' + insights.forecast.forecastPeriods + ' Periods', { x: 0.6, y: 0.18, w: 10, h: 0.7, fontSize: 26, bold: true, color: 'FFFFFF', fontFace: 'Georgia' });
  s.addText('Linear-trend regression on Actual spend, computed from ' + insights.forecast.actualHistory.length + ' historical fiscal periods', { x: 0.6, y: 0.7, w: 11, h: 0.35, fontSize: 12, color: CYAN });

  const hist = insights.forecast.actualHistory;
  const fc = insights.forecast.actualForecast;
  const allVals = [...hist, ...fc];
  const maxV = Math.max(...allVals), minV = Math.min(...allVals);
  const chartX = 0.7, chartY = 1.6, chartW = W - 1.4, chartH = 4.4;
  const n = allVals.length;
  const barW = (chartW / n) * 0.62;
  const gapW = (chartW / n) * 0.38;

  // axis baseline
  s.addShape(pres.shapes.LINE, { x: chartX, y: chartY + chartH, w: chartW, h: 0, line: { color: 'CBD5E1', width: 1 } });

  allVals.forEach((v, i) => {
    const isForecast = i >= hist.length;
    const barH = ((v - minV) / (maxV - minV)) * (chartH - 0.4) + 0.15;
    const x = chartX + i * (barW + gapW);
    const y = chartY + chartH - barH;
    s.addShape(pres.shapes.ROUNDED_RECTANGLE, {
      x, y, w: barW, h: barH, rectRadius: 0.04,
      fill: { color: isForecast ? AMBER : INDIGO, transparency: isForecast ? 10 : 0 },
      line: isForecast ? { color: AMBER, width: 1.5, dashType: 'dash' } : { type: 'none' }
    });
    if (i === hist.length - 1 || i === n - 1 || i % 4 === 0) {
      s.addText('$' + Math.round(v / 1000).toLocaleString('en-US') + 'K', { x: x - 0.25, y: y - 0.32, w: barW + 0.5, h: 0.3, align: 'center', fontSize: 9, bold: i >= hist.length, color: isForecast ? 'B45309' : INK });
    }
  });

  // legend
  s.addShape(pres.shapes.RECTANGLE, { x: chartX, y: chartY + chartH + 0.35, w: 0.3, h: 0.2, fill: { color: INDIGO } });
  s.addText('Actual (historical)', { x: chartX + 0.4, y: chartY + chartH + 0.25, w: 2.2, h: 0.4, fontSize: 11, color: INK });
  s.addShape(pres.shapes.RECTANGLE, { x: chartX + 2.8, y: chartY + chartH + 0.35, w: 0.3, h: 0.2, fill: { color: AMBER } });
  s.addText('Forecast (linear trend)', { x: chartX + 3.2, y: chartY + chartH + 0.25, w: 2.6, h: 0.4, fontSize: 11, color: INK });

  s.addText(insights.questions['Where is spend trending over the next quarter?'] || '', {
    x: chartX + 6.2, y: chartY + chartH + 0.15, w: chartW - 6.2, h: 0.6, fontSize: 11, italic: true, color: SLATE, align: 'right'
  });
}

// ---------- Slide 4: Next steps / closing ----------
{
  const s = pres.addSlide();
  s.background = { color: INK };
  s.addShape(pres.shapes.OVAL, { x: 9.5, y: -1.5, w: 5, h: 5, fill: { color: INDIGO, transparency: 65 }, line: { type: 'none' } });
  s.addText('Recommended Next Steps', { x: 0.6, y: 0.7, w: 9, h: 0.8, fontSize: 30, bold: true, color: 'FFFFFF', fontFace: 'Georgia' });

  const steps = [
    { t: 'Validate the forecast', d: 'Review the ' + insights.forecast.forecastPeriods + '-period spend trend with Finance before the next budget cycle.' },
    { t: 'Close open requisitions', d: 'Recruitment has open requisitions with a 50% offer-acceptance rate, prioritize follow-up.' },
    { t: 'Monitor overtime & absence', d: 'Attendance metrics are within range; keep tracking overtime rate month over month.' },
    { t: 'Schedule the review', d: 'Use the linked Teams meeting to walk stakeholders through this briefing live.' }
  ];
  steps.forEach((st, i) => {
    const y = 1.8 + i * 1.15;
    s.addShape(pres.shapes.OVAL, { x: 0.7, y, w: 0.5, h: 0.5, fill: { color: CYAN } });
    s.addText(String(i + 1), { x: 0.7, y, w: 0.5, h: 0.5, align: 'center', valign: 'middle', fontSize: 16, bold: true, color: INK });
    s.addText(st.t, { x: 1.5, y: y - 0.05, w: 9.5, h: 0.4, fontSize: 16, bold: true, color: 'FFFFFF' });
    s.addText(st.d, { x: 1.5, y: y + 0.35, w: 9.5, h: 0.5, fontSize: 12, color: 'CBD5E1' });
  });

  s.addText('Generated automatically by the Enterprise Finance + HR Cowork pipeline', { x: 0.6, y: H - 0.6, w: 10, h: 0.4, fontSize: 10, color: '64748B' });
}

fs.mkdirSync(path.dirname(outPath), { recursive: true });
await pres.writeFile({ fileName: outPath });
console.log('Deck written to: ' + outPath);
