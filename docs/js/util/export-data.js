/**
 * Data and Chart Export Utilities with Complete Provenance.
 *
 * Emits self-describing CSV and standalone SVG formats that preserve
 * observatory provenance, cycle precedence rules, and coverage caveats.
 *
 * Vanilla JS — zero TypeScript in executable code.
 */

/**
 * Format time series records into self-describing CSV with provenance metadata headers.
 *
 * @param {string} title
 * @param {Array<Object>} rows
 * @param {Object} [meta={}]
 * @param {string} [meta.terminal]
 * @param {string} [meta.coverageNote]
 * @param {string} [meta.cycleRule]
 * @param {string} [meta.conversionFormula]
 * @returns {string} CSV text
 */
export function formatCsvWithProvenance(title, rows, meta = {}) {
  const lines = [];
  lines.push(`# Blue Tide Natural Gas Observatory -- Data Export`);
  lines.push(`# Series: ${title}`);
  lines.push(`# Exported: ${new Date().toISOString()}`);
  if (meta.terminal) lines.push(`# Terminal: ${meta.terminal}`);
  if (meta.coverageNote) lines.push(`# Coverage Caveat: ${meta.coverageNote}`);
  lines.push(`# Conversion Formula: ${meta.conversionFormula || 'MMcf/d = Dth/d / 1.025 / 1000.0'}`);
  lines.push(`# Cycle Precedence: ${meta.cycleRule || 'NAESB timely < evening < late < latec < id1 < id2 < id3 (hourly idHH00 excluded)'}`);
  lines.push(`# Source URL: https://yieldchaser.github.io/Supply-Demand-Flows/`);
  lines.push('#');

  if (!rows || rows.length === 0) {
    lines.push('date,value_mmcf_d');
    return lines.join('\n');
  }

  // Determine headers from keys of first row
  const headers = Object.keys(rows[0]);
  lines.push(headers.join(','));

  rows.forEach((r) => {
    const rowValues = headers.map((h) => {
      const val = r[h];
      if (val === null || val === undefined) return '';
      if (typeof val === 'string' && val.includes(',')) return `"${val}"`;
      if (typeof val === 'number') return Number.isInteger(val) ? String(val) : val.toFixed(2);
      return String(val);
    });
    lines.push(rowValues.join(','));
  });

  return lines.join('\n');
}

/**
 * Serialize an SVG element to a standalone XML string.
 *
 * @param {SVGElement|string} svgSource
 * @returns {string} XML SVG string
 */
export function serializeSvgToString(svgSource) {
  let xml = '';
  if (typeof svgSource === 'string') {
    xml = svgSource;
  } else if (typeof XMLSerializer !== 'undefined' && svgSource) {
    const serializer = new XMLSerializer();
    xml = serializer.serializeToString(svgSource);
  } else {
    xml = String(svgSource || '');
  }

  if (!xml.startsWith('<?xml')) {
    xml = '<?xml version="1.0" standalone="no"?>\r\n' + xml;
  }
  return xml;
}
