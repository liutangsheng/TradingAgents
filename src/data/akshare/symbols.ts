// Copyright 2026 Tauric Research
// SPDX-License-Identifier: Apache-2.0
/**
 * A-share and China macro symbol recognition so the AkShare provider answers only Chinese instruments
 * and yields US/global instruments cleanly to downstream providers.
 */
import { parseInstrument } from "../instrument-key.js";

const A_SHARE_SYMBOL =
  /^((XSHG|XSHE|XBSE):[0-9]{6}|[0-9]{6}\.(SH|SS|SZ|BJ)|(SH|SZ|BJ)[0-9]{6}|(00|20|30|43|60|68|83|87|90|92)[0-9]{4})$/;

const A_SHARE_INDICES = new Set([
  "000001.SH",
  "000300.SH",
  "000905.SH",
  "000852.SH",
  "000688.SH",
  "000016.SH",
  "399001.SZ",
  "399006.SZ",
  "899050.BJ",
]);

const CHINA_QUERY = /[\u4e00-\u9fff]|^(sh|sz|bj)?[0-9]{6}(\.(sh|ss|sz|bj))?$|^x(shg|she|bse):/i;
const CHINA_INDEX_QUERY = /\b(csi\s*300|csi\s*500|csi\s*1000|sse|szse|chinext|star\s*50|a-share)\b/i;
const CHINA_MACRO_QUERY = /(^cn\.|china|chinese|中国|国债|制造业\s*pmi|非制造业)/i;

/** Whether `key` names an A-share stock or index supported by AkShare. */
export function isAShareSymbol(key: string): boolean {
  const { kind, id } = parseInstrument(key);
  if (kind !== "security") return false;
  return A_SHARE_SYMBOL.test(id);
}

/** Normalize an A-share key to `<6-digits>.<SH|SZ|BJ>` when recognizable; otherwise undefined. */
export function canonicalAShareSymbol(key: string): string | undefined {
  const { kind, id } = parseInstrument(key);
  if (kind !== "security") return undefined;
  const mic = /^(XSHG|XSHE|XBSE):([0-9]{6})$/.exec(id);
  if (mic) {
    const suffix = { XSHG: "SH", XSHE: "SZ", XBSE: "BJ" }[mic[1] as string];
    return `${mic[2]}.${suffix}`;
  }
  const suffix = /^([0-9]{6})\.(SH|SS|SZ|BJ)$/.exec(id);
  if (suffix) return `${suffix[1]}.${suffix[2] === "SS" ? "SH" : suffix[2]}`;
  const prefix = /^(SH|SZ|BJ)([0-9]{6})$/.exec(id);
  if (prefix) return `${prefix[2]}.${prefix[1]}`;
  if (/^[0-9]{6}$/.test(id)) {
    if (/^(60|68|90)/.test(id)) return `${id}.SH`;
    if (/^(00|20|30)/.test(id)) return `${id}.SZ`;
    if (/^(43|83|87|92)/.test(id)) return `${id}.BJ`;
  }
  return undefined;
}

export function isAShareIndex(key: string): boolean {
  const canonical = canonicalAShareSymbol(key);
  return canonical !== undefined && A_SHARE_INDICES.has(canonical);
}

export function isChinaInstrumentQuery(query: string, kind: string | undefined): boolean {
  if (kind && kind !== "security" && kind !== "series") return false;
  const trimmed = query.trim();
  if (kind === "series") return CHINA_MACRO_QUERY.test(trimmed);
  return CHINA_QUERY.test(trimmed) || CHINA_INDEX_QUERY.test(trimmed) || CHINA_MACRO_QUERY.test(trimmed);
}

export function isChinaMacroQuery(query: string): boolean {
  return CHINA_MACRO_QUERY.test(query.trim());
}

export function isChinaMacroKey(key: string): boolean {
  return key.replace(/^series:/i, "").toLowerCase().startsWith("cn.");
}
