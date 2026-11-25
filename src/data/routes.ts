// Copyright 2026 Tauric Research
// SPDX-License-Identifier: Apache-2.0
/**
 * The one place that says which providers serve each capability, in order: AkShare handles A-shares
 * and China macro series (passing non-Chinese symbols down the chain), Tauric serves US/global data,
 * Yahoo serves quotes and daily price history, and Polymarket serves prediction-market odds.
 * A provider that is not configured, or does not implement the method asked, is skipped.
 */
import type { Capability } from "./capabilities.js";

export type ProviderName = "tauric" | "yahoo" | "polymarket" | "akshare";

export const ROUTES: { readonly [Name in Capability]: readonly ProviderName[] } = {
  quotes: ["akshare", "tauric", "yahoo"],
  priceHistory: ["akshare", "tauric", "yahoo"],
  statements: ["akshare", "tauric"],
  filings: ["tauric"],
  news: ["akshare", "tauric"],
  profiles: ["akshare", "tauric"],
  ownership: ["tauric"],
  corporateActions: ["akshare", "tauric"],
  technicals: ["tauric"],
  macroSeries: ["akshare", "tauric"],
  instruments: ["akshare", "tauric"],
  syntheses: ["tauric"],
  crossSections: ["tauric"],
  markets: ["tauric"],
  predictionMarkets: ["polymarket"],
};
