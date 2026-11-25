// Copyright 2026 Tauric Research
// SPDX-License-Identifier: Apache-2.0
/**
 * AkShare as a data provider for Chinese A-shares and China macro series. Non-A-share instruments
 * raise `DataError("unsupported")` synchronously so the router passes them straight to Tauric/Yahoo.
 */
import { errorText } from "../../contracts/index.js";
import type {
  Call,
  Capabilities,
  DataProvider,
  InstrumentMatch,
  Quote,
  SeriesMeta,
  SeriesObservations,
  Statement,
} from "../capabilities.js";
import { DataError } from "../errors.js";
import { AkShareClient } from "./client.js";
import { isAShareSymbol, isChinaInstrumentQuery, isChinaMacroKey, isChinaMacroQuery } from "./symbols.js";

const NAME = "AkShare";

function requireAShare(symbol: string): void {
  if (!isAShareSymbol(symbol)) {
    throw new DataError("unsupported", `AkShare serves A-share symbols only, not ${symbol}`, {
      source: NAME,
    });
  }
}

export function akshareProvider(client: AkShareClient = new AkShareClient()): DataProvider {
  const singleQuote = async (symbol: string, call?: Call): Promise<Quote> => {
    requireAShare(symbol);
    return client.get<Quote>("/quote", { symbol }, call?.signal);
  };

  const dailyBars = async (symbol: string, window: { from: string; to: string }, call?: Call) => {
    requireAShare(symbol);
    return client.get<Awaited<ReturnType<Capabilities["priceHistory"]["daily"]>>["data"]>(
      "/history/daily",
      { symbol, from: window.from, to: window.to },
      call?.signal,
    );
  };

  return {
    name: NAME,
    capabilities: {
      quotes: {
        quote: singleQuote,
        many: async (keys, call?) => {
          const aShares = keys.filter(isAShareSymbol);
          if (!aShares.length) {
            throw new DataError("unsupported", "AkShare serves A-share symbols only.", { source: NAME });
          }
          const nonAShares = keys.filter((key) => !isAShareSymbol(key));
          const settled = await client.get<{
            quotes: { instrument: string; quote: Quote }[];
            errors: { instrument: string; message: string; kind?: DataError["kind"] }[];
          }>("/quotes", { symbols: aShares.join(",") }, call?.signal);
          const errors = [
            ...settled.errors,
            ...nonAShares.map((instrument) => ({
              instrument,
              message: `AkShare serves A-share symbols only, not ${instrument}`,
              kind: "unsupported" as const,
            })),
          ];
          if (!settled.quotes.length && nonAShares.length === 0 && errors[0]) {
            throw new DataError(errors[0].kind ?? "unavailable", errors[0].message, { source: NAME });
          }
          return { quotes: settled.quotes, errors };
        },
      },
      priceHistory: {
        daily: dailyBars,
        dailyPair: async ([first, second], window, call?) => [
          await dailyBars(first, window, call),
          await dailyBars(second, window, call),
        ],
        intraday: async (symbol, { interval, start, end, limit }, call?) => {
          requireAShare(symbol);
          return client.get(
            "/history/intraday",
            { symbol, interval, start, end, limit: String(limit) },
            call?.signal,
          );
        },
      },
      statements: {
        statement: async (symbol, { statement, periodType, limit, after }, call?) => {
          requireAShare(symbol);
          return client.get<Statement>(
            "/statements",
            { symbol, statement, period_type: periodType, limit: String(limit), after },
            call?.signal,
          );
        },
      },
      profiles: {
        profile: async (symbol, call?) => {
          requireAShare(symbol);
          return client.get("/profile", { symbol }, call?.signal);
        },
        analyst: async (symbol, call?) => {
          requireAShare(symbol);
          return client.get("/estimates", { symbol }, call?.signal);
        },
        calendar: async (symbol) => {
          requireAShare(symbol);
          throw new DataError("unsupported", "AkShare does not provide an earnings calendar for A-shares.", {
            source: NAME,
          });
        },
      },
      corporateActions: {
        list: async (symbol, call?) => {
          requireAShare(symbol);
          return client.get("/corporate_actions", { symbol }, call?.signal);
        },
      },
      news: {
        articles: async ({ symbol, limit, after }, call?) => {
          if (!symbol || !isAShareSymbol(symbol)) {
            throw new DataError("unsupported", "AkShare news requires an A-share symbol.", { source: NAME });
          }
          return client.get("/news", { symbol, limit: String(limit), after }, call?.signal);
        },
        article: async (id) => {
          throw new DataError(
            "unsupported",
            `AkShare returns article summaries in the news list; full text fetch is not supported for ${id}.`,
            { source: NAME },
          );
        },
      },
      instruments: {
        search: async (query, kind, call?) => {
          if (!isChinaInstrumentQuery(query, kind)) {
            throw new DataError("unsupported", `AkShare searches Chinese instruments only, not "${query}".`, {
              source: NAME,
            });
          }
          try {
            return await client.get<InstrumentMatch[]>(
              "/instruments",
              { q: query, kind, limit: "10" },
              call?.signal,
            );
          } catch (error) {
            if (error instanceof DataError) throw error;
            throw new DataError("unavailable", errorText(error), { source: NAME });
          }
        },
      },
      macroSeries: {
        search: async (query, call?) => {
          if (!isChinaMacroQuery(query)) {
            throw new DataError("unsupported", `AkShare serves China macro series only, not "${query}".`, {
              source: NAME,
            });
          }
          return client.get<SeriesMeta[]>("/macro/search", { q: query }, call?.signal);
        },
        observations: async (key, { limit, end }, call?) => {
          if (!isChinaMacroKey(key)) {
            throw new DataError("unsupported", `AkShare serves China macro keys (cn.*) only, not ${key}.`, {
              source: NAME,
            });
          }
          return client.get<SeriesObservations>(
            "/macro/observations",
            { key: key.replace(/^series:/i, ""), limit: String(limit), end },
            call?.signal,
          );
        },
      },
    },
  } satisfies DataProvider;
}
