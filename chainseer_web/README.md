# Chainseer Web

Public investor-oriented interface for Chainseer's evidence-backed token risk
analysis. The scanner supports three isolated analysis networks:

- Robinhood Chain — EVM, block-pinned
- Base — EVM, block-pinned
- Solana — SPL, confirmed-slot anchored

Selecting a network changes address validation, API routing, analysis copy, and
the full visual theme. Reports expose the decision, hard stops, market cap,
holder evidence, risk dimensions, entity/insider graph, provenance, and
Timechain/PoQ verification. Users may also enable browser-side polling for
critical liquidity, authority, upgrade, and sellability alerts.

The browser never receives the private upstream API token. Server routes under
`app/api/` validate requests, apply a pseudonymous client identity, and proxy
to the authenticated Chainseer API.

## Local development

Requirements: Node.js `>=22.13.0`.

```bash
npm install
npm run dev
```

Set the server-only values documented in `.env.example`, then open
`http://localhost:3000`.

## Verification

```bash
npm test
npm run build
```

The site is hosted with OpenAI Sites. `.openai/hosting.json` contains the
existing project ID and must be preserved. Production deployment is performed
only from a saved, verified source version.

## Safety boundary

Chainseer Web does not connect wallets, load private keys, sign transactions,
or execute trades. Scores and alerts are decision-support evidence, not a
guarantee of safety or investment performance.
