import type { Metadata } from "next";
import Link from "next/link";

export const metadata: Metadata = {
  title: "Terms — Chainseer",
  description: "Terms for using the Chainseer public risk-analysis scanner.",
  alternates: { canonical: "/terms" },
};

export default function TermsPage() {
  return (
    <main className="policy-page">
      <Link className="policy-back" href="/">← Back to Chainseer</Link>
      <div className="eyebrow">Terms of use</div>
      <h1>Evidence helps. It does not remove risk.</h1>
      <p className="policy-updated">Effective July 23, 2026</p>

      <div className="policy-body">
        <section>
          <h2>Informational service</h2>
          <p>
            Chainseer provides automated, evidence-backed risk analysis for
            public smart contracts. It does not provide financial, investment,
            legal, tax, or security advice.
          </p>
        </section>
        <section>
          <h2>No safety guarantee</h2>
          <p>
            A favorable score, a verified source, or a sealed Timechain Ring
            does not prove that a token is safe. Contracts can change, external
            data can be incomplete, providers can disagree, and previously
            unseen attacks can occur. Digital assets can lose all value.
          </p>
        </section>
        <section>
          <h2>Your responsibility</h2>
          <p>
            You are responsible for independently checking a contract and
            deciding whether to buy, sell, bridge, approve, or connect. Never
            rely on Chainseer as the sole basis for a financial transaction.
          </p>
        </section>
        <section>
          <h2>Permitted use</h2>
          <p>
            Do not disrupt the service, evade rate limits, probe for secrets,
            submit unlawful content, or misrepresent a Chainseer report. Public
            links and evidence hashes may be used to verify an authentic
            analysis.
          </p>
        </section>
        <section>
          <h2>Availability and changes</h2>
          <p>
            The service may be limited, changed, suspended, or withdrawn while
            it is developed. Reports reflect the evidence and block height
            available at analysis time.
          </p>
        </section>
      </div>
    </main>
  );
}
