import type { Metadata } from "next";
import Link from "next/link";

export const metadata: Metadata = {
  title: "Privacy — Chainseer",
  description: "How Chainseer handles scanner requests and analysis records.",
  alternates: { canonical: "/privacy" },
};

export default function PrivacyPage() {
  return (
    <main className="policy-page">
      <Link className="policy-back" href="/">← Back to Chainseer</Link>
      <div className="eyebrow">Privacy notice</div>
      <h1>Public-chain analysis, minimal private data.</h1>
      <p className="policy-updated">Effective July 23, 2026</p>

      <div className="policy-body">
        <section>
          <h2>What the scanner processes</h2>
          <p>
            Chainseer processes the public contract address you submit, public
            blockchain data, third-party risk and market responses, request
            identifiers, and limited operational metadata needed to secure and
            run the service.
          </p>
        </section>
        <section>
          <h2>Rate limiting</h2>
          <p>
            The website converts the connecting network address into a one-way
            keyed identifier before sending it to the analysis service. The raw
            network address is not written into a Chainseer Timechain Ring.
            Hosting providers may still process network metadata under their
            own policies.
          </p>
        </section>
        <section>
          <h2>Timechain records</h2>
          <p>
            Completed analyses are sealed into an append-only Timechain. A Ring
            can include the public contract address, conclusion, evidence
            hashes, block pin, and Proof-of-Qualia scores. This history is
            intentionally tamper-evident and is not designed for deletion or
            silent rewriting. Do not submit secrets or personal information.
          </p>
        </section>
        <section>
          <h2>External data sources</h2>
          <p>
            To produce a report, Chainseer may send the public contract address
            to blockchain RPC services, explorers, security providers, and
            market-data providers. Those services handle requests under their
            own privacy terms.
          </p>
        </section>
        <section>
          <h2>No wallet access</h2>
          <p>
            The public scanner does not connect to a wallet, request a
            signature, execute a transaction, or ask for a private key.
          </p>
        </section>
      </div>
    </main>
  );
}
