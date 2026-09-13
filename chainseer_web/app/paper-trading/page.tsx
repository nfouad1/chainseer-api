import { PaperTradingWorkspace } from "../page";

export default function PaperTradingPage() {
  return (
    <main className="chain-theme theme-robinhood">
      <header className="site-header">
        <a className="brand" href="/" aria-label="Chainseer home">
          <span className="brand-mark" aria-hidden="true">C</span>
          <span>CHAINSEER</span>
        </a>
        <nav aria-label="Primary navigation">
          <a href="/">Risk scanner</a>
          <a href="#portfolio-follow">Portfolio Follow</a>
        </nav>
        <a className="header-cta" href="/">Scan token</a>
      </header>
      <PaperTradingWorkspace />
    </main>
  );
}
