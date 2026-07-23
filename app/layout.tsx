import type { Metadata } from "next";
import { Geist, Geist_Mono } from "next/font/google";
import "./globals.css";

const geistSans = Geist({ variable: "--font-geist-sans", subsets: ["latin"] });
const geistMono = Geist_Mono({ variable: "--font-geist-mono", subsets: ["latin"] });

export const metadata: Metadata = {
  metadataBase: new URL("https://usechainseer.com"),
  title: "Chainseer — Evidence-backed on-chain risk intelligence",
  description:
    "Scan smart contracts across twelve risk dimensions, inspect hard stops, and verify every conclusion through block-pinned evidence.",
  openGraph: {
    title: "Chainseer — Read the chain. Verify the claim.",
    description: "Evidence-backed on-chain risk intelligence for investors.",
    type: "website",
    url: "https://usechainseer.com",
    images: [
      {
        url: "/og.png",
        width: 1792,
        height: 921,
        alt: "Chainseer — Read the chain. Verify the claim.",
      },
    ],
  },
  twitter: {
    card: "summary_large_image",
    title: "Chainseer — Read the chain. Verify the claim.",
    description: "Evidence-backed on-chain risk intelligence for investors.",
    images: ["/og.png"],
  },
};

export default function RootLayout({ children }: Readonly<{ children: React.ReactNode }>) {
  return (
    <html lang="en">
      <body className={`${geistSans.variable} ${geistMono.variable}`}>{children}</body>
    </html>
  );
}
