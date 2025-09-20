// src/types.ts

// Mantenha seu PostType original
export type PostType = {
  postId: number;
  text: string;
  createdAt: string; // O backend envia uma string no formato ISO
  authorId: number;
};



// Tipo para as citações (suposição da estrutura)
export type CitationType = {
  title: string;
  url: string;
  source: string;
};

export type PostDetailsType = {
  version: string;
  jobId: string; // NOVO CAMPO
  postId: number;
  stance: "SUPPORT" | "REFUTE" | "UNCLEAR" | string;
  decision: string;
  confidence: number;
  scores: {
    relevance_llm: number;
    evidence_score: number;
    freshness_avg: number;
  };
  explanation: string;
  education: string; // NOVO CAMPO
  citations: CitationType[];
  latency_ms: number;
  model_used: string;
};