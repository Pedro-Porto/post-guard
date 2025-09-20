// src/api.ts
import type { PostDetailsType } from './types';

// Esta função simula uma chamada de rede para buscar detalhes de um post.
export const fetchPostDetails2 = (postId: number): Promise<PostDetailsType> => {
  console.log(`Buscando detalhes para o post ID: ${postId}...`);

  return new Promise((resolve) => {
    setTimeout(() => {
      // Objeto mock ATUALIZADO com os novos campos
      const mockDetails: PostDetailsType = {
        version: "rag.results.v1",
        jobId: `job_${Date.now()}`, // NOVO CAMPO: Gerando um ID de job falso
        postId: postId,
        stance: ["SUPPORT", "REFUTE", "UNCLEAR"][Math.floor(Math.random() * 3)],
        decision: "Factually Correct (with context)",
        confidence: Math.random() * (0.98 - 0.75) + 0.75,
        scores: {
          relevance_llm: Math.random(),
          evidence_score: Math.random(),
          freshness_avg: Math.random(),
        },
        explanation: "The claim is generally supported by evidence, but lacks some critical context regarding the timeline of events. Multiple sources confirm the main assertion.",
        // NOVO CAMPO: Adicionando o conteúdo educacional
        education: "- Verifique a origem e a data das informações.\n- Busque estudos/órgãos oficiais antes de compartilhar.\n- Desinformação pode causar dano público (ex.: confusão e decisões arriscadas).",
        citations: [
          { title: "Primary Source Report on Event X", url: "#", source: "news-source.com" },
          { title: "Analysis of Timeline", url: "#", source: "analysis-journal.org" },
        ],
        latency_ms: Math.floor(Math.random() * (1200 - 800) + 800),
        model_used: Math.random() > 0.5 ? "gpt-4o-mini" : "llama3.1:8b-instruct",
      };
      console.log("Detalhes recebidos:", mockDetails);
      resolve(mockDetails);
    }, 1000);
  });
};