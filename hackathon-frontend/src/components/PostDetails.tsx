// src/components/PostDetails.tsx
import type { PostDetailsType } from "../types";

// Helper para colorir o 'stance'
const getStanceClasses = (stance: string) => {
  switch (stance) {
    case "SUPPORT":
      return "bg-green-100 text-green-800";
    case "REFUTE":
      return "bg-red-100 text-red-800";
    default:
      return "bg-yellow-100 text-yellow-800";
  }
};

export function PostDetails({ details }: { details: PostDetailsType }) {
  const educationPoints = details.education
    .split("\n")
    .filter((line) => line.length > 0);

  return (
    <div className="bg-slate-50 border border-slate-200 rounded-lg p-4 mt-2 animate-fade-in">
      <h4 className="text-lg font-bold text-slate-800 mb-3">
        Análise de Conteúdo
      </h4>

      <div className="grid grid-cols-1 md:grid-cols-3 gap-4 mb-4">
        <div className="flex flex-col">
          <span className="text-xs font-semibold text-gray-500">
            Posicionamento
          </span>
          <span
            className={`text-sm font-bold px-2 py-1 rounded-md self-start ${getStanceClasses(
              details.stance
            )}`}>
            {details.stance}
          </span>
        </div>
        <div className="flex flex-col">
          <span className="text-xs font-semibold text-gray-500">Decisão</span>
          <span className="text-sm font-bold text-slate-700">
            {details.decision}
          </span>
        </div>
        <div className="flex flex-col">
          <span className="text-xs font-semibold text-gray-500">Confiança</span>
          <span className="text-sm font-bold text-blue-600">
            {(details.confidence * 100).toFixed(1)}%
          </span>
        </div>
      </div>

      <div className="mb-4">
        <p className="text-xs font-semibold text-gray-500 mb-1">Explicação</p>
        <p className="text-sm text-gray-700 bg-white p-3 rounded-md border">
          {details.explanation}
        </p>
      </div>

      {/* NOVA SEÇÃO: Recomendações Educacionais */}
      <div className="mb-5 bg-yellow-50 border-l-4 border-yellow-400 p-4 rounded-r-lg">
        <div className="flex items-center">
          <svg
            className="h-6 w-6 text-yellow-500 mr-3"
            fill="none"
            viewBox="0 0 24 24"
            stroke="currentColor">
            <path
              strokeLinecap="round"
              strokeLinejoin="round"
              strokeWidth={2}
              d="M9.663 17h4.673M12 3v1m6.364 1.636l-.707.707M21 12h-1M4 12H3m3.343-5.657l-.707-.707m2.828 9.9a5 5 0 117.072 0l-.548.547A3.374 3.374 0 0014 18.469V19a2 2 0 11-4 0v-.531c0-.895-.356-1.754-.988-2.386l-.548-.547z"
            />
          </svg>
          <h5 className="text-md font-bold text-yellow-800">
            Recomendações e Dicas
          </h5>
        </div>
        <ul className="mt-2 list-disc list-inside pl-2 text-sm text-yellow-900 space-y-1">
          {educationPoints.map((point, index) => (
            <li key={index}>{point.replace("-", "").trim()}</li>
          ))}
        </ul>
      </div>

      <div>
        <p className="text-xs font-semibold text-gray-500 mb-1">Citações</p>
        <ul className="list-disc list-inside">
          {details.citations.map((citation, index) => (
            <li key={index} className="text-sm text-blue-700 hover:underline">
              <a href={citation.url} target="_blank" rel="noopener noreferrer">
                {citation.title} ({citation.source})
              </a>
            </li>
          ))}
        </ul>
      </div>

      <div className="text-right text-xs text-gray-400 mt-4">
        Análise gerada por {details.model_used} em {details.latency_ms}ms.
      </div>
    </div>
  );
}
