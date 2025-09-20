// src/components/Post.tsx
import type { PostType } from "../types";

type PostProps = {
  post: PostType;
  onPostSelect: (id: number) => void;
  isSelected: boolean;
};

// formata a data para uma leitura mais amigável
const formatDate = (dateString: string) => {
  return new Date(dateString).toLocaleDateString('pt-BR', {
    hour: '2-digit',
    minute: '2-digit',
    day: '2-digit',
    month: 'short',
    year: 'numeric',
  });
};

export function Post({ post, onPostSelect, isSelected }: PostProps) {
  // Adicionamos classes para o estado selecionado
  const selectedClasses = isSelected
    ? "ring-2 ring-blue-500"
    : "border-gray-200";

  return (
    // Transformamos o div principal em um botão clicável
    <div
      // MUDANÇA: Usando post.postId
      onClick={() => onPostSelect(post.postId)}
      className={`w-full text-left bg-white p-5 rounded-lg shadow-md mb-4 border transition-all duration-200 hover:bg-slate-50 ${selectedClasses}`}
    >
      <div className="flex items-center mb-3">
        <div className="w-10 h-10 rounded-full bg-slate-600 flex items-center justify-center font-bold text-white mr-3">
          {/* MUDANÇA: Exibindo o ID do autor */}
          {post.authorId}
        </div>
        <div>
          {/* MUDANÇA: Exibindo "Autor ID" */}
          <p className="font-bold text-slate-800">Autor ID: {post.authorId}</p>
          {/* MUDANÇA: Usando post.createdAt com a função formatDate */}
          <p className="text-xs text-gray-500">{formatDate(post.createdAt)}</p>
        </div>
      </div>
      {/* MUDANÇA: Usando post.text */}
      <p className="text-gray-700">{post.text}</p>
    </div>
  );
}
