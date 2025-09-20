import { useState } from "react";

type CreatePostProps = {
  onAddPost: (content: string) => void;
};

export function CreatePost({ onAddPost }: CreatePostProps) {
  const [content, setContent] = useState("");

  const handleSubmit = () => {
    if (content.trim() === "") {
      return;
    }
    onAddPost(content);
    setContent("");
  };

  return (
    <div className="bg-white p-4 rounded-lg shadow-md border border-gray-200">
      <textarea
        value={content}
        // 👇 A MÁGICA ACONTECE AQUI
        onChange={(evento) => setContent(evento.target.value)}
        className="w-full p-2 border border-gray-300 rounded-md focus:outline-none focus:ring-2 focus:ring-blue-500 text-black"
        placeholder="O que você está pensando?"
        rows={3}></textarea>
      <div className="text-right mt-3">
        <button
          onClick={handleSubmit}
          className="bg-blue-600 text-white font-bold py-2 px-6 rounded-lg hover:bg-blue-700 transition-colors duration-200">
          Postar
        </button>
      </div>
    </div>
  );
}
