import { useState, useEffect } from "react";
import { CreatePost } from "./components/CreatePost";
import { PostList } from "./components/PostList";
import type { PostType, PostDetailsType } from "./types";
import { fetchPostDetails2 } from "./api2";
import { fetchAllPosts, createPost, fetchPostDetails } from "./api";

// O array de posts iniciais continua o mesmo...
// const initialPosts: PostType[] = [
//   {
//     id: 1,
//     author: "Usuário 1",
//     content:
//       "Estou pensando em como essa Máquina Analítica poderia ser usada para compor músicas! 🎵",
//     timestamp: Date.now() - 1000 * 60 * 30, // 30 minutos atrás
//   },
//   {
//     id: 2,
//     author: "Usuário 2",
//     content:
//       "Acredito que em cerca de 50 anos, será possível programar computadores para que eles possam jogar xadrez muito bem.",
//     timestamp: Date.now() - 1000 * 60 * 120, // 2 horas atrás
//   },
// ];

function App() {
  // const [posts, setPosts] = useState<PostType[]>(initialPosts);
  // MUDANÇA: O estado inicial de posts é um array vazio
  const [posts, setPosts] = useState<PostType[]>([]);
  // MUDANÇA: Adicionamos um estado de loading para a busca inicial
  const [isListLoading, setIsListLoading] = useState<boolean>(true);

  // --- NOVOS ESTADOS ---
  const [selectedPostId, setSelectedPostId] = useState<number | null>(null);
  const [postDetails, setPostDetails] = useState<PostDetailsType | null>(null);
  const [isLoadingDetails, setIsLoadingDetails] = useState<boolean>(false);

  // Efeito que busca os dados quando um post é selecionado
  useEffect(() => {
    const loadPosts = async () => {
      try {
        const initialPosts = await fetchAllPosts();
        setPosts(initialPosts);
      } catch (error) {
        console.error(error);
        // Aqui você poderia mostrar uma mensagem de erro na tela
      } finally {
        setIsListLoading(false);
      }
    };
    loadPosts();
  }, []); // O array vazio [] garante que isso rode apenas uma vez

  useEffect(() => {
    if (selectedPostId === null) {
      setPostDetails(null);
      return;
    }
    const getDetails = async () => {
      setIsLoadingDetails(true);
      try {
        const details = await fetchPostDetails(selectedPostId);
        setPostDetails(details);
      } catch (error) {
        console.error(error);
      } finally {
        setIsLoadingDetails(false);
      }
    };
    getDetails();
  }, [selectedPostId]);

  // Função para lidar com o clique em um post
  const handlePostSelect = (id: number) => {
    if (id === selectedPostId) {
      setSelectedPostId(null);
    } else {
      setSelectedPostId(id);
    }
  };

  // 👇 SUBSTITUA SUA FUNÇÃO POR ESTA VERSÃO ATUALIZADA
  const handleAddPost = async (content: string) => {
    try {
      // Chamamos a nova versão da função createPost
      const newPostFromApi = await createPost(content);

      setPosts([newPostFromApi, ...posts]);
    } catch (error) {
      console.error(error);
      alert("Não foi possível criar o post. Tente novamente.");
    }
  };

  return (
    <div className="bg-slate-100 w-[100vw] min-h-screen">
      <header className="bg-white shadow-md">
        <nav className="container mx-auto px-4 py-4">
          <h1 className="text-2xl font-bold text-blue-600">
            Painel de controle da minha rede social
          </h1>
        </nav>
      </header>

      <main className="container mx-auto p-4 max-w-2xl">
        <h2 className="text-xl font-bold text-slate-700 mb-4">
          Simulação de Postagem em Rede Social
        </h2>
        <CreatePost onAddPost={handleAddPost} />
        <h2 className="text-xl font-bold text-slate-700 mt-8 mb-2">
          Mural de Posts
        </h2>
        {isListLoading ? (
          <p>Carregando mural de posts...</p>
        ) : (
          <PostList
            posts={posts}
            selectedPostId={selectedPostId}
            postDetails={postDetails}
            isLoading={isLoadingDetails}
            onPostSelect={handlePostSelect}
          />
        )}{" "}
      </main>
    </div>
  );
}

export default App;
